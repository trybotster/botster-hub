//! Retain family cleanup inputs across live and detached phases.

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
    release_prepared: bool,
    payload: Option<PackageEntityMutation>,
    mutation_lease: Option<EntityMutationLease>,
    resync_lease: Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
    fanout: Option<LeasedFanoutMutation>,
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
    pub(crate) fn step_direct_package_entity_cleanup(
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
                CausalAdmitResult::Applied => {
                    cursor.mutation_lease = None;
                    cursor.resync_lease = None;
                    FamilyCleanupStep::Pending
                }
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
        let selected = if let Some(selected) = cleanup.family_cursor.select_detached() {
            selected
        } else {
            let selected =
                self.with_direct_entity_model(|model| model.select_cleanup_live(cleanup));
            self.note_package_entity_resync_changed();
            selected
        };
        match selected {
            Selection::Pending => FamilyCleanupStep::Pending,
            Selection::Complete => FamilyCleanupStep::Complete,
            Selection::Payload => FamilyCleanupStep::Payload(
                cleanup
                    .family_cursor
                    .payload
                    .take()
                    .expect("the payload is retained"),
            ),
        }
    }

    pub(crate) fn complete_direct_package_entity_cleanup_item(
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
            match self.step_direct_package_entity_cleanup(cleanup) {
                FamilyCleanupStep::Pending => {}
                FamilyCleanupStep::Waiting | FamilyCleanupStep::Fault => return false,
                FamilyCleanupStep::Payload(payload) => {
                    drop(payload);
                    self.complete_direct_package_entity_cleanup_item(cleanup);
                }
                FamilyCleanupStep::Complete => return true,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CleanupPhase {
    Live,
    Detached,
    Complete,
}

enum Selection {
    Pending,
    Payload,
    Complete,
}

impl HostPackageCleanup {
    /// Read this only on a Host worker or through an exact completion.
    pub(super) fn entity_cleanup_phase(&self) -> CleanupPhase {
        let cursor = &self.family_cursor;
        if cursor.release.is_some()
            || cursor.payload.is_some()
            || cursor
                .retired
                .as_ref()
                .is_some_and(|(_, family)| family.is_some())
        {
            CleanupPhase::Detached
        } else if cursor.retired.is_none() && self.unloaded_families.is_empty() {
            CleanupPhase::Complete
        } else {
            CleanupPhase::Live
        }
    }

    /// The caller starts another phase only after the previous causal receipt is applied.
    pub(super) fn step_detached_entity_cleanup(&mut self, causal: &mut Option<CausalOp>) {
        assert_eq!(self.entity_cleanup_phase(), CleanupPhase::Detached);
        let cursor = &mut self.family_cursor;
        if cursor.release_prepared {
            cursor.release = None;
            cursor.mutation_lease = None;
            cursor.resync_lease = None;
            cursor.release_prepared = false;
        } else if cursor.payload.is_some() {
            drop(cursor.payload.take());
            cursor.awaiting_worker = false;
        } else if let Some(release) = cursor.release {
            *causal = Some(release);
            cursor.release_prepared = true;
        } else {
            assert!(cursor.select_detached().is_some());
        }
    }
}

impl FamilyCleanupCursor {
    fn select_detached(&mut self) -> Option<Selection> {
        let cursor = self;
        if let Some((_, retired)) = &mut cursor.retired
            && let Some(family) = retired
        {
            assert!(
                cursor.payload.is_none()
                    && cursor.mutation_lease.is_none()
                    && cursor.resync_lease.is_none()
            );
            if let Some((seq, payload)) = family.pending_by_seq.pop_first() {
                cursor.payload = Some(payload);
                cursor.mutation_lease = family.pending_leases.remove(&seq);
                cursor.release = cursor.mutation_lease.as_ref().map(mutation_release);
                cursor.awaiting_worker = true;
                return Some(Selection::Payload);
            }
            if let Some((_, lease)) = family.pending_leases.pop_first() {
                cursor.mutation_lease = Some(lease);
                cursor.release = cursor.mutation_lease.as_ref().map(mutation_release);
                return Some(Selection::Pending);
            }
            if !family.resync.leases.is_empty() {
                let family_token = family
                    .causal_token
                    .expect("resync lease has a family token");
                cursor.resync_lease = family.resync.leases.pop_first();
                cursor.release = Some(CausalOp::Release {
                    scope_id: cursor
                        .resync_lease
                        .as_ref()
                        .expect("the resync lease is retained")
                        .0,
                    identity: LeaseIdentity::ProviderResyncNeed { family_token },
                });
                return Some(Selection::Pending);
            }
            // Clear the state after its maps and lease set become empty.
            *retired = None;
            return Some(Selection::Pending);
        }
        None
    }
}

impl PackageEntities {
    pub(super) fn step_entity_cleanup(
        &mut self,
        cleanup: &mut HostPackageCleanup,
    ) -> Result<(), PackageEntityCleanupError> {
        assert_eq!(cleanup.entity_cleanup_phase(), CleanupPhase::Live);
        if cleanup.family_epoch.is_none() {
            cleanup.family_epoch = Some(self.advance_epoch()?);
        } else {
            self.select_cleanup_live(cleanup);
        }
        Ok(())
    }

    fn select_cleanup_live(&mut self, cleanup: &mut HostPackageCleanup) -> Selection {
        let cursor = &mut cleanup.family_cursor;
        let Some(epoch) = cleanup.family_epoch else {
            assert!(cleanup.unloaded_families.is_empty());
            return Selection::Complete;
        };
        if let Some((name, retired)) = &mut cursor.retired {
            assert!(retired.is_none());
            let fanout = &mut self.fanout;
            if let Some(generation) = fanout.next_family_generation_before(name, epoch) {
                fanout.take_one_family_into(name, generation, &mut cursor.fanout);
                let item = cursor
                    .fanout
                    .as_mut()
                    .expect("the indexed generation contains a mutation");
                cursor.mutation_lease = item.lease.take();
                cursor.release = cursor.mutation_lease.as_ref().map(mutation_release);
                cursor.awaiting_worker = true;
                cursor.payload = Some(
                    cursor
                        .fanout
                        .take()
                        .expect("the fanout item is retained")
                        .mutation,
                );
                return Selection::Payload;
            }
            cursor.retired = None;
            return Selection::Pending;
        }
        let Some((package, provided)) = cleanup.unloaded_families.last_mut() else {
            return Selection::Complete;
        };
        let PackageEntities {
            families,
            fanout,
            resync_releases,
            ..
        } = self;
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
                return Selection::Pending;
            }
            cursor.after = name.clone();
            name
        } else {
            let name = fanout.next_package_family(package, cursor.queued_after.as_deref());
            cursor.queued_after = name.clone();
            name
        };
        if let Some(name) = name {
            let detach = families
                .get(&name)
                .is_some_and(|family| family.generation < epoch);
            cursor.retired = Some((name, None));
            let (name, retired) = cursor
                .retired
                .as_mut()
                .expect("the cleanup name is retained");
            if detach {
                *retired = families.remove(name);
                let family = retired.as_ref().expect("the old family is retained");
                resync_releases.remove(&(name.clone(), family.generation));
            }
        } else {
            cleanup.unloaded_families.pop();
            cursor.after = None;
            cursor.queued_after = None;
            cursor.live_complete = false;
            cursor.checked_package = false;
        }
        Selection::Pending
    }
}

fn mutation_release(lease: &crate::package_entity_fanout::EntityMutationLease) -> CausalOp {
    CausalOp::Release {
        scope_id: lease.scope_id,
        identity: LeaseIdentity::AdmittedEntityMutation {
            family_token: lease.family_token,
            seq: lease.seq,
        },
    }
}
