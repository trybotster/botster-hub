//! Package entity state and transitions, independent of owner scheduling.

use super::{
    BTreeMap, BTreeSet, CausalOp, EntityMutationLease, Instant, LeaseIdentity,
    LeasedFanoutMutation, PackageEntityCleanupError, PackageEntityFamilyProgress,
    PackageEntityFamilyState, PackageEntityFamilyStep, PackageEntityFanoutQueue,
    PackageEntityMutation, PackageEntityPublishResult, PackageEntityPublishStatus,
    settle_entity_publish_op,
};

pub(super) struct PackageEntities {
    pub(super) families: BTreeMap<String, PackageEntityFamilyState>,
    pub(super) fanout: PackageEntityFanoutQueue,
    pub(super) resync_releases: BTreeSet<(String, u64)>,
    pub(super) epoch: u64,
    pub(super) next_family_token: u64,
    pub(super) publication: Option<super::PublicationRetirement>,
}

impl Default for PackageEntities {
    fn default() -> Self {
        Self {
            families: BTreeMap::new(),
            fanout: PackageEntityFanoutQueue::default(),
            resync_releases: BTreeSet::new(),
            epoch: 0,
            next_family_token: 1,
            publication: None,
        }
    }
}

pub(super) struct PublishTransition {
    pub(super) result: PackageEntityPublishResult,
    pub(super) discarded: Option<PackageEntityMutation>,
    pub(super) causal: Option<CausalOp>,
    pub(super) drain: Option<(String, u64)>,
}

/// The Host caller retains this record through completion and causal application.
#[derive(Default)]
pub(super) struct PublishWork {
    pub(super) admission: crate::package_entity_fanout::FamilyAdmissionWork,
    pub(super) incoming_lease: Option<EntityMutationLease>,
    pub(super) ready: Option<LeasedFanoutMutation>,
    pub(super) causal: Option<CausalOp>,
    pub(super) drain: Option<(String, u64)>,
}

impl PackageEntities {
    pub(super) fn publication_drains_family(&self, name: &str, generation: u64) -> bool {
        self.publication
            .as_ref()
            .and_then(|pending| pending.drain.as_ref())
            .is_some_and(|(active, active_generation)| {
                active == name && *active_generation == generation
            })
    }

    pub(super) fn advance_publish(
        &mut self,
        name: &str,
        generation: u64,
    ) -> (
        super::PublicationAdvance,
        Option<PackageEntityPublishResult>,
    ) {
        let mut retained = None;
        self.advance_publish_retained(name, generation, &mut retained)
    }

    pub(super) fn advance_publish_retained(
        &mut self,
        name: &str,
        generation: u64,
        retained: &mut Option<LeasedFanoutMutation>,
    ) -> (
        super::PublicationAdvance,
        Option<PackageEntityPublishResult>,
    ) {
        assert!(retained.is_none(), "the output slot must be empty");
        let Some(family) = self
            .families
            .get_mut(name)
            .filter(|family| family.generation == generation)
        else {
            return (super::PublicationAdvance::Complete, None);
        };
        let advanced = family.has_next_pending();
        if advanced {
            if !self.fanout.has_sequence_capacity() {
                return (super::PublicationAdvance::Fault, None);
            }
            family.take_next_pending_into(Instant::now(), retained);
            assert!(retained.is_some(), "the next mutation exists");
            assert!(
                self.fanout.try_push_from(retained),
                "exclusive preflight guarantees sequence capacity"
            );
        }
        let result = family.result(PackageEntityPublishStatus::Accepted);
        self.index_resync_releases(name);
        let status = if advanced {
            super::PublicationAdvance::Again
        } else {
            super::PublicationAdvance::Complete
        };
        (status, Some(result))
    }

    pub(super) fn next_epoch(&self) -> Result<u64, PackageEntityCleanupError> {
        self.epoch
            .checked_add(1)
            .ok_or(PackageEntityCleanupError::GenerationExhausted)
    }

    pub(super) fn advance_epoch(&mut self) -> Result<u64, PackageEntityCleanupError> {
        self.epoch = self.next_epoch()?;
        Ok(self.epoch)
    }

    pub(super) fn family(&mut self, name: &str) -> &mut PackageEntityFamilyState {
        let generation = self.epoch;
        self.families
            .entry(name.to_string())
            .or_insert_with(|| PackageEntityFamilyState {
                generation,
                ..PackageEntityFamilyState::default()
            })
    }

    pub(super) fn allocate_family_token(&mut self) -> Result<u64, String> {
        let token = self.next_family_token;
        if token == 0 {
            return Err(
                "entity family causal token exhausted (entity_family_token_exhausted)".into(),
            );
        }
        self.next_family_token = token.checked_add(1).unwrap_or(0);
        Ok(token)
    }

    pub(super) fn ensure_family_token(&mut self, name: &str) -> Result<u64, String> {
        if let Some(token) = self
            .families
            .get(name)
            .and_then(|family| family.causal_token)
        {
            return Ok(token);
        }
        let token = self.allocate_family_token()?;
        self.family(name).causal_token = Some(token);
        Ok(token)
    }

    pub(super) fn index_resync_releases(&mut self, name: &str) {
        let Some(family) = self.families.get(name) else {
            return;
        };
        let key = (name.to_string(), family.generation);
        if (!family.resync.needed || family.resync.degraded) && !family.resync.leases.is_empty() {
            self.resync_releases.insert(key);
        } else {
            self.resync_releases.remove(&key);
        }
    }

    pub(super) fn admit(
        &mut self,
        registration: &crate::lifecycle::EntityProviderRegistration,
        mutation: PackageEntityMutation,
        scope_id: Option<u64>,
        publication_token: u64,
    ) -> Result<PublishTransition, (String, PackageEntityMutation)> {
        let mut work = PublishWork {
            admission: crate::package_entity_fanout::FamilyAdmissionWork {
                input: Some(mutation),
                ..Default::default()
            },
            ..Default::default()
        };
        if let Err(error) =
            self.admit_retained(registration, scope_id, publication_token, &mut work)
        {
            return Err((
                error,
                work.admission.input.expect("refusal retains the input"),
            ));
        }
        Ok(PublishTransition {
            result: work.admission.result.expect("admission completed"),
            discarded: work.admission.discarded,
            causal: work.causal,
            drain: work.drain,
        })
    }

    pub(super) fn admit_retained(
        &mut self,
        registration: &crate::lifecycle::EntityProviderRegistration,
        scope_id: Option<u64>,
        publication_token: u64,
        work: &mut PublishWork,
    ) -> Result<(), String> {
        assert!(
            work.incoming_lease.is_none()
                && work.ready.is_none()
                && work.causal.is_none()
                && work.drain.is_none()
        );
        if !registration.is_live() {
            return Err("entity_publish provider registration is no longer live".into());
        }
        let mutation = work
            .admission
            .input
            .as_ref()
            .expect("publication retains its mutation");
        let mutation_seq = mutation.snapshot_seq();
        let entity_type = mutation.entity_type().to_string();
        if !self.fanout.has_sequence_capacity() {
            return Err(
                "entity_publish queue sequence exhausted (entity_fanout_sequence_exhausted)".into(),
            );
        }
        let causal_token = if scope_id.is_some() {
            Some(self.ensure_family_token(&entity_type)?)
        } else {
            None
        };
        let family = self.family(&entity_type);
        let admission = mutation.admission().cloned();
        work.incoming_lease = scope_id.map(|scope_id| EntityMutationLease {
            admission: admission.clone(),
            scope_id,
            family_token: causal_token.expect("scoped publication has a family token"),
            family: entity_type.clone(),
            generation: family.generation,
            seq: mutation_seq,
        });
        family.admit_retained(&mut work.admission, Instant::now());
        let result = work
            .admission
            .result
            .as_ref()
            .expect("family admission completed");
        if result.status == PackageEntityPublishStatus::PendingGap
            && let Some(lease) = work.incoming_lease.clone()
        {
            family.store_pending_lease(lease);
        }
        if let Some(mutation) = work.admission.ready.take() {
            work.ready = Some(LeasedFanoutMutation {
                mutation,
                lease: work.incoming_lease.take(),
                generation: family.generation,
            });
        }
        work.drain = (result.status == PackageEntityPublishStatus::Accepted)
            .then(|| (entity_type.clone(), family.generation));
        if let Some(scope_id) = scope_id {
            work.causal = Some(settle_entity_publish_op(
                family,
                scope_id,
                publication_token,
                mutation_seq,
                result,
            ));
            if work.admission.discarded.is_some() {
                if let Some(CausalOp::Transfer { from, to, .. }) = &mut work.causal {
                    to[2] = Some(*from);
                    family.remember_resync_lease_with_admission(scope_id, admission.clone());
                } else {
                    work.causal = None;
                }
            } else if result.ok && result.resync_needed {
                family.remember_resync_lease_with_admission(scope_id, admission.clone());
            }
        }
        self.index_resync_releases(&entity_type);
        if work.ready.is_some() {
            assert!(
                self.fanout.try_push_from(&mut work.ready),
                "exclusive admission preflight guarantees sequence capacity"
            );
        }
        Ok(())
    }

    pub(super) fn finish(
        &mut self,
        lease: &EntityMutationLease,
        scheduled_resync: bool,
    ) -> (CausalOp, bool) {
        let mut retained = None;
        self.finish_into(lease, scheduled_resync, &mut retained);
        retained.expect("finish completed")
    }

    pub(super) fn finish_into(
        &mut self,
        lease: &EntityMutationLease,
        scheduled_resync: bool,
        retained: &mut Option<(CausalOp, bool)>,
    ) {
        assert!(retained.is_none(), "the output slot must be empty");
        let admitted = LeaseIdentity::AdmittedEntityMutation {
            family_token: lease.family_token,
            seq: lease.seq,
        };
        if scheduled_resync {
            let Some(family) = self
                .families
                .get_mut(&lease.family)
                .filter(|family| family.causal_token == Some(lease.family_token))
            else {
                *retained = Some((
                    CausalOp::Release {
                        scope_id: lease.scope_id,
                        identity: admitted,
                    },
                    false,
                ));
                return;
            };
            let added = !family.resync.leases.contains_key(&lease.scope_id);
            *retained = Some((
                if added {
                    CausalOp::Transfer {
                        scope_id: lease.scope_id,
                        from: admitted,
                        to: [
                            Some(LeaseIdentity::ProviderResyncNeed {
                                family_token: lease.family_token,
                            }),
                            None,
                            None,
                        ],
                    }
                } else {
                    CausalOp::Release {
                        scope_id: lease.scope_id,
                        identity: admitted,
                    }
                },
                true,
            ));
            family.resync.mark_needed(Instant::now());
            family.remember_resync_lease_with_admission(lease.scope_id, lease.admission.clone());
            self.index_resync_releases(&lease.family);
            return;
        }
        *retained = Some((
            CausalOp::Release {
                scope_id: lease.scope_id,
                identity: admitted,
            },
            false,
        ));
    }

    pub(super) fn begin_snapshot(
        &mut self,
        name: &str,
        sequence: u64,
    ) -> PackageEntityFamilyProgress {
        let progress = self
            .family(name)
            .begin_provider_snapshot_seq(sequence, Instant::now());
        self.index_resync_releases(name);
        progress
    }

    pub(super) fn step_snapshot(&mut self, name: &str) -> (u64, PackageEntityFamilyStep) {
        let mut work = crate::package_entity_fanout::FamilySnapshotWork::default();
        let generation = self.step_snapshot_into(name, &mut work);
        (generation, work.step.expect("snapshot step completed"))
    }

    pub(super) fn step_snapshot_into(
        &mut self,
        name: &str,
        work: &mut crate::package_entity_fanout::FamilySnapshotWork,
    ) -> u64 {
        let family = self.family(name);
        let generation = family.generation;
        family.step_provider_snapshot_into(Instant::now(), work);
        self.index_resync_releases(name);
        generation
    }

    pub(super) fn mark_resync(&mut self, name: &str) {
        self.family(name).resync.mark_needed(Instant::now());
        self.index_resync_releases(name);
    }

    pub(super) fn rearm_resync(&mut self, name: &str) {
        self.family(name).resync.rearm(Instant::now());
        self.index_resync_releases(name);
    }

    pub(super) fn record_resync_attempt(&mut self, name: &str) -> bool {
        let degraded = self.family(name).resync.record_attempt(Instant::now());
        self.index_resync_releases(name);
        degraded
    }

    pub(super) fn take_resync_release(
        &mut self,
        name: &str,
        degraded_only: bool,
    ) -> Option<CausalOp> {
        let mut retained = None;
        let mut operation = None;
        self.take_resync_release_into(name, degraded_only, &mut retained, &mut operation);
        operation
    }

    pub(super) fn take_resync_release_into(
        &mut self,
        name: &str,
        degraded_only: bool,
        retained: &mut Option<(u64, Option<crate::lua_runtime::EntityPublishPermit>)>,
        operation: &mut Option<CausalOp>,
    ) {
        assert!(retained.is_none() && operation.is_none());
        let Some(family) = self.families.get_mut(name) else {
            return;
        };
        if (family.resync.degraded || (!degraded_only && !family.resync.needed))
            && !family.resync.leases.is_empty()
        {
            let family_token = family
                .causal_token
                .expect("resync lease has a family token");
            *retained = family.resync.leases.pop_first();
            *operation = Some(CausalOp::Release {
                scope_id: retained.as_ref().expect("the resync lease was retained").0,
                identity: LeaseIdentity::ProviderResyncNeed { family_token },
            });
        }
        self.index_resync_releases(name);
    }
}
