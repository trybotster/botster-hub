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
}

impl Default for PackageEntities {
    fn default() -> Self {
        Self {
            families: BTreeMap::new(),
            fanout: PackageEntityFanoutQueue::default(),
            resync_releases: BTreeSet::new(),
            epoch: 0,
            next_family_token: 1,
        }
    }
}

pub(super) struct PublishTransition {
    pub(super) result: PackageEntityPublishResult,
    pub(super) discarded: Option<PackageEntityMutation>,
    pub(super) causal: Option<CausalOp>,
    pub(super) drain: Option<(String, u64)>,
}

impl PackageEntities {
    pub(super) fn advance_publish(
        &mut self,
        name: &str,
        generation: u64,
    ) -> (
        super::PublicationAdvance,
        Option<PackageEntityPublishResult>,
    ) {
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
            let item = family
                .take_next_pending(Instant::now())
                .expect("the next mutation exists");
            self.fanout
                .try_push(item)
                .expect("exclusive preflight guarantees sequence capacity");
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
        if !registration.is_live() {
            return Err((
                "entity_publish provider registration is no longer live".into(),
                mutation,
            ));
        }
        let mutation_seq = mutation.snapshot_seq();
        let entity_type = mutation.entity_type().to_string();
        if !self.fanout.has_sequence_capacity() {
            return Err((
                "entity_publish queue sequence exhausted (entity_fanout_sequence_exhausted)".into(),
                mutation,
            ));
        }
        let causal_token = if scope_id.is_some() {
            match self.ensure_family_token(&entity_type) {
                Ok(token) => Some(token),
                Err(error) => return Err((error, mutation)),
            }
        } else {
            None
        };
        let family = self.family(&entity_type);
        let admission = mutation.admission().cloned();
        let (result, ready, discarded) = family.admit(mutation, Instant::now());
        let incoming_lease = scope_id.map(|scope_id| EntityMutationLease {
            admission: admission.clone(),
            scope_id,
            family_token: causal_token.expect("scoped publication has a family token"),
            family: entity_type.clone(),
            generation: family.generation,
            seq: mutation_seq,
        });
        if result.status == PackageEntityPublishStatus::PendingGap
            && let Some(lease) = incoming_lease.clone()
        {
            family.store_pending_lease(lease);
        }
        let leased_ready = ready.map(|mutation| LeasedFanoutMutation {
            mutation,
            lease: incoming_lease,
            generation: family.generation,
        });
        let drain = (result.status == PackageEntityPublishStatus::Accepted)
            .then(|| (entity_type.clone(), family.generation));
        let causal = scope_id.and_then(|scope_id| {
            let mut op = settle_entity_publish_op(
                family,
                scope_id,
                publication_token,
                mutation_seq,
                &result,
            );
            if discarded.is_some() {
                if let CausalOp::Transfer { from, to, .. } = &mut op {
                    // Disposal and resync each retain the originating publication.
                    to[2] = Some(*from);
                    family.remember_resync_lease_with_admission(scope_id, admission.clone());
                    Some(op)
                } else {
                    None
                }
            } else {
                if result.ok && result.resync_needed {
                    family.remember_resync_lease_with_admission(scope_id, admission.clone());
                }
                Some(op)
            }
        });
        self.index_resync_releases(&entity_type);
        if let Some(item) = leased_ready {
            self.fanout
                .try_push(item)
                .expect("exclusive admission preflight guarantees sequence capacity");
        }
        Ok(PublishTransition {
            result,
            discarded,
            causal,
            drain,
        })
    }

    pub(super) fn finish(
        &mut self,
        lease: &EntityMutationLease,
        scheduled_resync: bool,
    ) -> (CausalOp, bool) {
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
                return (
                    CausalOp::Release {
                        scope_id: lease.scope_id,
                        identity: admitted,
                    },
                    false,
                );
            };
            family.resync.mark_needed(Instant::now());
            let added = family
                .remember_resync_lease_with_admission(lease.scope_id, lease.admission.clone());
            self.index_resync_releases(&lease.family);
            if added {
                return (
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
                    },
                    true,
                );
            }
        }
        (
            CausalOp::Release {
                scope_id: lease.scope_id,
                identity: admitted,
            },
            scheduled_resync,
        )
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
        let family = self.family(name);
        let step = family.step_provider_snapshot(Instant::now());
        let generation = family.generation;
        self.index_resync_releases(name);
        (generation, step)
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
        let family = self.families.get_mut(name)?;
        let scope_id = if family.resync.degraded || (!degraded_only && !family.resync.needed) {
            family
                .resync
                .leases
                .pop_first()
                .map(|(scope_id, _admission)| scope_id)
        } else {
            None
        };
        let op = scope_id.map(|scope_id| CausalOp::Release {
            scope_id,
            identity: LeaseIdentity::ProviderResyncNeed {
                family_token: family
                    .causal_token
                    .expect("resync lease has a family token"),
            },
        });
        self.index_resync_releases(name);
        op
    }
}
