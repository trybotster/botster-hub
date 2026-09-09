//! Publication records retain requests through admission, disposal, and response.

use super::{
    PackageEntities, PackageEntityMutation, PackageEntityPublishResult, PublicationAdvance,
    PublicationRetirement,
};
use crate::lua_runtime::PendingEntityPublishRequest;
use crate::package_event_router::{CausalOp, LeaseIdentity};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Next {
    #[default]
    Idle,
    Advance,
    Dispose,
    Release,
    Reply,
}

#[derive(Default)]
pub(crate) struct Admission {
    pub(crate) pending: Option<PendingEntityPublishRequest>,
    pub(crate) acquired: bool,
    pub(super) retained: super::entities::PublishWork,
    registration: Option<crate::lifecycle::EntityProviderRegistration>,
    response: Option<std::sync::mpsc::Sender<Result<PackageEntityPublishResult, String>>>,
    scope_id: Option<u64>,
    token: u64,
    result: Option<Result<PackageEntityPublishResult, String>>,
}

#[derive(Default)]
pub(crate) struct Advance {
    family: Option<(String, u64)>,
    mutation: Option<crate::package_entity_fanout::LeasedFanoutMutation>,
    pub(crate) status: Option<PublicationAdvance>,
}

#[derive(Default)]
pub(crate) struct Reply {
    retired: Option<PublicationRetirement>,
}

impl PackageEntities {
    pub(super) fn publication_next(&self) -> Next {
        let Some(pending) = self.publication.as_ref() else {
            return Next::Idle;
        };
        if !pending.disposed {
            Next::Dispose
        } else if pending.drain.is_some() {
            Next::Advance
        } else if pending.release.is_some() {
            Next::Release
        } else {
            Next::Reply
        }
    }

    pub(super) fn admit_publication(
        &mut self,
        work: &mut Admission,
        causal: &mut Option<CausalOp>,
    ) {
        assert!(
            self.publication.is_none(),
            "a publication retires before the next admission"
        );
        let Some(pending) = work.pending.take() else {
            return;
        };
        work.retained.admission.input = Some(pending.mutation);
        work.registration = Some(pending.registration);
        work.response = Some(pending.response);
        work.scope_id = pending.scope_id;
        work.token = pending.token;
        work.result = Some(if work.acquired {
            match self.admit_retained(
                work.registration.as_ref().unwrap(),
                work.scope_id,
                work.token,
                &mut work.retained,
            ) {
                Ok(()) => Ok(work
                    .retained
                    .admission
                    .result
                    .take()
                    .expect("admission completed")),
                Err(error) => Err(error),
            }
        } else {
            Err("causal scope no longer exists".into())
        });
        if work.retained.admission.input.is_some() {
            assert!(work.retained.admission.discarded.is_none());
            work.retained.admission.discarded = work.retained.admission.input.take();
        }
        *causal = work.retained.causal;
        let disposed = work.retained.admission.discarded.is_none();
        let release = work
            .scope_id
            .filter(|_| work.acquired && !disposed)
            .map(|scope_id| CausalOp::Release {
                scope_id,
                identity: LeaseIdentity::PendingEntityPublish {
                    publication_token: work.token,
                },
            });
        self.publication = Some(PublicationRetirement {
            drain: work.retained.drain.take(),
            disposed,
            daemon_owned: true,
            response: work
                .response
                .take()
                .expect("the publication retains its response"),
            result: work
                .result
                .take()
                .expect("the publication retains its result"),
            release,
        });
    }

    pub(super) fn advance_publication(&mut self, work: &mut Advance) {
        work.family = self
            .publication
            .as_ref()
            .and_then(|pending| pending.drain.clone());
        let Some((name, generation)) = work.family.as_ref() else {
            work.status = Some(PublicationAdvance::Complete);
            return;
        };
        let (status, result) = self.advance_publish_retained(name, *generation, &mut work.mutation);
        work.status = Some(status);
        let pending = self
            .publication
            .as_mut()
            .expect("the publication remains retained");
        if let Some(result) = result {
            pending.result = Ok(result);
        }
        if work.status == Some(PublicationAdvance::Complete) {
            pending.drain = None;
        }
    }

    pub(super) fn dispose_publication(&mut self, payload: &mut Option<PackageEntityMutation>) {
        let pending = self
            .publication
            .as_mut()
            .expect("disposal retains a publication");
        assert!(
            !pending.disposed && payload.is_some(),
            "disposal retains its payload"
        );
        drop(payload.take());
        pending.disposed = true;
    }

    pub(super) fn release_publication(&mut self, causal: &mut Option<CausalOp>) {
        let pending = self
            .publication
            .as_mut()
            .expect("release retains a publication");
        assert!(pending.disposed && pending.drain.is_none());
        *causal = pending.release;
        pending.release = None;
    }

    pub(super) fn reply_publication(&mut self, work: &mut Reply) {
        assert!(
            self.publication_next() == Next::Reply,
            "the publication is ready to reply"
        );
        work.retired = self.publication.take();
        let retired = work
            .retired
            .as_mut()
            .expect("the response remains retained");
        let result = std::mem::replace(&mut retired.result, Err(String::new()));
        let _ = retired.response.send(result);
    }
}

impl Admission {
    pub(super) fn clear_inputs(&mut self) {
        drop(self.registration.take());
        drop(self.retained.incoming_lease.take());
    }

    pub(super) fn owner_drop_ready(&self) -> bool {
        self.pending.is_none()
            && self.registration.is_none()
            && self.response.is_none()
            && self.result.is_none()
            && self.retained.admission.input.is_none()
            && self.retained.admission.ready.is_none()
            && self.retained.admission.discarded.is_none()
            && self.retained.incoming_lease.is_none()
            && self.retained.ready.is_none()
            && self.retained.drain.is_none()
    }
}

impl Advance {
    pub(super) fn clear_inputs(&mut self) {
        drop(self.family.take());
    }
    pub(super) fn owner_drop_ready(&self) -> bool {
        self.family.is_none() && self.mutation.is_none()
    }
}

impl Reply {
    pub(super) fn clear_inputs(&mut self) {
        drop(self.retired.take());
    }
    pub(super) fn owner_drop_ready(&self) -> bool {
        self.retired.is_none()
    }
}
