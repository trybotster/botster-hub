//! Test-only entry points over crate-private terminal binding mechanisms.
//!
//! Compiled only with the `test-internals` feature, which this crate's own
//! dev-dependency enables for integration tests. Every entry returns the
//! same Core ticket the runtime uses internally; nothing here blocks.

use botster_core::contract::terminal_wake::WakingTerminalAdapter;
use botster_core::{
    ClientId, SessionId, SubscriptionId, TerminalCapabilitySet, TerminalSubscriptionGeneration,
};

use crate::data_plane::driver::CoreTicket;
use crate::persistence::{FileCommitError, FileCommitOutcome};
use crate::runtime::{
    AttachBindPlan, BindRoutePlan, HubRuntime, attach_and_bind_on_core, attach_route_on_core,
    bind_route_on_core,
};
use crate::shared_view::SharedView;
use crate::{
    FileHubStateStore, HubConfig, HubState, HubStateStore, HubStateStoreError, HubStateStoreResult,
};

/// Test-only durable state fixture writes through the reserved production path.
pub trait TestHubStateStoreExt {
    fn update_test_fixture(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState>;
}

impl TestHubStateStoreExt for FileHubStateStore {
    fn update_test_fixture(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState> {
        let (mut state, Some(mut authority)) = self.load_retained(config)? else {
            unreachable!("File load returns authority")
        };
        let prior = SharedView::from_reserved(
            state.clone(),
            authority.take_startup_charge().expect("startup charge"),
        );
        update(&mut state);
        let outcome = self.save_retained_startup_state(&authority, 0, Some(prior), state);
        match outcome {
            Ok(FileCommitOutcome::Synced { state, .. }) => Ok((*state).clone()),
            Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                Err(HubStateStoreError::PublishedUncertain(write))
            }
            Err(FileCommitError::Preparation(error))
            | Err(FileCommitError::BeforePublication { error, .. }) => Err(error),
            Err(FileCommitError::Stale(_)) | Err(FileCommitError::RevisionExhausted(_)) => {
                unreachable!("fixture startup revision is fixed")
            }
        }
    }
}

/// A fake or real terminal adapter a test binds to one route.
pub type TestTerminalAdapter = Box<dyn WakingTerminalAdapter + Send>;

/// Attach one client route and bind an adapter to it as one Core operation.
pub struct TestAttachBindPlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: TestTerminalAdapter,
}

/// Bind an adapter to a route that is already attached at `generation`.
pub struct TestBindRoutePlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub generation: TerminalSubscriptionGeneration,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: TestTerminalAdapter,
}

/// Attach and bind on the Core owner thread. Failures are reported as their
/// debug rendering; tests only assert on success or on the failure text.
pub fn attach_and_bind_terminal(
    runtime: &HubRuntime,
    plan: TestAttachBindPlan,
) -> CoreTicket<Result<TerminalSubscriptionGeneration, String>> {
    let plan = AttachBindPlan {
        client_id: plan.client_id,
        session_id: plan.session_id,
        subscription_id: plan.subscription_id,
        capabilities: plan.capabilities,
        now_seconds: plan.now_seconds,
        adapter: plan.adapter,
    };
    runtime.submit_core(move |daemon| {
        attach_and_bind_on_core(daemon, plan).map_err(|error| format!("{error:?}"))
    })
}

/// Attach one client route without binding an adapter.
pub fn attach_route(
    runtime: &HubRuntime,
    client_id: ClientId,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    now_seconds: u64,
) -> CoreTicket<Result<TerminalSubscriptionGeneration, String>> {
    runtime.submit_core(move |daemon| {
        attach_route_on_core(daemon, client_id, session_id, subscription_id, now_seconds)
            .map_err(|error| format!("{error:?}"))
    })
}

/// Bind an adapter to an attached route.
pub fn bind_route_adapter(
    runtime: &HubRuntime,
    plan: TestBindRoutePlan,
) -> CoreTicket<Result<(), String>> {
    let plan = BindRoutePlan {
        client_id: plan.client_id,
        session_id: plan.session_id,
        subscription_id: plan.subscription_id,
        generation: plan.generation,
        capabilities: plan.capabilities,
        now_seconds: plan.now_seconds,
        adapter: plan.adapter,
    };
    runtime.submit_core(move |daemon| {
        bind_route_on_core(daemon, plan).map_err(|error| format!("{error:?}"))
    })
}
