//! Test-only entry points over crate-private terminal binding mechanisms.
//!
//! Compiled only with the `test-internals` feature, which this crate's own
//! dev-dependency enables for integration tests. Every entry returns the
//! same Core ticket the runtime uses internally; nothing here blocks.

/// Source seams used only by the dedicated allocation test executable.
#[cfg(feature = "allocation-oracle")]
pub mod allocation_oracle {
    pub use crate::data_plane::driver::allocation_oracle::{Phase, Scenario, run, type_layouts};
}

#[cfg(feature = "allocation-oracle")]
pub mod hub_state_heap {
    pub use crate::hub_state_heap::{HeapWalk, admitted_pretty, walk_hub_state};
}

#[cfg(feature = "allocation-oracle")]
pub mod charged_collection {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use crate::lua_memory::charged_collection::{ChargedVec, ChargedVecDeque};
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    use crate::lua_runtime::PendingCoordinationRequest;
    use crate::runtime::InflightPluginCore;

    pub struct PendingQueue {
        inner: ChargedVecDeque<MaybeUninit<PendingCoordinationRequest>>,
    }

    pub struct InflightQueue {
        inner: ChargedVec<MaybeUninit<InflightPluginCore>>,
    }

    fn account(bytes: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: bytes.max(1),
            total_vm_bytes: bytes.max(1),
            per_callback_bytes: bytes.max(1),
            total_callback_bytes: bytes.max(1),
        })
        .unwrap()
    }

    impl PendingQueue {
        pub fn new() -> Self {
            let slot = std::mem::size_of::<PendingCoordinationRequest>();
            Self {
                inner: ChargedVecDeque::new(account(slot * 16)),
            }
        }

        pub fn slot() -> usize {
            std::mem::size_of::<PendingCoordinationRequest>()
        }

        pub fn try_push_uninit(&mut self) -> Result<(), String> {
            self.inner
                .try_push_back(MaybeUninit::uninit())
                .map_err(|error| error.to_string())
        }

        pub fn capacity(&self) -> usize {
            self.inner.capacity()
        }

        pub fn charge_bytes(&self) -> usize {
            self.inner.charge_bytes()
        }
    }

    impl InflightQueue {
        pub fn new() -> Self {
            let slot = std::mem::size_of::<InflightPluginCore>();
            Self {
                inner: ChargedVec::new(account(slot * 16)),
            }
        }

        pub fn slot() -> usize {
            std::mem::size_of::<InflightPluginCore>()
        }

        pub fn try_push_uninit(&mut self) -> Result<(), String> {
            self.inner
                .try_push(MaybeUninit::uninit())
                .map_err(|error| error.to_string())
        }

        pub fn capacity(&self) -> usize {
            self.inner.capacity()
        }

        pub fn charge_bytes(&self) -> usize {
            self.inner.charge_bytes()
        }
    }
}

use botster_core::contract::terminal_wake::WakingTerminalAdapter;
use botster_core::{
    ClientId, SessionId, SubscriptionId, TerminalCapabilitySet, TerminalSubscriptionGeneration,
};

use crate::data_plane::driver::CoreTicket;
use crate::runtime::{
    AttachBindPlan, BindRoutePlan, HubRuntime, attach_and_bind_on_core, attach_route_on_core,
    bind_route_on_core,
};
use crate::shared_view::SharedViewBudget;
use crate::{FileHubStateStore, HubConfig, HubState, HubStateStoreResult};

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
        let mut state = self.load_for_update(config)?;
        update(&mut state);
        let prepared = self.prepare_shared(state, &SharedViewBudget::new())?;
        self.commit_shared(prepared).map(|state| (*state).clone())
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
