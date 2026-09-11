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
pub use crate::lua_runtime::{HookRaiseStorm, prepare_hook_raise_storm};

#[cfg(feature = "allocation-oracle")]
pub struct CapacityRaiseStorm {
    lua: mlua::Lua,
    publish: mlua::Function,
    drain: mlua::Function,
    _capacity_string: crate::lua_memory::LuaCallbackCharge,
}

#[cfg(feature = "allocation-oracle")]
pub fn prepare_capacity_raise_storm() -> CapacityRaiseStorm {
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    use crate::lua_runtime::HubCoordinationBridge;
    use botster_core::PluginKey;
    use std::sync::Arc;

    let xrc = 2 * crate::lua_memory::layout::lua_reference_bytes();
    let memory = LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 1024 * 1024,
        total_vm_bytes: 1024 * 1024,
        per_callback_bytes: xrc,
        total_callback_bytes: xrc,
    })
    .expect("capacity raise account");
    let lua = mlua::Lua::new();
    let (table, capacity_string) = crate::lua_runtime::coordination_table(
        &lua,
        PluginKey("oracle.plugin".into()),
        HubCoordinationBridge::new(Arc::clone(&memory)),
        memory,
    )
    .expect("coordination table");
    lua.globals()
        .set("coordination", table)
        .expect("set coordination");
    lua.load(
        r#"
        kept = {}; for i = 1, 4000 do kept[i] = false end
        function storm_publish(n)
            for i = 1, n do
                local ok, err = pcall(coordination.publish, { id = 'e1', target = { type = 'topic', topic = 't' } })
                assert(not ok, tostring(err))
                kept[i] = err
            end
            for i = 2, n do
                assert(rawequal(kept[1], kept[i]))
            end
        end
        function storm_drain(n)
            for i = 1, n do
                local ok, err = pcall(coordination.drain, { target = { type = 'topic', topic = 't' } })
                assert(not ok, tostring(err))
                kept[i] = err
            end
            for i = 2, n do
                assert(rawequal(kept[1], kept[i]))
            end
        end
        "#,
    )
    .exec()
    .expect("pre-size retained error table");
    let publish = lua.globals().get("storm_publish").expect("storm_publish");
    let drain = lua.globals().get("storm_drain").expect("storm_drain");
    CapacityRaiseStorm {
        lua,
        publish,
        drain,
        _capacity_string: capacity_string,
    }
}

#[cfg(feature = "allocation-oracle")]
impl CapacityRaiseStorm {
    pub fn used_memory(&self) -> usize {
        self.lua.used_memory()
    }

    pub fn retain_drain_errors(&self, n: u32) -> Result<(), String> {
        self.drain.call(n).map_err(|error| error.to_string())
    }
}

#[cfg(feature = "allocation-oracle")]
impl CapacityRaiseStorm {
    pub fn retain_publish_errors(&self, n: u32) -> Result<(), String> {
        self.publish.call(n).map_err(|error| error.to_string())
    }
}

#[cfg(feature = "allocation-oracle")]
pub mod lua_json {
    use mlua::{Lua, Value};

    pub struct Prepared {
        lua: Lua,
        value: Value,
        memory: std::sync::Arc<crate::lua_memory::LuaMemoryAccount>,
    }

    pub fn prepare(source: &str) -> Prepared {
        let lua = Lua::new();
        let value: Value = lua.load(source).eval().expect("lua json oracle source");
        let memory = crate::lua_memory::LuaMemoryAccount::new(crate::lua_memory::LuaMemoryLimits {
            per_vm_bytes: 1024 * 1024,
            total_vm_bytes: 1024 * 1024,
            per_callback_bytes: 1024 * 1024,
            total_callback_bytes: 1024 * 1024,
        })
        .expect("lua json oracle account");
        Prepared { lua, value, memory }
    }

    pub fn live_refs_peak(prepared: &Prepared) -> usize {
        crate::lua_runtime::lua_json::value_size(&prepared.memory, &prepared.lua, &prepared.value)
            .expect("lua json size")
            .live_refs_peak
    }

    pub fn admitted(prepared: &Prepared) -> usize {
        let admission = crate::lua_runtime::lua_json::value_size(
            &prepared.memory,
            &prepared.lua,
            &prepared.value,
        )
        .expect("lua json size");
        admission
            .json_bytes
            .checked_add(admission.scratch_peak)
            .expect("lua json admission")
    }

    pub fn build(prepared: &Prepared) -> serde_json::Value {
        let admission = crate::lua_runtime::lua_json::value_size(
            &prepared.memory,
            &prepared.lua,
            &prepared.value,
        )
        .expect("lua json size");
        let prepaid = admission
            .prepaid(&prepared.memory)
            .expect("lua json scratch");
        crate::lua_runtime::lua_json::value_build(&prepared.lua, &prepared.value, prepaid)
            .expect("lua json build")
    }
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
                inner: ChargedVecDeque::new(account(slot * 32)),
            }
        }

        pub fn slot() -> usize {
            std::mem::size_of::<PendingCoordinationRequest>()
        }

        pub fn type_size() -> usize {
            std::mem::size_of::<PendingCoordinationRequest>()
        }

        pub fn type_align() -> usize {
            std::mem::align_of::<PendingCoordinationRequest>()
        }

        pub fn uninit_size() -> usize {
            std::mem::size_of::<MaybeUninit<PendingCoordinationRequest>>()
        }

        pub fn uninit_align() -> usize {
            std::mem::align_of::<MaybeUninit<PendingCoordinationRequest>>()
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
                inner: ChargedVec::new(account(slot * 32)),
            }
        }

        pub fn slot() -> usize {
            std::mem::size_of::<InflightPluginCore>()
        }

        pub fn type_size() -> usize {
            std::mem::size_of::<InflightPluginCore>()
        }

        pub fn type_align() -> usize {
            std::mem::align_of::<InflightPluginCore>()
        }

        pub fn uninit_size() -> usize {
            std::mem::size_of::<MaybeUninit<InflightPluginCore>>()
        }

        pub fn uninit_align() -> usize {
            std::mem::align_of::<MaybeUninit<InflightPluginCore>>()
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
