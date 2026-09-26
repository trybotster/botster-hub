//! Trusted Lua boundaries installed before plugin code runs.
//!
//! The Hub opens only the table, string, math, and utf8 libraries, but mlua
//! always opens the base library as well. This module removes or narrows every
//! base global that reaches outside the plugin's own VM or its accounting:
//!
//! - removed: `dofile` and `loadfile` (they read host files), `print` and
//!   `warn` (they write daemon stdio), and `string.dump` (it makes bytecode);
//! - also absent: `os`, `io`, `package`, `require`, and `debug`, which the
//!   selected libraries never open;
//! - narrowed: `load` accepts text chunks only, because binary chunks can break
//!   VM memory safety; `collectgarbage` answers only `"count"`, because the
//!   other options stop, step, or retune the collector that enforces the VM
//!   memory limit; `setmetatable` refuses `__gc` finalizers; `pcall` and
//!   `xpcall` re-raise every failure once the instruction budget is exhausted,
//!   because the budget hook raises an ordinary Lua error that a protected
//!   call could otherwise absorb forever;
//! - protected: the string metatable, which every string in the VM shares.
//!
//! The remaining base globals (`assert`, `error`, `getmetatable`, `ipairs`,
//! `next`, `pairs`, `rawequal`, `rawget`, `rawlen`, `rawset`, `select`,
//! `tonumber`, `tostring`, `type`, `_G`, and `_VERSION`) only read or change
//! values inside the plugin's own VM, so they stay.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mlua::{Function, Lua, Table, Value};

/// Globals that plugins never receive.
const REMOVED_GLOBALS: [&str; 9] = [
    "dofile", "loadfile", "print", "warn", "os", "io", "package", "require", "debug",
];

/// Install the sandbox. `instruction_budget` is the counter that the VM's
/// instruction hook decrements; zero means the current call is out of budget.
/// `budget_error` is the hook's shared error, which protected calls re-raise
/// without allocating a new one.
pub(super) fn install(
    lua: &Lua,
    instruction_budget: Arc<AtomicU64>,
    budget_error: Arc<dyn Error + Send + Sync>,
) -> mlua::Result<()> {
    install_setmetatable(lua)?;
    let globals = lua.globals();
    for name in REMOVED_GLOBALS {
        globals.raw_set(name, Value::Nil)?;
    }
    globals
        .raw_get::<Table>("string")?
        .raw_set("dump", Value::Nil)?;
    let raise_if_exhausted = lua.create_function(move |_, ()| {
        if instruction_budget.load(Ordering::Relaxed) == 0 {
            return Err(mlua::Error::ExternalError(Arc::clone(&budget_error)));
        }
        Ok(())
    })?;
    let (load, collectgarbage, pcall, xpcall): (Function, Function, Function, Function) = lua
        .load(
            r##"
            local raise_if_exhausted = ...
            local base_load, base_collect = load, collectgarbage
            local base_pcall, base_xpcall = pcall, xpcall
            local error, select, getmetatable = error, select, getmetatable
            local pack, unpack = table.pack, table.unpack
            getmetatable("").__metatable = false
            local function settle(results)
                if not results[1] then
                    raise_if_exhausted()
                end
                return unpack(results, 1, results.n)
            end
            local function pcall(...)
                return settle(pack(base_pcall(...)))
            end
            local function xpcall(...)
                return settle(pack(base_xpcall(...)))
            end
            local function load(chunk, name, mode, ...)
                if mode ~= nil and mode ~= "t" then
                    return nil, "plugins can load text chunks only"
                end
                if select("#", ...) == 0 then
                    return base_load(chunk, name, "t")
                end
                return base_load(chunk, name, "t", (...))
            end
            local function collectgarbage(option)
                if option ~= "count" then
                    error('plugins can call collectgarbage("count") only', 2)
                end
                return base_collect("count")
            end
            return load, collectgarbage, pcall, xpcall
            "##,
        )
        .set_name("@hub/sandbox-base")
        .call(raise_if_exhausted)?;
    globals.raw_set("load", load)?;
    globals.raw_set("collectgarbage", collectgarbage)?;
    globals.raw_set("pcall", pcall)?;
    globals.raw_set("xpcall", xpcall)
}

fn install_setmetatable(lua: &Lua) -> mlua::Result<()> {
    let setmetatable = lua
        .load(
            r#"
            local setmetatable, rawget, error, type = setmetatable, rawget, error, type
            return function(...)
                local value, metatable = ...
                if type(value) == "table" and type(metatable) == "table"
                    and rawget(metatable, "__gc") ~= nil then
                    error("plugin __gc finalizers are not supported", 2)
                end
                local result = setmetatable(...)
                return result
            end
            "#,
        )
        .set_name("@hub/sandbox")
        .eval::<Function>()?;
    lua.globals().set("setmetatable", setmetatable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_runtime::*;
    use botster_core::{PluginInvocationContext, RequestId};

    /// The Hub's library selection without the sandbox. Tests collect garbage
    /// through the host because plugins cannot request a full collection.
    fn control() -> Lua {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .unwrap();
        let full_collect = lua.create_function(|lua, ()| lua.gc_collect()).unwrap();
        lua.globals().set("full_collect", full_collect).unwrap();
        lua
    }

    fn sandbox() -> Lua {
        let lua = control();
        install(
            &lua,
            Arc::new(AtomicU64::new(u64::MAX)),
            Arc::new(InstructionBudgetExceeded),
        )
        .unwrap();
        lua
    }

    /// Each probe escapes in the control VM and is blocked in the sandbox, so
    /// every guard is proven separately against the same library selection.
    #[test]
    fn sandbox_blocks_each_base_library_escape_observed_in_control() {
        let directory = TestDirectory::new();
        let host_file = directory.0.join("host.lua");
        std::fs::write(&host_file, "return 'host file ran'").unwrap();
        let bytecode = control()
            .load("return 'bytecode ran'")
            .into_function()
            .unwrap()
            .dump(false);
        let probes: [(&str, &str); 11] = [
            ("dofile", "return pcall(dofile, ...)"),
            (
                "loadfile",
                "local ok, chunk = pcall(loadfile, ...) return ok and chunk ~= nil and chunk()",
            ),
            ("print", "return print ~= nil"),
            ("warn", "return warn ~= nil"),
            (
                "string.dump",
                "return string.dump ~= nil or ('').dump ~= nil",
            ),
            (
                "binary load",
                "local chunk = load(...) return chunk ~= nil and chunk()",
            ),
            (
                "binary mode",
                "local chunk = load(..., 'probe', 'bt') return chunk ~= nil and chunk()",
            ),
            ("collect", "return pcall(collectgarbage, 'collect')"),
            ("stop", "return pcall(collectgarbage, 'stop')"),
            ("default collect", "return pcall(collectgarbage)"),
            ("string metatable", "return getmetatable('') ~= false"),
        ];
        for (name, source) in probes {
            let run = |lua: &Lua| -> Value {
                let argument = match name {
                    "dofile" | "loadfile" => {
                        Value::String(lua.create_string(host_file.to_str().unwrap()).unwrap())
                    }
                    "binary load" | "binary mode" => {
                        Value::String(lua.create_string(&bytecode).unwrap())
                    }
                    _ => Value::Nil,
                };
                lua.load(source)
                    .set_name(name)
                    .call::<Value>(argument)
                    .unwrap_or_else(|error| panic!("{name} probe raised: {error}"))
            };
            let escaped = run(&control());
            assert!(
                !matches!(escaped, Value::Nil | Value::Boolean(false)),
                "{name} must escape without the sandbox, got {escaped:?}"
            );
            let blocked = run(&sandbox());
            assert!(
                matches!(blocked, Value::Nil | Value::Boolean(false)),
                "{name} must be blocked by the sandbox, got {blocked:?}"
            );
        }
    }

    fn external_cause(error: &mlua::Error) -> Option<Arc<dyn Error + Send + Sync>> {
        match error {
            mlua::Error::ExternalError(inner) => Some(Arc::clone(inner)),
            mlua::Error::CallbackError { cause, .. } => external_cause(cause),
            _ => None,
        }
    }

    /// An exhausted budget turns every protected-call failure into the hook's
    /// one shared error, whatever the failure or message handler produced.
    #[test]
    fn sandbox_reraises_the_shared_budget_error() {
        let lua = control();
        let shared: Arc<dyn Error + Send + Sync> = Arc::new(InstructionBudgetExceeded);
        install(&lua, Arc::new(AtomicU64::new(0)), Arc::clone(&shared)).unwrap();
        for source in [
            "pcall(error, 'ordinary')",
            "xpcall(error, function() return nil end, 'ordinary')",
        ] {
            let error = lua.load(source).exec().unwrap_err();
            let cause = external_cause(&error)
                .unwrap_or_else(|| panic!("{source} must raise the budget error: {error}"));
            assert!(Arc::ptr_eq(&cause, &shared), "{source} raised a copy");
        }
        assert!(
            lua.load("return pcall(function() end)")
                .eval::<bool>()
                .unwrap()
        );
    }

    #[test]
    fn sandbox_keeps_text_load_and_memory_count() {
        let lua = sandbox();
        lua.load(
            r#"
            marker = "sandbox global"
            assert(load("return 40 + 2")() == 42)
            assert(load("return marker", "probe", "t")() == "sandbox global")
            assert(load("return marker", "probe", nil, { marker = "own env" })() == "own env")
            assert(load("return marker", "probe", "t", nil) ~= nil)
            local parts = { "return ", "7" }
            local index = 0
            local reader = function() index = index + 1 return parts[index] end
            assert(load(reader)() == 7)
            local chunk, message = load("return (", "broken")
            assert(chunk == nil and message:find("broken", 1, true))
            assert(math.type(collectgarbage("count")) == "float")
            local ok, refusal = pcall(collectgarbage, "step")
            assert(not ok and refusal:find('collectgarbage("count") only', 1, true))
            for _, name in ipairs({ "os", "io", "package", "require", "debug" }) do
                assert(_G[name] == nil, name)
            end
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn sandbox_rejects_raw_finalizers_and_ignores_inherited_values() {
        let lua = sandbox();
        lua.load(
            r#"
            local calls = 0
            local function finalizer() calls = calls + 1 end
            for _, value in ipairs({ finalizer, false, true, "finalizer", 0 }) do
                local mt = { ["__" .. "gc"] = value }
                local object = {}
                local ok, message = pcall(setmetatable, object, mt)
                assert(not ok and type(message) == "string")
                assert(message:match("plugin __gc finalizers are not supported$"))
                assert(getmetatable(object) == nil)
            end
            local inherited = setmetatable({}, {
                __index = function(_, key)
                    assert(key == "__gc")
                    calls = calls + 1
                    return finalizer
                end
            })
            local object = {}
            assert(setmetatable(object, inherited) == object)
            assert(getmetatable(object) == inherited)
            object = nil
            full_collect()
            assert(calls == 0)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn sandbox_rejects_reattachment_after_late_finalizer_insertion() {
        let lua = sandbox();
        lua.load(
            r#"
            local calls = 0
            local mt = {}
            local object = setmetatable({}, mt)
            rawset(mt, "__gc", function() calls = calls + 1 end)
            local ok, message = pcall(setmetatable, object, mt)
            assert(not ok and message:match("plugin __gc finalizers are not supported$"))
            assert(getmetatable(object) == mt)
            assert(not pcall(setmetatable, {}, mt))
            object = nil
            full_collect()
            assert(calls == 0)
            local replacement = {}
            mt.__gc = nil
            assert(setmetatable(replacement, mt) == replacement)
            assert(setmetatable(replacement, nil) == replacement)
            assert(getmetatable(replacement) == nil)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn sandbox_blocks_finite_finalization_observed_in_control() {
        for source in [
            r#"
            local calls = 0
            local ok = pcall(setmetatable, {}, {
                __gc = function() calls = calls + 1 end
            })
            full_collect()
            return ok, calls
            "#,
            r#"
            local calls = 0
            local mt = {}
            local object = setmetatable({}, mt)
            mt.__gc = function() calls = calls + 1 end
            local ok = pcall(setmetatable, object, mt)
            object = nil
            full_collect()
            return ok, calls
            "#,
        ] {
            let observed: (bool, usize) = control().load(source).eval().unwrap();
            assert_eq!(observed, (true, 1));
            let blocked: (bool, usize) = sandbox().load(source).eval().unwrap();
            assert_eq!(blocked, (false, 0));
        }
    }

    #[test]
    fn sandbox_guard_survives_global_function_tampering() {
        let lua = sandbox();
        lua.load(
            r#"
            local guarded = setmetatable
            local check, protected_call = assert, pcall
            local original_type = type
            rawget = function() return nil end
            type = function() return "nil" end
            error = function() return nil end
            setmetatable = function(value) return value end
            local ok, message = protected_call(guarded, {}, { __gc = false })
            check(not ok and original_type(message) == "string")
            check(message:match("plugin __gc finalizers are not supported$"))
            local object = {}
            check(guarded(object, {}) == object)
            check(debug == nil)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn sandbox_preserves_metatable_and_argument_behavior() {
        let source = r#"
            local errors = {}
            local function rejected(...)
                local ok, message = pcall(setmetatable, ...)
                assert(not ok and type(message) == "string")
                errors[#errors + 1] = message
            end
            rejected()
            rejected({})
            rejected(nil, {})
            rejected(false, {})
            rejected({}, false)
            rejected({}, 1)
            local object = {}
            local mt = { __index = { answer = 42 } }
            assert(setmetatable(object, mt, "ignored") == object)
            assert(object.answer == 42 and getmetatable(object) == mt)
            assert(setmetatable(object, nil) == object)
            assert(object.answer == nil)
            local protected = setmetatable({}, { __metatable = false })
            assert(getmetatable(protected) == false)
            rejected(protected, {})
            rejected(protected, nil)
            assert(getmetatable(protected) == false)
            return errors
        "#;
        let control = control();
        let expected: Vec<String> = control.load(source).eval().unwrap();
        let lua = sandbox();
        let actual: Vec<String> = lua.load(source).eval().unwrap();
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(
                actual.strip_prefix("hub/sandbox:9: "),
                Some(expected.as_str())
            );
        }
    }

    #[test]
    fn sandbox_preserves_protected_serde_arrays() {
        let lua = sandbox();
        let array = lua.to_value(&vec![1, 2, 3]).unwrap();
        lua.load(
            r#"
            local array = ...
            assert(debug == nil)
            assert(getmetatable(array) == false)
            assert(#array == 3 and array[2] == 2)
            local ok, message = pcall(setmetatable, array, {})
            assert(not ok and type(message) == "string")
            assert(getmetatable(array) == false)
            "#,
        )
        .call::<()>(array)
        .unwrap();
    }

    struct TestDirectory(std::path::PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let mut random = [0_u8; 16];
            getrandom::fill(&mut random).expect("create a test directory identifier");
            let path = std::env::temp_dir().join(format!(
                "botster-sandbox-{:032x}",
                u128::from_le_bytes(random)
            ));
            std::fs::create_dir(&path).expect("exclusively create the test directory");
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn host_api(hub: &crate::HubRuntime) -> LuaHostApi {
        LuaHostApi {
            configuration: PackageConfigurationView {
                schema: None,
                effective_values: BTreeMap::new(),
                missing_required: Vec::new(),
                diagnostics: Vec::new(),
            },
            capabilities: hub.capability_runtime(),
            coordination: hub.coordination_bridge(),
            entity_publish: hub.entity_publish_bridge(),
            session_types: hub.session_type_spawner(),
            spawn_targets: hub.spawn_targets(),
            worktrees: hub.worktrees(),
            package_registry: hub.package_registry_publication(),
            package_event_router: hub.package_event_router().clone(),
            causal_scopes: hub.causal_scopes().clone(),
            memory: hub.lua_plugin_host_api().memory,
        }
    }

    fn invoke(runtime: &LuaPluginRuntime, handler_id: &str) -> PluginInvocationResult {
        runtime.invoke(
            PluginInvocationRequest {
                request_id: RequestId("sandbox-test".into()),
                handler: PluginHandlerRef {
                    plugin_key: PluginKey("sandbox-test.plugin".into()),
                    kind: PluginHandlerKind::McpTool,
                    handler_id: handler_id.into(),
                },
                timeout_ms: 1_000,
                context: PluginInvocationContext {
                    client_id: None,
                    session_id: None,
                    subscription_id: None,
                    surface_id: None,
                    origin: None,
                    metadata: None,
                },
                payload: BoundaryJson(json!({})),
            },
            PluginCancellationToken::new(),
        )
    }

    #[test]
    fn sandbox_rejects_finalizers_during_load_and_invocation() {
        let directory = TestDirectory::new();
        let root = &directory.0;
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(root.join("hub")),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let hub = crate::HubRuntime::new(config).unwrap();
        let entrypoint = root.join("plugin.lua");
        std::fs::write(
            &entrypoint,
            "setmetatable({}, { __gc = function() end }); return {}",
        )
        .unwrap();
        let result = LuaPluginRuntime::new(
            PluginKey("sandbox-test.plugin".into()),
            &entrypoint,
            host_api(&hub),
            hub.lua_plugin_host_api().memory,
        );
        let Err(error) = result else {
            panic!("the entrypoint must not register a finalizer");
        };
        assert!(
            error
                .to_string()
                .contains("plugin __gc finalizers are not supported")
        );
        std::fs::write(
            &entrypoint,
            r#"
            assert(debug == nil)
            local count = 0
            __botster_handlers.run = function()
                count = count + 1
                local ok, message = pcall(setmetatable, {}, { __gc = false })
                assert(not ok and type(message) == "string")
                return { count = count, message = message }
            end
            __botster_handlers.reject = function()
                setmetatable({}, { __gc = function() end })
            end
            return {}
            "#,
        )
        .unwrap();
        let (runtime, _) = LuaPluginRuntime::new(
            PluginKey("sandbox-test.plugin".into()),
            &entrypoint,
            host_api(&hub),
            hub.lua_plugin_host_api().memory,
        )
        .unwrap();
        for count in 1..=2 {
            let PluginInvocationResult::Completed(success) = invoke(&runtime, "run") else {
                panic!("ordinary invocation must complete after refusal");
            };
            let payload = success.payload.unwrap().0;
            assert_eq!(payload["count"], count);
            assert_eq!(
                payload["message"],
                "plugin __gc finalizers are not supported"
            );
            let PluginInvocationResult::Failed(failure) = invoke(&runtime, "reject") else {
                panic!("the invocation must not register a finalizer");
            };
            assert!(
                failure
                    .reason
                    .contains("plugin __gc finalizers are not supported")
            );
        }
        drop(runtime);
        drop(hub);
    }

    /// Without the guard, each protected call absorbs the hook's budget error
    /// and the loop completes far beyond the budget; with it, the call fails.
    #[test]
    fn sandbox_protected_calls_cannot_absorb_the_instruction_budget() {
        let directory = TestDirectory::new();
        let root = &directory.0;
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(root.join("hub")),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let hub = crate::HubRuntime::new(config).unwrap();
        let entrypoint = root.join("plugin.lua");
        std::fs::write(
            &entrypoint,
            r#"
            local function spin() for _ = 1, 2000 do end end
            __botster_handlers.ordinary = function()
                local ok, message = pcall(error, "ordinary failure")
                assert(not ok and message == "ordinary failure")
                ok, message = xpcall(error, function(caught) return "handled " .. caught end, "x")
                assert(not ok and message == "handled x")
                return { caught = true }
            end
            __botster_handlers.pcall_spin = function()
                for _ = 1, 100000 do pcall(spin) end
                return { survived = true }
            end
            __botster_handlers.xpcall_spin = function()
                for _ = 1, 100000 do xpcall(spin, function() end) end
                return { survived = true }
            end
            return {}
            "#,
        )
        .unwrap();
        let (runtime, _) = LuaPluginRuntime::new(
            PluginKey("sandbox-test.plugin".into()),
            &entrypoint,
            host_api(&hub),
            hub.lua_plugin_host_api().memory,
        )
        .unwrap();
        for _ in 1..=2 {
            for handler in ["pcall_spin", "xpcall_spin"] {
                let PluginInvocationResult::Failed(failure) = invoke(&runtime, handler) else {
                    panic!("{handler} must not absorb the instruction budget");
                };
                assert!(
                    failure.reason.contains("instruction budget"),
                    "{handler} failed for another reason: {}",
                    failure.reason
                );
            }
            let PluginInvocationResult::Completed(success) = invoke(&runtime, "ordinary") else {
                panic!("protected calls must still catch ordinary errors");
            };
            assert_eq!(success.payload.unwrap().0["caught"], true);
        }
        drop(runtime);
        drop(hub);
    }
}
