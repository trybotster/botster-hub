//! Trusted Lua boundaries installed before plugin code runs.

use mlua::{Function, Lua};

pub(super) fn install(lua: &Lua) -> mlua::Result<()> {
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

    fn control() -> Lua {
        Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8,
            LuaOptions::default(),
        )
        .unwrap()
    }

    fn sandbox() -> Lua {
        let lua = control();
        install(&lua).unwrap();
        lua
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
            collectgarbage("collect")
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
            collectgarbage("collect")
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
            collectgarbage("collect")
            return ok, calls
            "#,
            r#"
            local calls = 0
            local mt = {}
            local object = setmetatable({}, mt)
            mt.__gc = function() calls = calls + 1 end
            local ok = pcall(setmetatable, object, mt)
            object = nil
            collectgarbage("collect")
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
            package_records: Vec::new(),
            package_event_router: hub.package_event_router().clone(),
            causal_scopes: hub.causal_scopes().clone(),
            memory: None,
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
            None,
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
            None,
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
}
