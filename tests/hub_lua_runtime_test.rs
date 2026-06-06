use std::path::PathBuf;

use botster_core::{
    BoundaryJson, PluginHandlerKind, PluginHandlerRef, PluginInvocationContext,
    PluginInvocationFailure, PluginInvocationFailureKind, PluginInvocationRequest,
    PluginInvocationResult, PluginInvocationSuccess, PluginKey, RequestId,
};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubRuntime, HubStartupOptions, LuaPluginRuntime,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, TransportBindings,
    default_package_policy,
};

fn explicit_runtime(name: &str) -> HubRuntime {
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: format!("hub-lua-runtime-test-{name}"),
            display_name: "Hub Lua Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(
            PathBuf::from("target")
                .join("botster-hub-test-data")
                .join("lua-runtime")
                .join(name),
        ),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn install_fixture_registry() -> PackageRegistry {
    let mut policy = default_package_policy();
    policy
        .install_local_path(
            PathBuf::from("examples/synthetic-plugin"),
            "install synthetic lua plugin",
        )
        .expect("install local lua package");
    policy
        .enable("dogfood.synthetic-plugin", "enable synthetic lua plugin")
        .expect("enable local lua package");
    policy.registry().clone()
}

fn invocation(handler: PluginHandlerRef, payload: serde_json::Value) -> PluginInvocationRequest {
    PluginInvocationRequest {
        request_id: RequestId("lua-runtime-invoke".to_string()),
        handler,
        timeout_ms: 1_000,
        context: PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: Some("hub-lua-runtime-test".to_string()),
            metadata: None,
        },
        payload: BoundaryJson(payload),
    }
}

#[test]
fn real_lua_plugin_loads_invokes_tool_and_uses_hub_capability_runtime() {
    let registry = install_fixture_registry();
    let mut hub = explicit_runtime("fixture");

    let plugin_key = hub
        .load_lua_plugin_package(&registry, "dogfood.synthetic-plugin")
        .expect("load real lua plugin package");

    assert_eq!(
        plugin_key,
        PluginKey("dogfood.synthetic-plugin".to_string())
    );
    let tools = hub.list_plugin_mcp_tools();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "dogfood.synthetic.echo");

    let handler = PluginHandlerRef {
        plugin_key,
        kind: PluginHandlerKind::McpTool,
        handler_id: "echo".to_string(),
    };
    let outcome = hub.invoke_plugin(invocation(
        handler,
        serde_json::json!({ "message": "hello" }),
    ));

    let PluginInvocationResult::Completed(PluginInvocationSuccess { payload, .. }) = outcome.result
    else {
        panic!("real lua invocation should complete");
    };
    let payload = payload.expect("lua response payload").0;
    assert_eq!(payload["message"], "hello");
    assert_eq!(payload["ambient"]["os"], true);
    assert_eq!(payload["ambient"]["io"], true);
    assert_eq!(payload["ambient"]["package"], true);
    assert_eq!(payload["capability"]["event_count"], 2);
    assert!(
        payload["capability"]["resource_id"]
            .as_str()
            .is_some_and(|resource| resource.starts_with("timer-"))
    );
}

#[test]
fn plugin_mcp_call_uses_loaded_runtime_and_returns_structured_payload() {
    let registry = install_fixture_registry();
    let mut hub = explicit_runtime("mcp-call");
    hub.load_lua_plugin_package(&registry, "dogfood.synthetic-plugin")
        .expect("load real lua plugin package");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "dogfood.synthetic.echo".to_string(),
            arguments: serde_json::json!({ "message": "from-mcp" }),
        })
        .expect("call plugin MCP tool");

    assert_eq!(result["message"], "from-mcp");
    assert_eq!(result["ambient"]["os"], true);
}

#[test]
fn lua_instruction_budget_reports_structured_handler_failure() {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("runaway-package");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create runaway package root");
    std::fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "runaway.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }, { "surface": "timers", "scope": "callbacks" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write runaway manifest");
    std::fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "runaway.loop",
      description = "Loop through a capability helper until the runtime budget stops execution.",
      handler = "loop",
      call = function()
        while true do
          botster.capabilities.timer_once(1)
        end
      end,
    },
  },
})
"#,
    )
    .expect("write runaway plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install runaway lua package")
        .expect("install runaway package");
    policy
        .enable("runaway.plugin", "enable runaway lua package")
        .expect("enable runaway package");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("runaway");
    hub.load_lua_plugin_package(&registry, "runaway.plugin")
        .expect("load runaway plugin");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "runaway.loop".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect_err("runaway tool should fail");

    assert_eq!(result.code, "plugin_tool_failed");
    assert!(result.message.contains("instruction budget"));
}

#[test]
fn reload_replaces_lua_tool_descriptors_and_removes_stale_handlers() {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("reload-package");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create reload package root");
    std::fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "reload.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "." },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write reload manifest");
    std::fs::write(
        root.join("plugin.lua"),
        reload_plugin_source("reload.old", "old"),
    )
    .expect("write old reload plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install reload lua package")
        .expect("install reload package");
    policy
        .enable("reload.plugin", "enable reload lua package")
        .expect("enable reload package");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("reload");
    hub.load_lua_plugin_package(&registry, "reload.plugin")
        .expect("load old reload plugin");
    assert_eq!(hub.list_plugin_mcp_tools()[0].name, "reload.old");

    std::fs::write(
        root.join("plugin.lua"),
        reload_plugin_source("reload.new", "new"),
    )
    .expect("write new reload plugin");
    let prepared = registry
        .prepare_local_package("reload.plugin", "prepare reload lua package")
        .expect("prepare reload package");
    let bundle = LuaPluginRuntime::load_prepared(
        &prepared,
        hub.capability_runtime(),
        hub.routed_envelope_runtime(),
    )
    .expect("load new reload lua bundle");
    let cleanup = hub
        .reload_plugin_package(
            RequestId("reload-lua-runtime".to_string()),
            &registry,
            "reload.plugin",
            bundle,
        )
        .expect("reload lua plugin");

    assert_eq!(cleanup.removed_descriptors.len(), 1);
    assert_eq!(cleanup.removed_resources.len(), 1);
    let tools = hub.list_plugin_mcp_tools();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "reload.new");
    assert!(
        hub.call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "reload.old".to_string(),
            arguments: serde_json::json!({}),
        })
        .is_err()
    );
    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "reload.new".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("call reloaded tool");
    assert_eq!(result["version"], "new");
}

fn reload_plugin_source(tool_name: &str, version: &str) -> String {
    format!(
        r#"
return botster.register({{
  tools = {{
    {{
      name = "{tool_name}",
      description = "Reload test tool.",
      handler = "run",
      call = function()
        return {{ version = "{version}" }}
      end,
    }},
  }},
}})
"#
    )
}

#[test]
fn invoking_after_unload_fails_through_worker_stopped_path() {
    let registry = install_fixture_registry();
    let mut hub = explicit_runtime("unload");
    let plugin_key = hub
        .load_lua_plugin_package(&registry, "dogfood.synthetic-plugin")
        .expect("load real lua plugin package");
    let _ = hub.unload_plugin_package(
        RequestId("unload-lua".to_string()),
        "dogfood.synthetic-plugin",
    );

    let outcome = hub.invoke_plugin(invocation(
        PluginHandlerRef {
            plugin_key,
            kind: PluginHandlerKind::McpTool,
            handler_id: "echo".to_string(),
        },
        serde_json::json!({}),
    ));

    assert!(matches!(
        outcome.result,
        PluginInvocationResult::Failed(PluginInvocationFailure {
            kind: PluginInvocationFailureKind::WorkerStopped,
            ..
        })
    ));
}
