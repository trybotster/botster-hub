use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use botster_core::{
    BoundaryJson, Capability, CapabilitySurface, PackageConfigurationSecretValue,
    PackageConfigurationValue, PluginHandlerKind, PluginHandlerRef, PluginInvocationContext,
    PluginInvocationFailure, PluginInvocationFailureKind, PluginInvocationRequest,
    PluginInvocationResult, PluginInvocationSuccess, PluginKey, RequestId, UiActionResultState,
    UiNodeKind,
};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubClientApi, HubClientRequest,
    HubClientResponseBody, HubRuntime, HubStartupOptions, LuaPluginHostApi, LuaPluginRuntime,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings, Worktree,
    default_package_policy,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    ensure_session_worker_binary();
    let data_directory = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join(name);
    let _ = std::fs::remove_dir_all(&data_directory);
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: format!("hub-lua-runtime-test-{name}"),
            display_name: "Hub Lua Runtime Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory),
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

fn configured_fixture_registry() -> PackageRegistry {
    let mut policy = default_package_policy();
    policy
        .install_local_path(
            PathBuf::from("examples/synthetic-plugin"),
            "install configured synthetic lua plugin",
        )
        .expect("install local lua package");
    policy
        .registry_mut()
        .set_configuration(
            "dogfood.synthetic-plugin",
            [
                (
                    "endpoint".to_string(),
                    PackageConfigurationValue::Url {
                        value: "https://operator.example.invalid/hook".to_string(),
                    },
                ),
                (
                    "api_token".to_string(),
                    PackageConfigurationValue::Secret {
                        state: PackageConfigurationSecretValue::WriteOnly,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            "set synthetic lua plugin configuration",
        )
        .expect("set package configuration");
    policy
        .enable("dogfood.synthetic-plugin", "enable synthetic lua plugin")
        .expect("enable local lua package");
    policy.registry().clone()
}

fn install_project_pipelines_registry() -> PackageRegistry {
    let mut policy = default_package_policy();
    policy
        .install_local_path(
            PathBuf::from("examples/project-pipelines"),
            "install project pipelines lua plugin",
        )
        .expect("install project pipelines package");
    policy
        .enable("project-pipelines", "enable project pipelines package")
        .expect("enable project pipelines package");
    policy.registry().clone()
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn install_session_template_spawn_registry(
    name: &str,
    capabilities: Vec<Capability>,
) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("bin")).expect("create session-template package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "session_template.spawn",
      description = "Spawn a declared session template through the production Lua capability.",
      handler = "spawn",
      call = function(args)
        return botster.capabilities.session_templates.spawn(args)
      end,
    },
  },
  handlers = {
    {
      id = "spawn_action",
      kind = "ui_action",
      descriptor_id = "session_template.spawn_action",
      descriptor = {
        action_id = "session_template.spawn_action",
        surface_id = "session-template-spawner.surface",
      },
      call = function(args)
        local spawned = botster.capabilities.session_templates.spawn(args)
        return {
          request_id = args.request_id or "spawn-action",
          surface_id = "session-template-spawner.surface",
          action_id = "session_template.spawn_action",
          node_id = "session-template-spawner-form",
          state = "accepted",
          payload = spawned,
        }
      end,
    },
  },
})
"#,
    )
    .expect("write session-template plugin");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'template:%s:%s\\n' \"$BOTSTER_SESSION_ID\" \"$BOTSTER_MODE\"\n",
    )
    .expect("write session-template script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session-template script");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "session-template-spawner.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": capabilities,
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "session_templates": [{
                "id": "init",
                "command": "bin/init.sh",
                "environment": { "BOTSTER_MODE": "default" },
                "allowed_environment_overrides": ["BOTSTER_MODE"],
                "context": ["prompt", "ticket_id"]
            }]
        })
        .to_string(),
    )
    .expect("write session-template package manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install session-template spawn lua package")
        .expect("install session-template package");
    policy
        .enable(
            "session-template-spawner.plugin",
            "enable session-template package",
        )
        .expect("enable session-template package");
    policy.registry().clone()
}

fn install_spawn_target_reader_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create spawn-target package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "spawn_targets.inspect",
      description = "Inspect hub-owned spawn target capability shape.",
      handler = "inspect",
      call = function(args)
        local targets = botster.capabilities.spawn_targets.list()
        local validation = botster.capabilities.spawn_targets.validate({ target_id = args.target_id })
        local disabled = botster.capabilities.spawn_targets.validate({ target_id = args.disabled_target_id })
        local missing = botster.capabilities.spawn_targets.validate({ target_id = "missing-target" })
        return {
          targets = targets,
          validation = validation,
          disabled = disabled,
          missing = missing,
          mutation_methods = {
            create = botster.capabilities.spawn_targets.create ~= nil,
            update = botster.capabilities.spawn_targets.update ~= nil,
            delete = botster.capabilities.spawn_targets.delete ~= nil,
          },
        }
      end,
    },
  },
})
"#,
    )
    .expect("write spawn-target plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "spawn-target-reader.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write spawn-target package manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install spawn target reader package")
        .expect("install spawn target reader package");
    policy
        .enable("spawn-target-reader.plugin", "enable spawn target reader")
        .expect("enable spawn target reader package");
    policy.registry().clone()
}

fn install_worktree_reader_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create worktree package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "worktrees.inspect",
      description = "Inspect hub-owned worktree capability shape.",
      handler = "inspect",
      call = function(args)
        local worktrees = botster.capabilities.worktrees.list()
        local shown = botster.capabilities.worktrees.show({ worktree_id = args.worktree_id })
        local missing = botster.capabilities.worktrees.show({ worktree_id = "missing-worktree" })
        return {
          worktrees = worktrees,
          shown = shown,
          missing = missing,
          mutation_methods = {
            create = botster.capabilities.worktrees.create ~= nil,
            update = botster.capabilities.worktrees.update ~= nil,
            delete = botster.capabilities.worktrees.delete ~= nil,
          },
        }
      end,
    },
  },
})
"#,
    )
    .expect("write worktree plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "worktree-reader.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write worktree package manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install worktree reader package")
        .expect("install worktree reader package");
    policy
        .enable("worktree-reader.plugin", "enable worktree reader")
        .expect("enable worktree reader package");
    policy.registry().clone()
}

fn install_plugin_db_probe_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create plugin-db probe package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
local plugin_db = botster.capabilities.plugin_db

return botster.register({
  tools = {
    {
      name = "plugin_db_probe.missing_get",
      description = "Read a missing plugin_db record through the production Lua helper.",
      handler = "missing_get",
      call = function()
        local result = plugin_db.get({ key = "missing-state" })
        return {
          kind = result.kind,
          record_is_nil = result.record == nil,
          record_type = type(result.record),
        }
      end,
    },
    {
      name = "plugin_db_probe.successful_get",
      description = "Write then read a plugin_db record through the production Lua helper.",
      handler = "successful_get",
      call = function()
        plugin_db.set({
          key = "state",
          schema_version = 1,
          payload = { count = 2, label = "stored" },
        })
        local result = plugin_db.get({ key = "state" })
        return {
          kind = result.kind,
          record_payload = result.record.payload,
          record_revision = result.record.revision,
        }
      end,
    },
    {
      name = "plugin_db_probe.missing_writes",
      description = "Prove missing patch and delete still raise runtime errors.",
      handler = "missing_writes",
      call = function()
        local patch_ok, patch_error = pcall(plugin_db.patch, {
          key = "missing-state",
          patch = { archived = true },
        })
        local delete_ok, delete_error = pcall(plugin_db.delete, {
          key = "missing-state",
        })
        return {
          patch_ok = patch_ok,
          patch_error = tostring(patch_error),
          delete_ok = delete_ok,
          delete_error = tostring(delete_error),
        }
      end,
    },
  },
})
"#,
    )
    .expect("write plugin-db probe plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "botster-workspaces",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [
                { "surface": "mcp" },
                { "surface": "plugin_db", "scope": "botster-workspaces" }
            ],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write plugin-db probe manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install plugin-db probe lua package")
        .expect("install plugin-db probe package");
    policy
        .enable("botster-workspaces", "enable plugin-db probe package")
        .expect("enable plugin-db probe package");
    policy.registry().clone()
}

fn install_event_probe_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create event probe package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
events.on("worktree_created", function(event)
  return {
    received = event.event,
    worktree_id = event.worktree_id,
    target_id = event.target_id,
  }
end)

events.on("worktree_deleted", function(event)
  return {
    received = event.event,
    worktree_id = event.worktree_id,
  }
end)

events.on("worktree_created", function(event)
  return {
    received = event.event,
    observer = "second",
    worktree_id = event.worktree_id,
  }
end)

return botster.register({})
"#,
    )
    .expect("write event probe plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "event-probe.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write event probe manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install event probe lua package")
        .expect("install event probe package");
    policy
        .enable("event-probe.plugin", "enable event probe package")
        .expect("enable event probe package");
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
fn events_on_registers_exact_event_subscription_and_invokes_worker_handler() {
    let registry = install_event_probe_registry("event-probe");
    let mut hub = explicit_runtime("event-probe");
    hub.load_lua_plugin_package(&registry, "event-probe.plugin")
        .expect("load event probe plugin");

    let outcomes = hub.emit_plugin_event(
        "worktree_created",
        serde_json::json!({
            "event": "worktree_created",
            "worktree_id": "wt_1",
            "target_id": "tgt_1",
        }),
    );
    assert_eq!(outcomes.len(), 2);
    let payloads = completed_payloads(&outcomes);
    assert!(payloads.contains(&serde_json::json!({
        "received": "worktree_created",
        "worktree_id": "wt_1",
        "target_id": "tgt_1",
    })));
    assert!(payloads.contains(&serde_json::json!({
        "received": "worktree_created",
        "observer": "second",
        "worktree_id": "wt_1",
    })));

    let deleted = hub.emit_plugin_event(
        "worktree_deleted",
        serde_json::json!({
            "event": "worktree_deleted",
            "worktree_id": "wt_1",
        }),
    );
    assert_eq!(deleted.len(), 1);
    assert_eq!(
        completed_payloads(&deleted),
        vec![serde_json::json!({
            "received": "worktree_deleted",
            "worktree_id": "wt_1",
        })]
    );

    let unmatched = hub.emit_plugin_event(
        "worktree_create_failed",
        serde_json::json!({ "event": "worktree_create_failed" }),
    );
    assert!(
        unmatched.is_empty(),
        "event subscriptions should match exact event names"
    );
}

fn completed_payloads(
    outcomes: &[botster_core::PluginInvocationOutcome],
) -> Vec<serde_json::Value> {
    outcomes
        .iter()
        .map(|outcome| {
            let botster_core::PluginInvocationResult::Completed(success) = &outcome.result else {
                panic!("event handler should complete: {:?}", outcome.result);
            };
            success
                .payload
                .as_ref()
                .map(|payload| payload.0.clone())
                .expect("event handler should return payload")
        })
        .collect()
}

#[test]
fn plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape() {
    let registry = install_plugin_db_probe_registry("plugin-db-missing-get");
    let mut hub = explicit_runtime("plugin-db-missing-get");
    hub.load_lua_plugin_package(&registry, "botster-workspaces")
        .expect("load plugin-db probe package");

    let missing = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_probe.missing_get".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("missing plugin_db.get should return absent data");

    assert_eq!(missing["kind"], "record");
    assert_eq!(missing["record_is_nil"], true);
    assert_eq!(missing["record_type"], "nil");

    let present = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_probe.successful_get".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("successful plugin_db.get should preserve record shape");

    assert_eq!(present["kind"], "record");
    assert_eq!(
        present["record_payload"],
        serde_json::json!({ "count": 2, "label": "stored" })
    );
    assert_eq!(present["record_revision"], 1);
}

#[test]
fn plugin_db_missing_patch_and_delete_still_raise_runtime_errors() {
    let registry = install_plugin_db_probe_registry("plugin-db-missing-writes");
    let mut hub = explicit_runtime("plugin-db-missing-writes");
    hub.load_lua_plugin_package(&registry, "botster-workspaces")
        .expect("load plugin-db probe package");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_probe.missing_writes".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("missing patch/delete probe should return pcall results");

    assert_eq!(result["patch_ok"], false);
    assert!(
        result["patch_error"]
            .as_str()
            .unwrap()
            .contains("plugin_db operation failed: plugin-store record was not found")
    );
    assert_eq!(result["delete_ok"], false);
    assert!(
        result["delete_error"]
            .as_str()
            .unwrap()
            .contains("plugin_db operation failed: plugin-store record was not found")
    );
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
    assert_eq!(
        payload["config"]["values"]["endpoint"]["value"],
        "https://example.invalid/hook"
    );
    assert_eq!(payload["config"]["values"]["mode"]["value"], "read");
    assert!(payload["config"]["values"].get("api_token").is_none());
    assert_eq!(payload["cross_package_config_attempt"]["ok"], false);
    assert_eq!(payload["capability"]["event_count"], 2);
    assert!(
        payload["capability"]["resource_id"]
            .as_str()
            .is_some_and(|resource| resource.starts_with("timer-"))
    );
}

#[test]
fn real_lua_plugin_lists_and_validates_spawn_targets_without_mutation_surface() {
    let registry = install_spawn_target_reader_registry("spawn-target-reader");
    let mut hub = explicit_runtime("spawn-target-reader");
    let target_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("spawn-target-reader-target");
    fs::create_dir_all(&target_root).expect("create target root");
    let mut state = hub.state().clone();
    state.spawn_targets = vec![
        SpawnTarget {
            target_id: "tgt_lua_enabled".to_string(),
            label: "Lua Enabled".to_string(),
            root: target_root.clone(),
            enabled: true,
            kind: "directory".to_string(),
            metadata: BTreeMap::new(),
        },
        SpawnTarget {
            target_id: "tgt_lua_disabled".to_string(),
            label: "Lua Disabled".to_string(),
            root: target_root,
            enabled: false,
            kind: "directory".to_string(),
            metadata: BTreeMap::new(),
        },
    ];
    hub.replace_state(state);
    hub.load_lua_plugin_package(&registry, "spawn-target-reader.plugin")
        .expect("load spawn target reader plugin");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "spawn_targets.inspect".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_lua_enabled",
                "disabled_target_id": "tgt_lua_disabled"
            }),
        })
        .expect("call spawn target reader tool");

    assert_eq!(result["targets"].as_array().expect("target list").len(), 2);
    assert_eq!(result["validation"]["ok"], true);
    assert_eq!(result["validation"]["status"], "ok");
    assert_eq!(result["disabled"]["ok"], false);
    assert_eq!(result["disabled"]["status"], "disabled");
    assert_eq!(result["missing"]["ok"], false);
    assert_eq!(result["missing"]["status"], "not_found");
    assert_eq!(result["mutation_methods"]["create"], false);
    assert_eq!(result["mutation_methods"]["update"], false);
    assert_eq!(result["mutation_methods"]["delete"], false);
}

#[test]
fn real_lua_plugin_lists_and_shows_worktrees_without_mutation_surface() {
    let registry = install_worktree_reader_registry("worktree-reader");
    let mut hub = explicit_runtime("worktree-reader");
    let target_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("worktree-reader-target");
    let worktree_path = target_root.join("plain");
    fs::create_dir_all(&worktree_path).expect("create worktree path");
    let mut state = hub.state().clone();
    state.spawn_targets = vec![SpawnTarget {
        target_id: "tgt_lua_worktrees".to_string(),
        label: "Lua Worktrees".to_string(),
        root: target_root,
        enabled: true,
        kind: "directory".to_string(),
        metadata: BTreeMap::new(),
    }];
    state.worktrees = vec![Worktree {
        worktree_id: "wt_lua_plain".to_string(),
        target_id: "tgt_lua_worktrees".to_string(),
        label: "Lua Plain".to_string(),
        path: worktree_path,
        status: "present".to_string(),
        git: None,
        metadata: BTreeMap::new(),
    }];
    hub.replace_state(state);
    hub.load_lua_plugin_package(&registry, "worktree-reader.plugin")
        .expect("load worktree reader plugin");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "worktrees.inspect".to_string(),
            arguments: serde_json::json!({ "worktree_id": "wt_lua_plain" }),
        })
        .expect("call worktree reader tool");

    assert_eq!(
        result["worktrees"].as_array().expect("worktree list").len(),
        1
    );
    assert_eq!(result["worktrees"][0]["status"], "present");
    assert_eq!(result["shown"]["ok"], true);
    assert_eq!(result["shown"]["status"], "present");
    assert_eq!(result["shown"]["worktree"]["worktree_id"], "wt_lua_plain");
    assert_eq!(result["missing"]["ok"], false);
    assert_eq!(result["missing"]["status"], "not_found");
    assert_eq!(result["mutation_methods"]["create"], false);
    assert_eq!(result["mutation_methods"]["update"], false);
    assert_eq!(result["mutation_methods"]["delete"], false);
}

#[test]
fn real_lua_plugin_observes_worktrees_added_after_plugin_load() {
    let registry = install_worktree_reader_registry("worktree-live-refresh");
    let mut hub = explicit_runtime("worktree-live-refresh");
    let target_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("worktree-live-refresh-target");
    let worktree_path = target_root.join("late");
    fs::create_dir_all(&worktree_path).expect("create late worktree path");
    let mut state = hub.state().clone();
    state.spawn_targets = vec![SpawnTarget {
        target_id: "tgt_lua_worktrees".to_string(),
        label: "Lua Worktrees".to_string(),
        root: target_root,
        enabled: true,
        kind: "directory".to_string(),
        metadata: BTreeMap::new(),
    }];
    hub.replace_state(state.clone());
    hub.load_lua_plugin_package(&registry, "worktree-reader.plugin")
        .expect("load worktree reader plugin");

    state.worktrees = vec![Worktree {
        worktree_id: "wt_lua_late".to_string(),
        target_id: "tgt_lua_worktrees".to_string(),
        label: "Lua Late".to_string(),
        path: worktree_path,
        status: "present".to_string(),
        git: None,
        metadata: BTreeMap::new(),
    }];
    hub.replace_state(state);

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "worktrees.inspect".to_string(),
            arguments: serde_json::json!({ "worktree_id": "wt_lua_late" }),
        })
        .expect("call worktree reader tool after state refresh");

    assert_eq!(
        result["worktrees"].as_array().expect("worktree list").len(),
        1
    );
    assert_eq!(result["shown"]["ok"], true);
    assert_eq!(result["shown"]["worktree"]["worktree_id"], "wt_lua_late");
}

#[test]
fn real_lua_plugin_reads_operator_config_and_redacted_secret_from_own_package_only() {
    let registry = configured_fixture_registry();
    let mut hub = explicit_runtime("configured-fixture");

    let plugin_key = hub
        .load_lua_plugin_package(&registry, "dogfood.synthetic-plugin")
        .expect("load real lua plugin package");
    let handler = PluginHandlerRef {
        plugin_key,
        kind: PluginHandlerKind::McpTool,
        handler_id: "echo".to_string(),
    };
    let outcome = hub.invoke_plugin(invocation(
        handler,
        serde_json::json!({ "message": "configured" }),
    ));

    let PluginInvocationResult::Completed(PluginInvocationSuccess { payload, .. }) = outcome.result
    else {
        panic!("real lua invocation should complete");
    };
    let payload = payload.expect("lua response payload").0;
    assert_eq!(
        payload["config"]["values"]["endpoint"]["value"],
        "https://operator.example.invalid/hook"
    );
    assert_eq!(payload["config"]["values"]["mode"]["value"], "read");
    assert_eq!(
        payload["config"]["values"]["api_token"]["state"],
        "redacted"
    );
    assert_eq!(payload["config"]["missing_required"], serde_json::json!([]));
    assert_eq!(payload["config"]["diagnostics"], serde_json::json!([]));
    assert_eq!(payload["cross_package_config_attempt"]["ok"], false);
    assert!(!payload.to_string().contains("WriteOnly"));
    assert!(!payload.to_string().contains("operator-secret"));
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
fn real_lua_plugin_spawns_session_template_through_worker_capability() {
    let registry = install_session_template_spawn_registry(
        "session-template-spawn",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_template_spawn"),
            ),
        ],
    );
    let mut hub = explicit_runtime("session-template-spawn");
    hub.load_lua_plugin_package(&registry, "session-template-spawner.plugin")
        .expect("load session-template plugin");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_template.spawn".to_string(),
            arguments: serde_json::json!({
                "template_id": "session-template-spawner.plugin/init",
                "session_id": "lua-template-session",
                "environment": { "BOTSTER_MODE": "worker" },
                "context": {
                    "prompt": "spawned from lua worker",
                    "ticket_id": "ticket-worker-proof"
                }
            }),
        })
        .expect("spawn session template through real Lua worker");

    assert_eq!(result["session_id"], "lua-template-session");
    assert_eq!(result["lifecycle"], "running");
    assert_eq!(
        result["template_id"],
        "session-template-spawner.plugin/init"
    );
    assert_eq!(result["context_id"], "ctx-lua-template-session");
    assert_eq!(
        result["context_keys"],
        serde_json::json!([
            "context_id",
            "hub_socket",
            "prompt",
            "repo_path",
            "session_dir",
            "session_id",
            "target_id",
            "ticket_id",
            "worktree_path"
        ])
    );
    assert!(
        hub.session(&botster_core::SessionId("lua-template-session".to_string()))
            .expect("list spawned session")
            .is_some(),
        "production core daemon should own the spawned PTY session"
    );
}

#[test]
fn session_template_spawn_helper_works_from_non_mcp_plugin_invocation_path() {
    let registry = install_session_template_spawn_registry(
        "session-template-spawn-action",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_template_spawn"),
            ),
        ],
    );
    let mut hub = explicit_runtime("session-template-spawn-action");
    hub.load_lua_plugin_package(&registry, "session-template-spawner.plugin")
        .expect("load session-template plugin");

    let action = hub
        .dispatch_plugin_surface_action(
            "session-template-spawner.plugin",
            "session-template-spawner.surface",
            "session_template.spawn_action",
            serde_json::json!({
                "request_id": "spawn-action-non-mcp",
                "template_id": "session-template-spawner.plugin/init",
                "session_id": "lua-template-action-session",
                "environment": { "BOTSTER_MODE": "action" },
                "context": {
                    "prompt": "spawned from lua action worker",
                    "ticket_id": "ticket-action-proof"
                }
            }),
        )
        .expect("spawn session template through UI action worker path");

    assert_eq!(action.state, UiActionResultState::Accepted);
    assert_eq!(
        action.payload.as_ref().unwrap()["session_id"],
        "lua-template-action-session"
    );
    assert!(
        hub.session(&botster_core::SessionId(
            "lua-template-action-session".to_string()
        ))
        .expect("list action-spawned session")
        .is_some(),
        "generic invoke_plugin pump should fulfill non-MCP helper requests"
    );
}

#[test]
fn lua_session_template_spawn_requires_exact_scoped_package_capability() {
    let registry = install_session_template_spawn_registry(
        "session-template-spawn-unscoped",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(CapabilitySurface::SessionActions, None),
        ],
    );
    let mut hub = explicit_runtime("session-template-spawn-unscoped");
    hub.load_lua_plugin_package(&registry, "session-template-spawner.plugin")
        .expect("load unscoped session-template plugin");

    let error = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_template.spawn".to_string(),
            arguments: serde_json::json!({
                "template_id": "session-template-spawner.plugin/init",
                "session_id": "lua-template-denied"
            }),
        })
        .expect_err("unscoped SessionActions grant must not allow template spawn");

    assert_eq!(error.code, "plugin_tool_failed");
    assert!(error.message.contains("session_template_spawn capability"));
    assert!(
        hub.session(&botster_core::SessionId("lua-template-denied".to_string()))
            .expect("list denied session")
            .is_none(),
        "denied plugin call must not spawn a session"
    );
}

#[test]
fn project_pipelines_surface_action_round_trip_uses_client_api_and_plugin_worker() {
    let registry = install_project_pipelines_registry();
    let mut hub = explicit_runtime("project-pipelines-ui");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load project pipelines plugin package");
    let api = HubClientApi::local_operator("project-pipelines-ui-test");

    let surface = api
        .handle_request(
            &mut hub,
            &registry,
            HubClientRequest::PluginSurfaceRender {
                request_id: RequestId("render-project-pipelines-surface".to_string()),
                package_name: "project-pipelines".to_string(),
                surface_id: "project-pipelines.create-ticket".to_string(),
                payload: serde_json::json!({}),
            },
        )
        .expect("render project pipelines surface through client api");
    let HubClientResponseBody::PluginSurface(surface) = surface.body else {
        panic!("plugin surface response expected");
    };
    assert_eq!(surface.package_name, "project-pipelines");
    assert_eq!(surface.surface_id, "project-pipelines.create-ticket");
    assert_eq!(surface.body.kind, UiNodeKind::Panel);
    assert_eq!(
        surface.body.id.as_ref().map(|id| id.0.as_str()),
        Some("project-pipelines-create-panel")
    );

    let invalid = api
        .handle_request(
            &mut hub,
            &registry,
            HubClientRequest::PluginSurfaceAction {
                request_id: RequestId("invalid-project-pipelines-action".to_string()),
                package_name: "project-pipelines".to_string(),
                surface_id: "project-pipelines.create-ticket".to_string(),
                action_id: "project_pipelines.create_ticket".to_string(),
                payload: serde_json::json!({
                    "request_id": "invalid-project-pipelines-action",
                    "title": "   ",
                    "pipeline_id": "local_pipeline",
                }),
            },
        )
        .expect("submit invalid project pipelines action through client api");
    let HubClientResponseBody::PluginActionResult(invalid) = invalid.body else {
        panic!("plugin action response expected");
    };
    assert_eq!(invalid.state, UiActionResultState::Rejected);
    assert_eq!(invalid.surface_id.0, "project-pipelines.create-ticket");
    assert_eq!(
        invalid
            .field_errors
            .get("project-pipelines-create-title")
            .and_then(|errors| errors.first())
            .map(String::as_str),
        Some("Title is required")
    );
    assert_eq!(invalid.form_errors, vec!["Title is required".to_string()]);

    let valid = api
        .handle_request(
            &mut hub,
            &registry,
            HubClientRequest::PluginSurfaceAction {
                request_id: RequestId("valid-project-pipelines-action".to_string()),
                package_name: "project-pipelines".to_string(),
                surface_id: "project-pipelines.create-ticket".to_string(),
                action_id: "project_pipelines.create_ticket".to_string(),
                payload: serde_json::json!({
                    "request_id": "valid-project-pipelines-action",
                    "title": "  Dogfood ticket  ",
                    "pipeline_id": "local.pipeline",
                }),
            },
        )
        .expect("submit valid project pipelines action through client api");
    let HubClientResponseBody::PluginActionResult(valid) = valid.body else {
        panic!("plugin action response expected");
    };
    assert_eq!(valid.state, UiActionResultState::Accepted);
    assert_eq!(valid.surface_id.0, "project-pipelines.create-ticket");
    assert_eq!(
        valid.normalized_values.as_ref().unwrap().0["title"],
        "Dogfood ticket"
    );

    let context = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "project_pipelines.current_context".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("read current context after plugin action");
    assert_eq!(context["tickets"].as_array().unwrap().len(), 1);
    assert_eq!(context["tickets"][0]["title"], "Dogfood ticket");
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
        registry
            .package("reload.plugin")
            .expect("reload package")
            .configuration_view(),
        LuaPluginHostApi {
            capabilities: hub.capability_runtime(),
            routed_envelopes: hub.routed_envelope_runtime(),
            session_templates: hub.session_template_spawner(),
            spawn_targets: hub.spawn_targets(),
            worktrees: hub.worktrees(),
        },
        registry.packages().into_iter().cloned().collect(),
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
