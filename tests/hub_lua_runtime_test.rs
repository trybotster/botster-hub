use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    BoundaryJson, Capability, CapabilitySurface, EndpointId, EnvelopeDeliveryStatus, EnvelopeId,
    EnvelopeTarget, PackageConfigurationSecretValue, PackageConfigurationValue, PluginHandlerKind,
    PluginHandlerRef, PluginInvocationContext, PluginInvocationFailure,
    PluginInvocationFailureKind, PluginInvocationRequest, PluginInvocationResult,
    PluginInvocationSuccess, PluginKey, RequestId, RoutedEnvelope, RoutedEnvelopePayload,
    SessionId,
};
use botster_hub::package_event_router::{
    CAUSAL_FLUSH_MAX, CAUSAL_PENDING_MAX, CausalAdmitResult, CausalOp, LeaseIdentity,
    release_or_retract,
};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubClientApi, HubClientRequest,
    HubClientResponseBody, HubRuntime, HubStartupOptions, LuaPluginHostApi, LuaPluginRuntime,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings, Worktree,
    default_package_policy,
};
use botster_ui_contract::{UiActionRequest, UiActionResultState, UiAuthoredNodeId, UiNodeKind};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    let data_directory = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join(format!("{}-{name}", std::process::id()));
    explicit_runtime_in(name, data_directory)
}

fn explicit_runtime_in(name: &str, data_directory: PathBuf) -> HubRuntime {
    explicit_runtime_in_with_cleanup(name, data_directory, true)
}

fn explicit_runtime_preserving(name: &str, data_directory: PathBuf) -> HubRuntime {
    explicit_runtime_in_with_cleanup(name, data_directory, false)
}

fn explicit_runtime_in_with_cleanup(
    name: &str,
    data_directory: PathBuf,
    clear_data_directory: bool,
) -> HubRuntime {
    ensure_session_worker_binary();
    if clear_data_directory {
        let _ = std::fs::remove_dir_all(&data_directory);
    }
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn unique_short_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    PathBuf::from("/tmp").join(format!("bh-{name}-{nanos}"))
}

fn write_plugin_store_generation(directory: &std::path::Path, status: &str, revision: u64) {
    fs::create_dir_all(directory).expect("create plugin-store generation");
    let key = "tickets/ticket-1";
    let encoded_key = key
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    fs::write(
        directory.join(format!("{encoded_key}.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "plugin_key": "project-pipelines",
            "key": key,
            "schema_version": 1,
            "revision": revision,
            "payload": { "id": "ticket-1", "status": status }
        }))
        .expect("encode plugin-store fixture record"),
    )
    .expect("write plugin-store fixture record");
}

fn ui_action_request(
    request_id: &str,
    surface_id: &str,
    action_id: &str,
    node_id: &str,
    values: serde_json::Value,
    payload: serde_json::Value,
) -> UiActionRequest {
    serde_json::from_value(serde_json::json!({
        "request_id": request_id,
        "surface_id": surface_id,
        "action_id": action_id,
        "node_id": node_id,
        "kind": "submit",
        "values": values,
        "payload": payload
    }))
    .expect("canonical UI action request")
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
        .enable("runtime.synthetic-plugin", "enable synthetic lua plugin")
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
            "runtime.synthetic-plugin",
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
        .enable("runtime.synthetic-plugin", "enable synthetic lua plugin")
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

fn install_cross_package_managed_session_registry(
    name: &str,
    template_package_root: &std::path::Path,
    allow_managed_spawn: bool,
) -> PackageRegistry {
    let caller_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let caller_source_root = std::env::current_dir()
        .expect("current dir")
        .join(&caller_root);
    let _ = fs::remove_dir_all(&caller_root);
    fs::create_dir_all(&caller_root).expect("create cross-package caller root");
    fs::write(
        caller_root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "cross_package.atomic",
      description = "Spawn an eligible template contributed by another package.",
      handler = "atomic",
      call = function(args)
        return botster.capabilities.session_types.ensure_worktree_and_spawn(args)
      end,
    },
    {
      name = "cross_package.inspect",
      description = "Inspect target-effective templates contributed by another package.",
      handler = "inspect",
      call = function(args)
        return {
          list = botster.capabilities.session_types.list({ target_id = args.target_id }),
          shown = botster.capabilities.session_types.show({
            target_id = args.target_id,
            session_type_id = args.session_type_id,
          }),
        }
      end,
    },
  },
})
"#,
    )
    .expect("write cross-package caller plugin");
    let caller_capabilities = if allow_managed_spawn {
        serde_json::json!([
            { "surface": "mcp" },
            {
                "surface": "session_actions",
                "scope": "session_type_managed_git_spawn"
            }
        ])
    } else {
        serde_json::json!([{ "surface": "mcp" }])
    };
    fs::write(
        caller_root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "managed-session-caller.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": caller_source_root },
            "capabilities": caller_capabilities,
            "entrypoints": [
                { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
            ]
        }))
        .expect("serialize cross-package caller manifest"),
    )
    .expect("write cross-package caller manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&caller_root, "install cross-package caller")
        .expect("install cross-package caller");
    policy
        .enable(
            "managed-session-caller.plugin",
            "enable cross-package caller",
        )
        .expect("enable cross-package caller");
    policy
        .install_local_path(
            template_package_root,
            "install cross-package template contributor",
        )
        .expect("install cross-package template contributor");
    let template_package_name = serde_json::from_slice::<serde_json::Value>(
        &fs::read(template_package_root.join("botster-package.json"))
            .expect("read template contributor manifest"),
    )
    .expect("parse template contributor manifest")["name"]
        .as_str()
        .expect("template contributor name")
        .to_string();
    policy
        .enable(
            &template_package_name,
            "enable cross-package template contributor",
        )
        .expect("enable cross-package template contributor");
    policy.registry().clone()
}

fn write_cross_package_template_contributor(root: &std::path::Path, target_id: &str) {
    fs::create_dir_all(root.join("bin")).expect("create template contributor bin");
    let command = root.join("bin/init.sh");
    fs::write(
        &command,
        "#!/bin/sh\nprintf 'cross-package\\n' > cross-package-executed.txt\n",
    )
    .expect("write cross-package template command");
    fs::set_permissions(&command, fs::Permissions::from_mode(0o755))
        .expect("make cross-package template command executable");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "managed-session-type.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root },
            "capabilities": [],
            "entrypoints": [],
            "session_types": [{
                "id": "init",
                "label": "Managed agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "traits": ["test"],
                "lifecycle": "task",
                "command": "bin/init.sh",
                "target_id": target_id
            }]
        }))
        .expect("serialize cross-package template contributor"),
    )
    .expect("write cross-package template contributor manifest");
}

fn install_coordination_probe_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create coordination probe package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "coordination_probe.publish",
      description = "Publish one routed envelope through Lua coordination.",
      handler = "publish",
      call = function(args)
        return botster.coordination.publish({
          id = args.envelope_id,
          target = args.target,
          body = "lua coordination payload",
          content_type = "text/plain",
          created_at = 42,
        })
      end,
    },
    {
      name = "coordination_probe.drain",
      description = "Drain routed envelopes through Lua coordination.",
      handler = "drain",
      call = function(args)
        return botster.coordination.drain({
          target = args.target,
          after = args.after,
          limit = args.limit or 16,
        })
      end,
    },
    {
      name = "coordination_probe.acknowledge",
      description = "Acknowledge one routed envelope through Lua coordination.",
      handler = "acknowledge",
      call = function(args)
        return botster.coordination.acknowledge({
          target = args.target,
          envelope_id = args.envelope_id,
        })
      end,
    },
  },
})
"#,
    )
    .expect("write coordination probe plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "coordination-probe.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write coordination probe manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install coordination probe package")
        .expect("install coordination probe package");
    policy
        .enable(
            "coordination-probe.plugin",
            "enable coordination probe package",
        )
        .expect("enable coordination probe package");
    policy.registry().clone()
}

fn install_load_time_coordination_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create load-time coordination package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
botster.coordination.publish({
  id = "load-time-envelope",
  target = { type = "session", session_id = "load-time-target" },
  body = "load-time coordination payload",
  content_type = "text/plain",
  created_at = 42,
})

return botster.register({})
"#,
    )
    .expect("write load-time coordination plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "load-time-coordination.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write load-time coordination manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install load-time coordination package")
        .expect("install load-time coordination package");
    policy
        .enable(
            "load-time-coordination.plugin",
            "enable load-time coordination package",
        )
        .expect("enable load-time coordination package");
    policy.registry().clone()
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn install_session_type_spawn_registry(
    name: &str,
    capabilities: Vec<Capability>,
) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("bin")).expect("create session-type package root");
    fs::create_dir_all(root.join("subdir")).expect("create relative template directory");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "session_type.spawn",
      description = "Spawn a declared session type through the production Lua capability.",
      handler = "spawn",
      call = function(args)
        return botster.capabilities.session_types.spawn(args)
      end,
    },
    {
      name = "session_type.atomic",
      description = "Atomically ensure a managed worktree and spawn its configured session.",
      handler = "atomic",
      call = function(args)
        return botster.capabilities.session_types.ensure_worktree_and_spawn(args)
      end,
    },
    {
      name = "session_type.inspect",
      description = "Inspect target-filtered effective templates.",
      handler = "inspect",
      call = function(args)
        return {
          list = botster.capabilities.session_types.list({ target_id = args.target_id }),
          shown = botster.capabilities.session_types.show({
            target_id = args.target_id,
            session_type_id = args.session_type_id,
          }),
        }
      end,
    },
  },
  handlers = {
    {
      id = "spawn_action",
      kind = "ui_action",
      descriptor_id = "session_type.spawn_action",
      descriptor = {
        action_id = "session_type.spawn_action",
        surface_id = "session-type-spawner.surface",
      },
      call = function(args)
        local spawned = botster.capabilities.session_types.spawn(args.payload)
        return {
          request_id = args.request_id or "spawn-action",
          surface_id = "session-type-spawner.surface",
          action_id = "session_type.spawn_action",
          node_id = "session-type-spawner-form",
          state = "accepted",
          payload = spawned,
        }
      end,
    },
  },
})
"#,
    )
    .expect("write session-type plugin");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'template:%s:%s\\n' \"$BOTSTER_SESSION_ID\" \"$BOTSTER_MODE\"\nprintf 'managed\\n' > template-executed.txt\n",
    )
    .expect("write session-type script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session-type script");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "session-type-spawner.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": capabilities,
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "session_types": [
                {
                    "id": "init",
                    "label": "Managed agent",
                    "role": "botster.agent",
                    "interaction": "interactive",
                    "traits": ["test"],
                    "lifecycle": "task",
                    "command": "bin/init.sh",
                    "environment": { "BOTSTER_MODE": "default" },
                    "allowed_environment_overrides": ["BOTSTER_MODE"],
                    "context": ["prompt", "ticket_id"],
                    "target_id": "tgt_managed"
                },
                {
                    "id": "relative",
                    "label": "Relative managed agent",
                    "role": "botster.agent",
                    "interaction": "interactive",
                    "traits": ["test"],
                    "lifecycle": "task",
                    "command": "bin/init.sh",
                    "working_directory": { "policy": "relative", "path": "subdir" },
                    "environment": { "BOTSTER_MODE": "default" },
                    "allowed_environment_overrides": ["BOTSTER_MODE"],
                    "context": ["prompt", "ticket_id"],
                    "target_id": "tgt_managed"
                }
            ]
        })
        .to_string(),
    )
    .expect("write session-type package manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install session-type spawn lua package")
        .expect("install session-type package");
    policy
        .enable("session-type-spawner.plugin", "enable session-type package")
        .expect("enable session-type package");
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

fn install_atomic_plugin_db_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create atomic plugin-db package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
local plugin_db = botster.capabilities.plugin_db

local function read(key)
  return plugin_db.get({ key = key }).record
end

return botster.register({
  tools = {
    {
      name = "plugin_db_atomic.lifecycle",
      description = "Commit one Project Pipelines-shaped lifecycle transition.",
      handler = "lifecycle",
      call = function()
        if read("tickets/ticket-1") == nil then
          plugin_db.set({
            key = "tickets/ticket-1",
            schema_version = 1,
            payload = { id = "ticket-1", status = "open" },
            expected_revision = 0,
          })
        end
        local result = plugin_db.batch({ mutations = {
          {
            operation = "patch",
            key = "tickets/ticket-1",
            patch = { status = "active", current_run_id = "run-1" },
            expected_revision = 1,
          },
          {
            operation = "set",
            key = "runs/run-1",
            schema_version = 1,
            payload = { id = "run-1", ticket_id = "ticket-1", status = "active", current_step_id = "step-1" },
            expected_revision = 0,
          },
          {
            operation = "set",
            key = "steps/step-1",
            schema_version = 1,
            payload = { id = "step-1", run_id = "run-1", status = "active" },
            expected_revision = 0,
          },
          {
            operation = "set",
            key = "events/event-1",
            schema_version = 1,
            payload = { id = "event-1", run_id = "run-1", kind = "run.started" },
            expected_revision = 0,
          },
        } })
        return {
          result = result,
          ticket = read("tickets/ticket-1"),
          run = read("runs/run-1"),
          step = read("steps/step-1"),
          event = read("events/event-1"),
        }
      end,
    },
    {
      name = "plugin_db_atomic.failures",
      description = "Return typed atomic conflict, patch, and quota failures.",
      handler = "failures",
      call = function(args)
        local conflict = plugin_db.batch({ mutations = {
          {
            operation = "patch",
            key = "tickets/ticket-1",
            patch = { status = "closed" },
            expected_revision = 1,
          },
          {
            operation = "delete",
            key = "runs/run-1",
            expected_revision = 1,
          },
        } })
        local patch = plugin_db.batch({ mutations = {
          {
            operation = "patch",
            key = "tickets/ticket-1",
            patch = "not-an-object",
            expected_revision = 2,
          },
        } })
        local quota = plugin_db.batch({ mutations = {
          {
            operation = "set",
            key = "oversized",
            payload = { value = args.oversized },
            expected_revision = 0,
          },
          {
            operation = "set",
            key = "small",
            payload = { value = 1 },
            expected_revision = 0,
          },
        } })
        local late_conflict = plugin_db.batch({ mutations = {
          {
            operation = "patch",
            key = "runs/run-1",
            patch = { status = "must-not-commit" },
            expected_revision = 1,
          },
          {
            operation = "patch",
            key = "tickets/ticket-1",
            patch = { status = "must-not-commit" },
            expected_revision = 1,
          },
        } })
        local invalid = plugin_db.batch({ mutations = {
          {
            operation = "get",
            key = "tickets/ticket-1",
          },
        } })
        local missing_key = plugin_db.batch({ mutations = {
          {
            operation = "set",
            key = "must-not-create-before-missing-key",
            payload = { value = 1 },
            expected_revision = 0,
          },
          {
            operation = "set",
            payload = { value = 2 },
            expected_revision = 0,
          },
        } })
        local duplicate = plugin_db.batch({ mutations = {
          {
            operation = "set",
            key = "duplicate",
            payload = { value = 1 },
            expected_revision = 0,
          },
          {
            operation = "set",
            key = "duplicate",
            payload = { value = 2 },
            expected_revision = 0,
          },
        } })
        local missing = plugin_db.batch({ mutations = {
          {
            operation = "delete",
            key = "missing",
            expected_revision = 0,
          },
        } })
        return {
          conflict = conflict,
          patch = patch,
          quota = quota,
          late_conflict = late_conflict,
          invalid = invalid,
          missing_key = missing_key,
          duplicate = duplicate,
          missing = missing,
          records = plugin_db.list({}),
        }
      end,
    },
    {
      name = "plugin_db_atomic.snapshot",
      description = "Read the committed lifecycle after runtime reconstruction.",
      handler = "snapshot",
      call = function()
        return {
          ticket = read("tickets/ticket-1"),
          run = read("runs/run-1"),
          step = read("steps/step-1"),
          event = read("events/event-1"),
          records = plugin_db.list({}),
        }
      end,
    },
    {
      name = "plugin_db_atomic.recovery_marker",
      description = "Commit a batch after read-path recovery.",
      handler = "recovery_marker",
      call = function()
        return plugin_db.batch({ mutations = {
          {
            operation = "set",
            key = "recovery-marker",
            payload = { recovered = true },
            expected_revision = 0,
          },
        } })
      end,
    },
  },
})
"#,
    )
    .expect("write atomic plugin-db plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [
                { "surface": "mcp" },
                { "surface": "plugin_db", "scope": "project-pipelines" }
            ],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write atomic plugin-db manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install atomic plugin-db lua package")
        .expect("install atomic plugin-db package");
    policy
        .enable("project-pipelines", "enable atomic plugin-db package")
        .expect("enable atomic plugin-db package");
    policy.registry().clone()
}

fn install_denied_plugin_db_batch_registry(name: &str) -> PackageRegistry {
    let root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join(name);
    let source_root = std::env::current_dir().expect("current dir").join(&root);
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create denied plugin-db package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
local plugin_db = botster.capabilities.plugin_db

return botster.register({
  tools = {
    {
      name = "plugin_db_denied.batch",
      description = "Prove plugin_db batch capability denial raises a Lua error.",
      handler = "batch",
      call = function()
        local ok, err = pcall(plugin_db.batch, { mutations = {
          {
            operation = "set",
            key = "forbidden",
            payload = { value = true },
            expected_revision = 0,
          },
        } })
        return { ok = ok, error = tostring(err) }
      end,
    },
  },
})
"#,
    )
    .expect("write denied plugin-db plugin");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "plugin-db-denied",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root.display().to_string() },
            "capabilities": [{ "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write denied plugin-db manifest");

    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install denied plugin-db lua package")
        .expect("install denied plugin-db package");
    policy
        .enable("plugin-db-denied", "enable denied plugin-db package")
        .expect("enable denied plugin-db package");
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
events.on("hub", "worktree_created", function(event)
  return {
    received = event.event,
    worktree_id = event.worktree_id,
    target_id = event.target_id,
  }
end)

events.on("hub", "worktree_deleted", function(event)
  return {
    received = event.event,
    worktree_id = event.worktree_id,
  }
end)

events.on("hub", "worktree_created", function(event)
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

    let created = serde_json::json!({
        "event": "worktree_created",
        "worktree_id": "wt_1",
        "target_id": "tgt_1",
    });
    assert_eq!(
        hub.package_event_router()
            .try_ingress(
                "hub",
                "worktree_created",
                &created,
                std::time::Instant::now()
            )
            .as_str(),
        "accepted"
    );
    let outcomes = hub.drive_package_events_for_test();
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

    let deleted_payload = serde_json::json!({
        "event": "worktree_deleted",
        "worktree_id": "wt_1",
    });
    assert_eq!(
        hub.package_event_router()
            .try_ingress(
                "hub",
                "worktree_deleted",
                &deleted_payload,
                std::time::Instant::now()
            )
            .as_str(),
        "accepted"
    );
    let deleted = hub.drive_package_events_for_test();
    assert_eq!(deleted.len(), 1);
    assert_eq!(
        completed_payloads(&deleted),
        vec![serde_json::json!({
            "received": "worktree_deleted",
            "worktree_id": "wt_1",
        })]
    );

    assert_eq!(
        hub.package_event_router()
            .try_ingress(
                "hub",
                "worktree_create_failed",
                &serde_json::json!({ "event": "worktree_create_failed" }),
                std::time::Instant::now()
            )
            .as_str(),
        "accepted"
    );
    let unmatched = hub.drive_package_events_for_test();
    assert!(
        unmatched.is_empty(),
        "event subscriptions should match exact event names"
    );
}

fn completed_payloads(outcomes: &[botster_core::PluginCompletion]) -> Vec<serde_json::Value> {
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
fn plugin_db_batch_atomically_commits_project_pipeline_lifecycle_and_returns_typed_failures() {
    let registry = install_atomic_plugin_db_registry("plugin-db-atomic-lifecycle");
    let data_directory = unique_short_test_dir("plugin-db-atomic");
    let mut hub = explicit_runtime_in("plugin-db-atomic", data_directory.clone());
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load atomic plugin-db package");

    let lifecycle = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_atomic.lifecycle".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("atomic lifecycle tool should complete");
    assert_eq!(lifecycle["result"]["ok"], true);
    assert_eq!(
        lifecycle["result"]["results"]
            .as_array()
            .expect("ordered mutation results")
            .len(),
        4
    );
    assert_eq!(lifecycle["ticket"]["payload"]["status"], "active");
    assert_eq!(lifecycle["ticket"]["revision"], 2);
    assert_eq!(lifecycle["run"]["payload"]["current_step_id"], "step-1");
    assert_eq!(lifecycle["run"]["revision"], 1);
    assert_eq!(lifecycle["step"]["payload"]["status"], "active");
    assert_eq!(lifecycle["event"]["payload"]["kind"], "run.started");

    let failures = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_atomic.failures".to_string(),
            arguments: serde_json::json!({ "oversized": "x".repeat(65 * 1024) }),
        })
        .expect("typed failure probe should complete");
    for (name, kind, index, key) in [
        ("conflict", "revision_conflict", 1, "tickets/ticket-1"),
        ("patch", "patch_failed", 1, "tickets/ticket-1"),
        ("quota", "quota_exceeded", 1, "oversized"),
        ("late_conflict", "revision_conflict", 2, "tickets/ticket-1"),
        ("invalid", "invalid_request", 1, "tickets/ticket-1"),
        ("duplicate", "invalid_request", 2, "duplicate"),
        ("missing", "store_not_found", 1, "missing"),
    ] {
        assert_eq!(failures[name]["ok"], false, "{name} must fail");
        assert_eq!(failures[name]["error_kind"], kind, "{name} kind");
        assert_eq!(failures[name]["mutation_index"], index, "{name} index");
        assert_eq!(failures[name]["key"], key, "{name} key");
    }
    assert_eq!(failures["missing_key"]["ok"], false);
    assert_eq!(failures["missing_key"]["error_kind"], "invalid_request");
    assert_eq!(failures["missing_key"]["mutation_index"], 2);
    assert!(failures["missing_key"]["key"].is_null());
    let entries = failures["records"]["entries"]
        .as_array()
        .expect("list entries after failures");
    assert_eq!(entries.len(), 4, "failed batches must create no records");
    assert!(entries.iter().all(|entry| {
        entry["revision"]
            .as_u64()
            .is_some_and(|revision| revision <= 2)
    }));
    let unchanged = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_atomic.snapshot".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("failed batches leave lifecycle payloads readable");
    assert_eq!(unchanged["ticket"]["payload"]["status"], "active");
    assert_eq!(unchanged["ticket"]["revision"], 2);
    assert_eq!(unchanged["run"]["revision"], 1);
    assert_eq!(unchanged["run"]["payload"]["status"], "active");
    assert_eq!(unchanged["step"]["revision"], 1);
    assert_eq!(unchanged["event"]["revision"], 1);
    drop(hub);

    let mut restarted = explicit_runtime_preserving("plugin-db-atomic", data_directory.clone());
    restarted
        .load_lua_plugin_package(&registry, "project-pipelines")
        .expect("reload atomic plugin-db package");
    let snapshot = restarted
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_atomic.snapshot".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("read committed lifecycle after restart");
    assert_eq!(snapshot["ticket"]["payload"]["status"], "active");
    assert_eq!(snapshot["run"]["payload"]["status"], "active");
    assert_eq!(snapshot["step"]["payload"]["status"], "active");
    assert_eq!(snapshot["event"]["payload"]["kind"], "run.started");
    assert_eq!(
        snapshot["records"]["entries"]
            .as_array()
            .expect("restarted list entries")
            .len(),
        4
    );
    let _ = fs::remove_dir_all(data_directory);
}

#[test]
fn plugin_db_batch_capability_denial_raises_a_lua_error() {
    let registry = install_denied_plugin_db_batch_registry("plugin-db-batch-denied");
    let data_directory = unique_short_test_dir("plugin-db-batch-denied");
    let mut hub = explicit_runtime_in("plugin-db-batch-denied", data_directory.clone());
    hub.load_lua_plugin_package(&registry, "plugin-db-denied")
        .expect("load package without plugin-db grant");

    let denied = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "plugin_db_denied.batch".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("pcall should capture capability denial");
    assert_eq!(denied["ok"], false);
    assert!(
        denied["error"]
            .as_str()
            .is_some_and(|error| error.contains("plugin-store namespace must exactly match")),
        "unexpected capability denial: {denied}"
    );
    let _ = fs::remove_dir_all(data_directory);
}

#[test]
fn plugin_db_reads_recover_every_batch_directory_shape_before_a_subsequent_public_commit() {
    let registry = install_atomic_plugin_db_registry("plugin-db-batch-recovery");
    let cases = [
        (
            "live-staging",
            Some(("old", 1)),
            None,
            Some(("staged", 2)),
            Some("old"),
        ),
        (
            "backup-staging",
            None,
            Some(("old", 1)),
            Some(("staged", 2)),
            Some("old"),
        ),
        (
            "live-backup",
            Some(("new", 2)),
            Some(("old", 1)),
            None,
            Some("new"),
        ),
        (
            "initially-empty-staging",
            None,
            None,
            Some(("staged", 1)),
            None,
        ),
    ];

    for (name, live, backup, staging, expected_status) in cases {
        let data_directory = unique_short_test_dir(name);
        let plugin_data = data_directory.join("plugin-data");
        let live_directory = plugin_data.join("project-pipelines");
        let staging_directory = plugin_data.join(".project-pipelines.batch-staging");
        let backup_directory = plugin_data.join(".project-pipelines.batch-backup");
        if let Some((status, revision)) = live {
            write_plugin_store_generation(&live_directory, status, revision);
        }
        if let Some((status, revision)) = backup {
            write_plugin_store_generation(&backup_directory, status, revision);
        }
        if let Some((status, revision)) = staging {
            write_plugin_store_generation(&staging_directory, status, revision);
        }

        let mut hub = explicit_runtime_preserving(name, data_directory.clone());
        hub.load_lua_plugin_package(&registry, "project-pipelines")
            .expect("load recovery probe package");
        let snapshot = hub
            .call_plugin_mcp_tool(botster_hub::McpCallRequest {
                name: "plugin_db_atomic.snapshot".to_string(),
                arguments: serde_json::json!({}),
            })
            .expect("public get/list should recover transaction artifacts");

        match expected_status {
            Some(status) => {
                assert_eq!(snapshot["ticket"]["payload"]["status"], status, "{name}");
                assert_eq!(
                    snapshot["records"]["entries"]
                        .as_array()
                        .expect("recovered entries")
                        .len(),
                    1,
                    "{name}"
                );
            }
            None => {
                assert!(snapshot["ticket"].is_null(), "{name}");
                assert_eq!(
                    snapshot["records"]["entries"]
                        .as_array()
                        .expect("empty recovered entries")
                        .len(),
                    0,
                    "{name}"
                );
            }
        }
        assert!(!staging_directory.exists(), "{name} staging cleanup");
        assert!(!backup_directory.exists(), "{name} backup cleanup");

        let marker = hub
            .call_plugin_mcp_tool(botster_hub::McpCallRequest {
                name: "plugin_db_atomic.recovery_marker".to_string(),
                arguments: serde_json::json!({}),
            })
            .expect("batch should commit after recovery");
        assert_eq!(marker["ok"], true, "{name}");
        assert!(!staging_directory.exists(), "{name} staging after commit");
        assert!(!backup_directory.exists(), "{name} backup after commit");
        drop(hub);
        let _ = fs::remove_dir_all(data_directory);
    }
}

#[test]
fn real_lua_plugin_loads_invokes_tool_and_uses_hub_capability_runtime() {
    let registry = install_fixture_registry();
    let mut hub = explicit_runtime("fixture");

    let plugin_key = hub
        .load_lua_plugin_package(&registry, "runtime.synthetic-plugin")
        .expect("load real lua plugin package");

    assert_eq!(
        plugin_key,
        PluginKey("runtime.synthetic-plugin".to_string())
    );
    let tools = hub.list_plugin_mcp_tools();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "runtime.synthetic.echo");

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
            base_ref: None,
            metadata: BTreeMap::new(),
        },
        SpawnTarget {
            target_id: "tgt_lua_disabled".to_string(),
            label: "Lua Disabled".to_string(),
            root: target_root,
            enabled: false,
            kind: "directory".to_string(),
            base_ref: None,
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
        base_ref: None,
        metadata: BTreeMap::new(),
    }];
    state.worktrees = vec![Worktree {
        worktree_id: "wt_lua_plain".to_string(),
        target_id: "tgt_lua_worktrees".to_string(),
        label: "Lua Plain".to_string(),
        path: worktree_path,
        status: "present".to_string(),
        management: "registered".to_string(),
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
        base_ref: None,
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
        management: "registered".to_string(),
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
        .load_lua_plugin_package(&registry, "runtime.synthetic-plugin")
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
    hub.load_lua_plugin_package(&registry, "runtime.synthetic-plugin")
        .expect("load real lua plugin package");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "runtime.synthetic.echo".to_string(),
            arguments: serde_json::json!({ "message": "from-mcp" }),
        })
        .expect("call plugin MCP tool");

    assert_eq!(result["message"], "from-mcp");
    assert_eq!(result["ambient"]["os"], true);
}

fn exercise_cross_package_managed_spawn(
    name: &str,
    registry: &PackageRegistry,
    target_id: &str,
    session_type_id: &str,
    inspect_template: bool,
    expected_marker: Option<&str>,
) -> (Option<serde_json::Value>, serde_json::Value) {
    let data_directory = unique_short_test_dir(name);
    let mut hub = explicit_runtime_in(name, data_directory.clone());
    let repo_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join(format!("{name}-repository"));
    let _ = fs::remove_dir_all(&repo_root);
    fs::create_dir_all(&repo_root).expect("create cross-package managed repository");
    run_git(None, &["init", "-b", "main", path_str(&repo_root)]);
    run_git(
        Some(&repo_root),
        &["config", "user.email", "botster@example.invalid"],
    );
    run_git(Some(&repo_root), &["config", "user.name", "Botster Test"]);
    fs::write(repo_root.join("README.md"), "cross-package\n")
        .expect("write cross-package repository fixture");
    run_git(Some(&repo_root), &["add", "-A"]);
    run_git(Some(&repo_root), &["commit", "-m", "cross-package fixture"]);
    let mut state = hub.state().clone();
    state.spawn_targets = vec![SpawnTarget {
        target_id: target_id.to_string(),
        label: "Cross-package managed target".to_string(),
        root: repo_root.clone(),
        enabled: true,
        kind: "git".to_string(),
        base_ref: Some("main".to_string()),
        metadata: BTreeMap::new(),
    }];
    hub.replace_state(state);
    hub.load_lua_plugin_package(registry, "managed-session-caller.plugin")
        .expect("load cross-package caller");

    let inspected = inspect_template.then(|| {
        hub.call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "cross_package.inspect".to_string(),
            arguments: serde_json::json!({
                "target_id": target_id,
                "session_type_id": session_type_id
            }),
        })
        .expect("inspect cross-package template through real worker")
    });
    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "cross_package.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": target_id,
                "branch": format!("feature/{name}"),
                "session_type_id": session_type_id
            }),
        })
        .expect("spawn cross-package template through real worker");
    if let Some(expected_marker) = expected_marker {
        let marker = PathBuf::from(
            result["result"]["worktree_path"]
                .as_str()
                .expect("spawned worktree path"),
        )
        .join("cross-package-executed.txt");
        for _ in 0..100 {
            if marker.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            fs::read_to_string(marker).expect("cross-package command marker"),
            expected_marker,
            "the selected template contributor's command must execute"
        );
    }

    drop(hub);
    let _ = fs::remove_dir_all(&data_directory);
    fs::remove_dir_all(&repo_root).expect("remove cross-package repository");
    (inspected, result)
}

fn assert_cross_package_session_type_is_listed(
    inspected: &serde_json::Value,
    session_type_id: &str,
    source_name: &str,
) {
    let listed = inspected["list"]
        .as_array()
        .expect("cross-package template list");
    assert!(
        listed.iter().any(|session_type| {
            session_type["session_type_id"] == session_type_id
                && session_type["source_name"] == source_name
        }),
        "{session_type_id} from {source_name} must be visible to the caller; list: {listed:?}"
    );
}

#[test]
fn real_lua_plugin_cross_package_managed_session_type_spawning() {
    let contributor_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join("cross-package-template-contributor");
    let _ = fs::remove_dir_all(&contributor_root);
    write_cross_package_template_contributor(&contributor_root, "tgt_cross_package");
    let registry = install_cross_package_managed_session_registry(
        "cross-package-caller",
        &contributor_root,
        true,
    );
    let (inspected, result) = exercise_cross_package_managed_spawn(
        "cross-package-explicit-target",
        &registry,
        "tgt_cross_package",
        "managed-session-type.plugin/init",
        true,
        Some("cross-package\n"),
    );
    let inspected = inspected.expect("cross-package inspection");
    assert_cross_package_session_type_is_listed(
        &inspected,
        "managed-session-type.plugin/init",
        "managed-session-type.plugin",
    );
    assert_eq!(
        inspected["shown"]["source_name"],
        "managed-session-type.plugin"
    );
    assert_eq!(result["ok"], true, "cross-package result: {result}");
    assert_eq!(
        result["result"]["session_id"].as_str().map(str::len),
        Some(36)
    );

    let project_pipelines_registry = install_cross_package_managed_session_registry(
        "project-pipelines-cross-package-caller",
        std::path::Path::new("examples/project-pipelines"),
        true,
    );
    let (inspected, result) = exercise_cross_package_managed_spawn(
        "cross-package-project-pipelines",
        &project_pipelines_registry,
        "package:project-pipelines",
        "project-pipelines/agent-step",
        true,
        None,
    );
    let inspected = inspected.expect("Project Pipelines inspection");
    assert_cross_package_session_type_is_listed(
        &inspected,
        "project-pipelines/agent-step",
        "project-pipelines",
    );
    assert_eq!(inspected["shown"]["source_name"], "project-pipelines");
    assert_eq!(result["ok"], true, "Project Pipelines result: {result}");
    assert_eq!(
        result["result"]["session_id"].as_str().map(str::len),
        Some(36)
    );

    let denied_registry = install_cross_package_managed_session_registry(
        "cross-package-denied-caller",
        &contributor_root,
        false,
    );
    let (_, denied) = exercise_cross_package_managed_spawn(
        "cross-package-capability-denied",
        &denied_registry,
        "tgt_cross_package",
        "managed-session-type.plugin/init",
        false,
        None,
    );
    assert_eq!(denied["ok"], false);
    assert_eq!(denied["error"]["kind"], "capability_denied");

    let (_, mismatched) = exercise_cross_package_managed_spawn(
        "cross-package-target-mismatch",
        &registry,
        "tgt_other",
        "managed-session-type.plugin/init",
        false,
        None,
    );
    assert_eq!(mismatched["ok"], false);
    assert_eq!(mismatched["error"]["kind"], "session_type_not_eligible");
}

#[test]
fn real_lua_plugin_spawns_session_type_through_worker_capability() {
    let registry = install_session_type_spawn_registry(
        "session-type-spawn",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_type_spawn"),
            ),
        ],
    );
    let mut hub = explicit_runtime("session-type-spawn");
    hub.load_lua_plugin_package(&registry, "session-type-spawner.plugin")
        .expect("load session-type plugin");

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.spawn".to_string(),
            arguments: serde_json::json!({
                "session_type_id": "session-type-spawner.plugin/init",
                "session_id": "lua-template-session",
                "environment": { "BOTSTER_MODE": "worker" },
                "context": {
                    "prompt": "spawned from lua worker",
                    "ticket_id": "ticket-worker-proof"
                }
            }),
        })
        .expect("spawn session type through real Lua worker");

    assert_eq!(result["session_id"], "lua-template-session");
    assert_eq!(result["lifecycle"], "running");
    assert_eq!(
        result["session_type_id"],
        "session-type-spawner.plugin/init"
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
fn real_lua_plugin_atomically_ensures_managed_worktree_and_spawns_session() {
    let registry = install_session_type_spawn_registry(
        "managed-session-type-spawn",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_type_managed_git_spawn"),
            ),
        ],
    );
    let data_directory = unique_short_test_dir("managed-lua");
    let mut hub = explicit_runtime_in("managed-session-type-spawn", data_directory.clone());
    let repo_root = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime")
        .join("managed-session-type-repository");
    let _ = fs::remove_dir_all(&repo_root);
    fs::create_dir_all(&repo_root).expect("create managed repository");
    run_git(None, &["init", "-b", "main", path_str(&repo_root)]);
    run_git(
        Some(&repo_root),
        &["config", "user.email", "botster@example.invalid"],
    );
    run_git(Some(&repo_root), &["config", "user.name", "Botster Test"]);
    fs::write(repo_root.join("README.md"), "managed\n").expect("write managed fixture");
    fs::create_dir_all(repo_root.join("subdir")).expect("create managed relative directory");
    fs::write(repo_root.join("subdir/.keep"), "managed\n").expect("write managed relative fixture");
    fs::create_dir_all(repo_root.join("bin")).expect("create repo command directory");
    let repo_script = repo_root.join("bin/init.sh");
    fs::write(
        &repo_script,
        "#!/bin/sh\nprintf 'main\\n' > repo-executed.txt\n",
    )
    .expect("write repo branch command");
    let mut repo_script_permissions = fs::metadata(&repo_script)
        .expect("repo script metadata")
        .permissions();
    repo_script_permissions.set_mode(0o755);
    fs::set_permissions(&repo_script, repo_script_permissions).expect("chmod repo script");
    fs::create_dir_all(repo_root.join(".botster")).expect("create repo template directory");
    fs::write(
        repo_root.join(".botster/session-types.json"),
        serde_json::json!({
            "session_types": [{
                "id": "init",
                "label": "Repo agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "traits": ["test"],
                "lifecycle": "task",
                "command": "bin/init.sh",
                "target_id": "tgt_managed",
                "context": ["prompt", "ticket_id"]
            }]
        })
        .to_string(),
    )
    .expect("write repo template override");
    run_git(Some(&repo_root), &["add", "-A"]);
    run_git(Some(&repo_root), &["commit", "-m", "managed fixture"]);
    run_git(Some(&repo_root), &["switch", "-c", "feature/atomic"]);
    fs::write(
        &repo_script,
        "#!/bin/sh\nprintf 'feature\\n' > repo-executed.txt\n",
    )
    .expect("write branch-specific repo command");
    run_git(Some(&repo_root), &["add", "bin/init.sh"]);
    run_git(
        Some(&repo_root),
        &["commit", "-m", "branch-specific command"],
    );
    run_git(Some(&repo_root), &["switch", "main"]);
    let mut state = hub.state().clone();
    state.spawn_targets = vec![SpawnTarget {
        target_id: "tgt_managed".to_string(),
        label: "Managed".to_string(),
        root: repo_root.clone(),
        enabled: true,
        kind: "git".to_string(),
        base_ref: Some("main".to_string()),
        metadata: BTreeMap::new(),
    }];
    hub.replace_state(state);
    hub.load_lua_plugin_package(&registry, "session-type-spawner.plugin")
        .expect("load managed session-type plugin");

    let inspected = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.inspect".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "session_type_id": "tgt_managed/init"
            }),
        })
        .expect("inspect target-filtered templates");
    assert_eq!(inspected["list"].as_array().map(Vec::len), Some(2));
    assert_eq!(inspected["shown"]["target_id"], "tgt_managed");
    assert_eq!(inspected["shown"]["source"], "repo");
    assert_eq!(
        inspected["shown"]["overridden_sources"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        inspected["shown"]["diagnostics"],
        serde_json::json!(["overrides 1 lower-precedence definition(s)"])
    );
    let inspected_bare = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.inspect".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "session_type_id": "init"
            }),
        })
        .expect("inspect target-filtered template by bare id");
    assert_eq!(inspected_bare["shown"], inspected["shown"]);

    let result = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/atomic",
                "session_type_id": "tgt_managed/init",
                "context": {
                    "prompt": "trusted managed spawn",
                    "ticket_id": "ticket-managed-proof",
                    "metadata": { "safe": "value" }
                }
            }),
        })
        .expect("call atomic managed session-type capability");
    assert_eq!(result["ok"], true, "atomic result: {result}");
    let spawned = &result["result"];
    let session_id = spawned["session_id"].as_str().expect("session UUID");
    assert_eq!(session_id.len(), 36);
    assert_eq!(spawned["target_id"], "tgt_managed");
    assert_eq!(spawned["branch"], "feature/atomic");
    assert_eq!(spawned["base_ref"], "main");
    assert_eq!(spawned["created_branch"], false);
    assert_eq!(spawned["created_worktree"], true);
    assert!(
        hub.session(&SessionId(session_id.to_string()))
            .expect("read managed spawned session")
            .is_some()
    );
    let context = hub
        .session_context(session_id)
        .expect("read trusted managed session context");
    assert_eq!(context.values["branch_name"], "feature/atomic");
    assert_eq!(context.values["target_id"], "tgt_managed");
    assert_eq!(context.values["metadata.base_ref"], "main");
    assert_eq!(context.values["metadata.safe"], "value");
    assert_eq!(
        context.values["worktree_path"],
        spawned["worktree_path"].as_str().expect("worktree path")
    );
    let repo_marker = PathBuf::from(
        spawned["worktree_path"]
            .as_str()
            .expect("spawned worktree path"),
    )
    .join("repo-executed.txt");
    for _ in 0..100 {
        if repo_marker.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        fs::read_to_string(repo_marker).expect("repo-source command marker"),
        "feature\n",
        "repo-source templates must execute code from the selected managed branch"
    );

    let reused = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/atomic",
                "session_type_id": "tgt_managed/init"
            }),
        })
        .expect("reuse managed worktree through atomic capability");
    assert_eq!(reused["ok"], true);
    assert_eq!(reused["result"]["reused_worktree"], true);
    assert_ne!(
        reused["result"]["session_id"], spawned["session_id"],
        "each successful atomic call creates a distinct session UUID"
    );

    let relative = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/relative",
                "session_type_id": "session-type-spawner.plugin/relative"
            }),
        })
        .expect("spawn relative managed template");
    assert_eq!(relative["ok"], true);
    let relative_session_id = relative["result"]["session_id"]
        .as_str()
        .expect("relative session UUID");
    hub.session_context(relative_session_id)
        .expect("relative managed context");
    let relative_marker = PathBuf::from(
        relative["result"]["worktree_path"]
            .as_str()
            .expect("relative worktree path"),
    )
    .join("subdir/template-executed.txt");
    for _ in 0..100 {
        if relative_marker.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        fs::read_to_string(relative_marker).expect("relative cwd marker"),
        "managed\n",
        "relative working-directory policy must resolve beneath the managed worktree"
    );

    run_git(
        Some(&repo_root),
        &["switch", "-c", "feature/symlink-escape"],
    );
    fs::remove_dir_all(repo_root.join("subdir")).expect("replace relative directory");
    std::os::unix::fs::symlink("/tmp", repo_root.join("subdir")).expect("create escaping symlink");
    run_git(Some(&repo_root), &["add", "-A"]);
    run_git(
        Some(&repo_root),
        &["commit", "-m", "symlink escape fixture"],
    );
    run_git(Some(&repo_root), &["switch", "main"]);
    let symlink_escape = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/symlink-escape",
                "session_type_id": "session-type-spawner.plugin/relative"
            }),
        })
        .expect("return tagged symlink escape failure");
    assert_eq!(symlink_escape["ok"], false);
    assert_eq!(symlink_escape["error"]["kind"], "cwd_not_admitted");
    assert!(
        git_succeeds(
            &repo_root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                "refs/heads/feature/symlink-escape"
            ]
        ),
        "pre-existing symlink fixture branch must survive rejected materialization"
    );

    let package_script = PathBuf::from("target")
        .join("botster-hub-test-data")
        .join("lua-runtime-packages")
        .join("managed-session-type-spawn")
        .join("bin/init.sh");
    fs::remove_file(package_script).expect("force configured spawn failure");

    let new_branch_failure = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/rollback-new",
                "session_type_id": "session-type-spawner.plugin/relative"
            }),
        })
        .expect("return tagged new-branch spawn failure");
    assert_eq!(new_branch_failure["ok"], false);
    assert_eq!(new_branch_failure["error"]["kind"], "spawn_failed");
    assert!(
        !git_succeeds(
            &repo_root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                "refs/heads/feature/rollback-new"
            ]
        ),
        "a branch created by the failed call must be rolled back"
    );

    run_git(Some(&repo_root), &["branch", "feature/rollback-existing"]);
    let existing_branch_failure = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/rollback-existing",
                "session_type_id": "session-type-spawner.plugin/relative"
            }),
        })
        .expect("return tagged existing-branch spawn failure");
    assert_eq!(existing_branch_failure["ok"], false);
    assert_eq!(existing_branch_failure["error"]["kind"], "spawn_failed");
    assert!(
        git_succeeds(
            &repo_root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                "refs/heads/feature/rollback-existing"
            ]
        ),
        "a pre-existing branch must survive failed session spawn rollback"
    );

    drop(hub);
    fs::remove_dir_all(&data_directory).expect("remove short managed Lua data directory");
    fs::remove_dir_all(&repo_root).expect("remove managed Lua repository");
}

#[test]
fn managed_session_type_capability_denies_old_scope_and_trusted_field_smuggling() {
    let registry = install_session_type_spawn_registry(
        "managed-session-type-denied",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_type_spawn"),
            ),
        ],
    );
    let mut hub = explicit_runtime("managed-session-type-denied");
    hub.load_lua_plugin_package(&registry, "session-type-spawner.plugin")
        .expect("load denied managed session-type plugin");
    let denied = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/denied",
                "session_type_id": "session-type-spawner.plugin/init"
            }),
        })
        .expect("typed capability denial");
    assert_eq!(denied["ok"], false);
    assert_eq!(denied["error"]["kind"], "capability_denied");

    let smuggling = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.atomic".to_string(),
            arguments: serde_json::json!({
                "target_id": "tgt_managed",
                "branch": "feature/denied",
                "session_type_id": "session-type-spawner.plugin/init",
                "cwd": "/tmp/untrusted",
                "context": { "worktree_path": "/tmp/untrusted" }
            }),
        })
        .expect_err("trusted fields must be rejected at Lua boundary");
    assert!(smuggling.message.contains("trusted fields"));
}

fn path_str(path: &std::path::Path) -> &str {
    path.to_str().expect("test path is UTF-8")
}

fn run_git(root: Option<&std::path::Path>, args: &[&str]) {
    let mut command = Command::new("git");
    if let Some(root) = root {
        command.arg("-C").arg(root);
    }
    assert!(
        command.args(args).status().expect("run git").success(),
        "git command failed: {args:?}"
    );
}

fn git_succeeds(root: &std::path::Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .status()
        .expect("run git status")
        .success()
}

#[test]
fn session_type_spawn_helper_works_from_non_mcp_plugin_invocation_path() {
    let registry = install_session_type_spawn_registry(
        "session-type-spawn-action",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(
                CapabilitySurface::SessionActions,
                Some("session_type_spawn"),
            ),
        ],
    );
    let mut hub = explicit_runtime("session-type-spawn-action");
    hub.load_lua_plugin_package(&registry, "session-type-spawner.plugin")
        .expect("load session-type plugin");

    let action = hub
        .dispatch_plugin_surface_action(
            "session-type-spawner.plugin",
            &ui_action_request(
                "spawn-action-non-mcp",
                "session-type-spawner.surface",
                "session_type.spawn_action",
                "session-type-spawner-form",
                serde_json::json!({}),
                serde_json::json!({
                "session_type_id": "session-type-spawner.plugin/init",
                "session_id": "lua-template-action-session",
                "environment": { "BOTSTER_MODE": "action" },
                "context": {
                    "prompt": "spawned from lua action worker",
                    "ticket_id": "ticket-action-proof"
                }
                }),
            ),
        )
        .expect("spawn session type through UI action worker path");

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
fn lua_session_type_spawn_requires_exact_scoped_package_capability() {
    let registry = install_session_type_spawn_registry(
        "session-type-spawn-unscoped",
        vec![
            capability(CapabilitySurface::Mcp, None),
            capability(CapabilitySurface::SessionActions, None),
        ],
    );
    let mut hub = explicit_runtime("session-type-spawn-unscoped");
    hub.load_lua_plugin_package(&registry, "session-type-spawner.plugin")
        .expect("load unscoped session-type plugin");

    let error = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "session_type.spawn".to_string(),
            arguments: serde_json::json!({
                "session_type_id": "session-type-spawner.plugin/init",
                "session_id": "lua-template-denied"
            }),
        })
        .expect_err("unscoped SessionActions grant must not allow template spawn");

    assert_eq!(error.code, "plugin_tool_failed");
    assert!(error.message.contains("session_type_spawn capability"));
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
        surface
            .body
            .id
            .as_ref()
            .and_then(UiAuthoredNodeId::as_literal)
            .map(|id| id.0.as_str()),
        Some("project-pipelines-create-panel")
    );

    let invalid = api
        .handle_request(
            &mut hub,
            &registry,
            HubClientRequest::PluginSurfaceAction {
                request_id: RequestId("invalid-project-pipelines-action".to_string()),
                package_name: "project-pipelines".to_string(),
                action: ui_action_request(
                    "invalid-project-pipelines-action",
                    "project-pipelines.create-ticket",
                    "project_pipelines.create_ticket",
                    "project-pipelines-create-form",
                    serde_json::json!({ "title": "   " }),
                    serde_json::json!({ "pipeline_id": "local_pipeline" }),
                ),
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
    assert!(invalid.presentation.is_none());
    assert!(invalid.replacement.is_none());

    let valid = api
        .handle_request(
            &mut hub,
            &registry,
            HubClientRequest::PluginSurfaceAction {
                request_id: RequestId("valid-project-pipelines-action".to_string()),
                package_name: "project-pipelines".to_string(),
                action: ui_action_request(
                    "valid-project-pipelines-action",
                    "project-pipelines.create-ticket",
                    "project_pipelines.create_ticket",
                    "project-pipelines-create-form",
                    serde_json::json!({ "title": "  Runtime ticket  " }),
                    serde_json::json!({ "pipeline_id": "local.pipeline" }),
                ),
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
        "Runtime ticket"
    );
    assert_eq!(
        valid
            .presentation
            .as_ref()
            .and_then(|operations| operations.first())
            .map(|operation| serde_json::to_value(operation).unwrap()["kind"].clone()),
        Some(serde_json::json!("clear"))
    );
    assert_eq!(
        valid.replacement.as_ref().map(|node| node.kind),
        Some(UiNodeKind::Panel)
    );

    let context = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "project_pipelines.current_context".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("read current context after plugin action");
    assert_eq!(context["tickets"].as_array().unwrap().len(), 1);
    assert_eq!(context["tickets"][0]["title"], "Runtime ticket");
}

#[test]
fn lua_and_native_coordination_publish_into_coredaemon_router() {
    let registry = install_coordination_probe_registry("coordination-probe");
    let mut hub = explicit_runtime("single-coredaemon-coordination");
    hub.load_lua_plugin_package(&registry, "coordination-probe.plugin")
        .expect("load coordination probe package");

    let native_target = EnvelopeTarget::Session {
        session_id: SessionId("native-coordination-target".to_string()),
    };
    let native_envelope_id = EnvelopeId("native-coredaemon-visible".to_string());
    let native_publish = hub
        .publish_routed_envelope(RoutedEnvelope::new(
            native_envelope_id.clone(),
            EndpointId("hub:native-test".to_string()),
            vec![native_target.clone()],
            RoutedEnvelopePayload {
                content_type: "text/plain".to_string(),
                body: b"native coordination payload".to_vec(),
                extension: None,
            },
            41,
        ))
        .expect("publish native routed envelope");
    assert_eq!(
        native_publish.deliveries[0].status,
        EnvelopeDeliveryStatus::Queued
    );
    assert_eq!(
        hub.routed_envelope_delivery_state(&native_target, &native_envelope_id)
            .state
            .expect("CoreDaemon should record native delivery")
            .status,
        EnvelopeDeliveryStatus::Queued
    );
    let lua_drain_native = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "coordination_probe.drain".to_string(),
            arguments: serde_json::json!({
                "target": native_target.clone(),
                "limit": 1,
            }),
        })
        .expect("drain native envelope through Lua coordination");
    assert_eq!(
        lua_drain_native["envelopes"][0]["id"],
        native_envelope_id.0.as_str()
    );
    let lua_ack_native = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "coordination_probe.acknowledge".to_string(),
            arguments: serde_json::json!({
                "target": native_target.clone(),
                "envelope_id": native_envelope_id.0,
            }),
        })
        .expect("ack native envelope through Lua coordination");
    assert_eq!(lua_ack_native["state"]["status"], "acknowledged");
    assert_eq!(
        hub.routed_envelope_delivery_state(&native_target, &native_envelope_id)
            .state
            .expect("CoreDaemon should record Lua ack")
            .status,
        EnvelopeDeliveryStatus::Acknowledged
    );

    let lua_target = EnvelopeTarget::Session {
        session_id: SessionId("lua-coordination-target".to_string()),
    };
    let lua_envelope_id = EnvelopeId("lua-coredaemon-visible".to_string());
    let lua_publish = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "coordination_probe.publish".to_string(),
            arguments: serde_json::json!({
                "envelope_id": lua_envelope_id.0,
                "target": lua_target.clone(),
            }),
        })
        .expect("publish routed envelope through Lua coordination");
    assert_eq!(lua_publish["deliveries"][0]["status"], "queued");

    let native_drain_lua = hub
        .drain_routed_envelopes(lua_target.clone(), None, 1)
        .expect("drain Lua envelope through native HubRuntime");
    assert_eq!(native_drain_lua.envelopes[0].id, lua_envelope_id);
    let native_ack_lua = hub
        .acknowledge_routed_envelope(lua_target.clone(), lua_envelope_id.clone())
        .expect("ack Lua envelope through native HubRuntime");
    assert_eq!(
        native_ack_lua
            .state
            .expect("CoreDaemon should return native ack state")
            .status,
        EnvelopeDeliveryStatus::Acknowledged
    );
    assert_eq!(
        hub.routed_envelope_delivery_state(&lua_target, &lua_envelope_id)
            .state
            .expect("CoreDaemon should record Lua delivery")
            .status,
        EnvelopeDeliveryStatus::Acknowledged
    );
}

#[test]
fn lua_coordination_at_plugin_load_fails_with_context_error() {
    let registry = install_load_time_coordination_registry("load-time-coordination");
    let mut hub = explicit_runtime("load-time-coordination");

    let error = hub
        .load_lua_plugin_package(&registry, "load-time-coordination.plugin")
        .expect_err("load-time coordination should fail before timeout");

    let message = error.to_string();
    assert!(
        message.contains(
            "botster.coordination is only available during handler invocation, not at plugin load"
        ),
        "unexpected load error: {message}"
    );
    assert!(
        !message.contains("did not complete before timeout"),
        "load-time coordination should fail with a context error, not timeout: {message}"
    );
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
            coordination: hub.coordination_bridge(),
            entity_publish: hub.entity_publish_bridge(),
            session_types: hub.session_type_spawner(),
            spawn_targets: hub.spawn_targets(),
            worktrees: hub.worktrees(),
            package_event_router: hub.package_event_router().clone(),
            causal_scopes: hub.causal_scopes().clone(),
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
        .load_lua_plugin_package(&registry, "runtime.synthetic-plugin")
        .expect("load real lua plugin package");
    let _ = hub.unload_plugin_package(
        RequestId("unload-lua".to_string()),
        "runtime.synthetic-plugin",
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

#[test]
fn package_owned_entity_provider_drives_surface_admission_and_fresh_snapshots() {
    let root = unique_short_test_dir("entity-provider");
    fs::create_dir_all(&root).expect("create entity provider package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("package path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write entity provider manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
local generation = 0
return botster.register({
  handlers = {
    {
      id = "home",
      kind = "surface_route",
      descriptor_id = "project-pipelines.home",
      call = function()
        return {
          type = "panel",
          id = "project-pipelines-home",
          children = {{
            ["$kind"] = "bind_list",
            source = "/project-pipelines.run",
            item_template = {
              type = "text",
              id = { ["$bind"] = "@/id" },
              props = { text = { ["$bind"] = "@/status" } },
            },
          }},
        }
      end,
    },
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function(request)
        generation = generation + 1
        local items = {{
          id = "run-1",
          status = "generation-" .. generation,
          requested_entity_type = request.entity_type,
          subscription_id = request.subscription_id,
        }}
        if generation == 3 then
          items = {
            { id = "run-1", status = "duplicate-a" },
            { id = "run-1", status = "duplicate-b" },
          }
        end
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = generation,
          items = items,
        }
      end,
    },
  },
})
"#,
    )
    .expect("write entity provider plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install entity provider package")
        .expect("install entity provider package");
    policy
        .enable("project-pipelines", "enable entity provider package")
        .expect("enable entity provider package");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("entity-provider");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load entity provider package");

    hub.render_plugin_surface(
        "project-pipelines",
        "project-pipelines.home",
        serde_json::json!({}),
    )
    .expect("render surface bound to its declared family");
    for (subscription_id, generation) in [("first", 1_u64), ("reconnect", 2_u64)] {
        let (snapshot_seq, items) = hub
            .plugin_entity_snapshot("project-pipelines.run", subscription_id)
            .expect("query provider through Hub runtime worker boundary");
        assert_eq!(snapshot_seq, generation);
        assert_eq!(items[0]["id"], "run-1");
        assert_eq!(items[0]["status"], format!("generation-{generation}"));
        assert_eq!(items[0]["requested_entity_type"], "project-pipelines.run");
        assert_eq!(items[0]["subscription_id"], subscription_id);
    }

    let duplicate_error = hub
        .plugin_entity_snapshot("project-pipelines.run", "duplicate-output")
        .expect_err("duplicate provider record ids must fail validation");
    assert_eq!(duplicate_error.code, "invalid_entity_provider");
    assert!(
        duplicate_error
            .message
            .contains("duplicate record id run-1")
    );

    let cleanup = hub.unload_plugin_package(
        RequestId("unload-entity-provider".to_string()),
        "project-pipelines",
    );
    assert!(cleanup.removed_resources.iter().any(|resource| {
        resource.kind == botster_core::PluginResourceKind::EntityProvider
            && resource.resource_id == "project-pipelines.run"
    }));
    assert!(
        hub.plugin_entity_snapshot("project-pipelines.run", "after-unload")
            .is_err()
    );
}

#[test]
fn entity_options_select_admits_dual_families_and_serves_fresh_snapshots() {
    let root = unique_short_test_dir("entity-options-select");
    fs::create_dir_all(&root).expect("create entity-options package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("package path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("write entity-options manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
local generation = 0
return botster.register({
  handlers = {
    {
      id = "picker",
      kind = "surface_route",
      descriptor_id = "project-pipelines.picker",
      call = function()
        return {
          type = "select",
          id = "session-picker",
          props = {
            name = "session",
            label = "Session",
            options_source = {
              ["$kind"] = "entity_options",
              source = "/session",
              value_field = "session_uuid",
              display_fields = { "label", "lifecycle_class" },
              order = { "label", "session_uuid" },
              where = { lifecycle_class = "current" },
              exclude = {
                source = "/project-pipelines.run",
                value_field = "session_uuid",
                where = { status = "active" },
              },
            },
          },
        }
      end,
    },
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function(request)
        generation = generation + 1
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = generation,
          items = {{
            id = "run-1",
            session_uuid = "sess-alpha",
            status = "active",
            subscription_id = request.subscription_id,
          }},
        }
      end,
    },
  },
})
"#,
    )
    .expect("write entity-options plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install entity-options package")
        .expect("install entity-options package");
    policy
        .enable("project-pipelines", "enable entity-options package")
        .expect("enable entity-options package");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("entity-options-select");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load entity-options package");

    let surface = hub
        .render_plugin_surface(
            "project-pipelines",
            "project-pipelines.picker",
            serde_json::json!({}),
        )
        .expect("render entity-options select surface");
    let families = botster_ui_contract::collect_entity_option_families(&surface);
    assert_eq!(
        families,
        vec!["project-pipelines.run".to_string(), "session".to_string()]
    );

    for (subscription_id, generation) in [("first", 1_u64), ("reconnect", 2_u64)] {
        let (snapshot_seq, items) = hub
            .plugin_entity_snapshot("project-pipelines.run", subscription_id)
            .expect("subscribe exclude family through Hub worker");
        assert_eq!(snapshot_seq, generation);
        assert_eq!(items[0]["session_uuid"], "sess-alpha");
        assert_eq!(items[0]["subscription_id"], subscription_id);
    }

    // Foreign exclude family is rejected as invalid_surface without loading.
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  handlers = {
    {
      id = "picker",
      kind = "surface_route",
      descriptor_id = "project-pipelines.picker",
      call = function()
        return {
          type = "select",
          id = "session-picker",
          props = {
            name = "session",
            label = "Session",
            options_source = {
              ["$kind"] = "entity_options",
              source = "/session",
              value_field = "session_uuid",
              display_fields = { "label" },
              order = { "label" },
              exclude = {
                source = "/project-pipelines.ticket",
                value_field = "session_uuid",
              },
            },
          },
        }
      end,
    },
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 1,
          items = {},
        }
      end,
    },
  },
})
"#,
    )
    .expect("write foreign exclude plugin");
    let mut foreign_policy = default_package_policy();
    foreign_policy
        .install_local_path(&root, "reinstall foreign exclude package")
        .expect("reinstall");
    foreign_policy
        .enable("project-pipelines", "enable foreign exclude package")
        .expect("enable");
    let foreign_registry = foreign_policy.registry().clone();
    let mut foreign_hub = explicit_runtime("entity-options-foreign");
    foreign_hub
        .load_lua_plugin_package(&foreign_registry, "project-pipelines")
        .expect("load foreign exclude package");
    let error = foreign_hub
        .render_plugin_surface(
            "project-pipelines",
            "project-pipelines.picker",
            serde_json::json!({}),
        )
        .expect_err("undeclared exclude family must fail admission");
    assert_eq!(error.code, "invalid_surface");
    assert!(
        error.message.contains("/project-pipelines.ticket"),
        "{error:?}"
    );
}

#[test]
fn dotted_package_entity_provider_rejects_noncanonical_owner_tokens() {
    for (label, entity_type, expected_error) in [
        (
            "raw-package-name",
            "botster.plugin-contract-matrix.run",
            "is not owned by plugin",
        ),
        (
            "other-package-token",
            "bns1_612e62.run",
            "is not owned by plugin",
        ),
        ("malformed-token", "bns1_0.run", "is not owned by plugin"),
        (
            "reserved-session",
            "session",
            "entity provider family session is reserved by Hub/Core",
        ),
        (
            "reserved-workspace",
            "workspace",
            "entity provider family workspace is reserved by Hub/Core",
        ),
    ] {
        let root = unique_short_test_dir(label);
        fs::create_dir_all(&root).expect("create invalid provider package");
        fs::write(
            root.join("botster-package.json"),
            serde_json::json!({
                "name": "botster.plugin-contract-matrix",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": root.canonicalize().expect("package path") },
                "capabilities": [{ "surface": "surfaces" }],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            })
            .to_string(),
        )
        .expect("write invalid provider manifest");
        fs::write(
            root.join("plugin.lua"),
            format!(
                r#"
return botster.register({{ handlers = {{
  {{ id = "runs", kind = "entity_provider", descriptor_id = "{entity_type}",
    descriptor = {{ entity_type = "{entity_type}", id_field = "id" }},
    call = function() return {{ type = "entity_snapshot", entity_type = "{entity_type}", snapshot_seq = 1, items = {{}} }} end }},
}} }})
"#
            ),
        )
        .expect("write invalid provider plugin");
        let mut policy = default_package_policy();
        policy
            .install_local_path(&root, "install invalid provider package")
            .expect("install invalid provider package");
        policy
            .enable(
                "botster.plugin-contract-matrix",
                "enable invalid provider package",
            )
            .expect("enable invalid provider package");
        let registry = policy.registry().clone();
        let mut hub = explicit_runtime(label);
        let error = hub
            .load_lua_plugin_package(&registry, "botster.plugin-contract-matrix")
            .expect_err("noncanonical provider namespace must be rejected");
        assert!(
            error.to_string().contains(expected_error),
            "{label}: {error}"
        );
    }
}

#[test]
fn entity_provider_empty_items_table_becomes_json_array() {
    let root = unique_short_test_dir("empty-items");
    fs::create_dir_all(&root).expect("create package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 1,
          items = {},
        }
      end,
    },
  },
})
"#,
    )
    .expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    policy
        .enable("project-pipelines", "enable")
        .expect("enable");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("empty-items");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load");
    let (snapshot_seq, items) = hub
        .plugin_entity_snapshot("project-pipelines.run", "empty-sub")
        .expect("empty items snapshot must decode as array");
    assert_eq!(snapshot_seq, 1);
    assert!(items.is_empty(), "items must be empty Vec, got {items:?}");
}

#[test]
fn entity_provider_empty_items_preserves_nested_empty_object_fields() {
    let root = unique_short_test_dir("nested-empty");
    fs::create_dir_all(&root).expect("create package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 1,
          items = {
            { id = "run-1", meta = {}, labels = {} },
          },
        }
      end,
    },
  },
})
"#,
    )
    .expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    policy
        .enable("project-pipelines", "enable")
        .expect("enable");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("nested-empty");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load");
    let (_, items) = hub
        .plugin_entity_snapshot("project-pipelines.run", "nested-sub")
        .expect("snapshot");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["meta"], serde_json::json!({}));
    // Nested empty tables become empty objects under default mlua conversion.
    assert!(
        items[0]["labels"].is_object(),
        "nested empty table must remain object, got {}",
        items[0]["labels"]
    );
}

#[test]
fn entity_publish_patch_nested_empty_object_remains_object() {
    let root = unique_short_test_dir("publish-nested");
    fs::create_dir_all(&root).expect("create package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 0,
          items = {},
        }
      end,
    },
    {
      id = "patch",
      kind = "ui_action",
      descriptor_id = "project-pipelines.patch",
      descriptor = {
        action_id = "project-pipelines.patch",
        surface_id = "project-pipelines.home",
      },
      call = function(args)
        local published = botster.entity_publish({
          type = "entity_patch",
          entity_type = "project-pipelines.run",
          snapshot_seq = 1,
          id = "run-1",
          patch = { meta = {}, status = "patched" },
        })
        return {
          request_id = args.request_id,
          surface_id = "project-pipelines.home",
          action_id = "project-pipelines.patch",
          node_id = args.node_id,
          state = "accepted",
          payload = published,
        }
      end,
    },
  },
})
"#,
    )
    .expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    policy
        .enable("project-pipelines", "enable")
        .expect("enable");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("publish-nested");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load");
    let result = hub
        .dispatch_plugin_surface_action(
            "project-pipelines",
            &ui_action_request(
                "patch-nested",
                "project-pipelines.home",
                "project-pipelines.patch",
                "form",
                serde_json::json!({}),
                serde_json::json!({}),
            ),
        )
        .expect("publish patch");
    assert_eq!(result.state, UiActionResultState::Accepted);
    let payload = result.payload.expect("publish payload");
    assert_eq!(payload["ok"], true);
    assert_eq!(payload["status"], "accepted");
    let ready = hub.take_package_entity_fanout();
    assert_eq!(ready.len(), 1);
    match &ready[0] {
        botster_hub::package_entity_fanout::PackageEntityMutation::Patch { patch, .. } => {
            assert_eq!(patch["meta"], serde_json::json!({}));
            assert!(
                patch["meta"].is_object(),
                "nested empty patch field must remain object"
            );
        }
        other => panic!("expected patch mutation, got {other:?}"),
    }
}

#[test]
fn non_provider_handler_payload_is_not_empty_items_coerced() {
    let root = unique_short_test_dir("non-provider-items");
    fs::create_dir_all(&root).expect("create package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("path") },
            "capabilities": [{ "surface": "surfaces" }, { "surface": "mcp" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {
    {
      name = "project-pipelines.echo_shape",
      description = "Return a colliding entity-shaped object that is not a provider frame.",
      handler = "echo_shape",
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "not-a-provider",
          snapshot_seq = 1,
          items = {},
        }
      end,
    },
  },
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 1,
          items = {},
        }
      end,
    },
  },
})
"#,
    )
    .expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    policy
        .enable("project-pipelines", "enable")
        .expect("enable");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("non-provider-items");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load");

    // Provider path still coerces empty items to an array.
    let (_, provider_items) = hub
        .plugin_entity_snapshot("project-pipelines.run", "provider-sub")
        .expect("provider empty items");
    assert!(provider_items.is_empty());

    // MCP tool with colliding keys must keep object-shaped items.
    let tool = hub
        .call_plugin_mcp_tool(botster_hub::McpCallRequest {
            name: "project-pipelines.echo_shape".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("mcp tool");
    assert_eq!(tool["type"], "entity_snapshot");
    assert!(
        tool["items"].is_object(),
        "non-provider handler items must remain object, got {}",
        tool["items"]
    );
}

#[test]
fn entity_publish_rejects_oversized_mutation_at_admission() {
    let root = unique_short_test_dir("publish-oversized");
    fs::create_dir_all(&root).expect("create package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        })
        .to_string(),
    )
    .expect("manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "project-pipelines.run",
      descriptor = { entity_type = "project-pipelines.run", id_field = "id" },
      call = function()
        return {
          type = "entity_snapshot",
          entity_type = "project-pipelines.run",
          snapshot_seq = 0,
          items = {},
        }
      end,
    },
    {
      id = "big",
      kind = "ui_action",
      descriptor_id = "project-pipelines.big",
      descriptor = {
        action_id = "project-pipelines.big",
        surface_id = "project-pipelines.home",
      },
      call = function(args)
        local ok, err = pcall(function()
          botster.entity_publish({
            type = "entity_upsert",
            entity_type = "project-pipelines.run",
            snapshot_seq = 1,
            id = "run-1",
            entity = { id = "run-1", blob = string.rep("x", 1024 * 1024) },
          })
        end)
        return {
          request_id = args.request_id,
          surface_id = "project-pipelines.home",
          action_id = "project-pipelines.big",
          node_id = args.node_id,
          state = "accepted",
          payload = { ok = ok, err = tostring(err) },
        }
      end,
    },
  },
})
"#,
    )
    .expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    policy
        .enable("project-pipelines", "enable")
        .expect("enable");
    let registry = policy.registry().clone();
    let mut hub = explicit_runtime("publish-oversized");
    hub.load_lua_plugin_package(&registry, "project-pipelines")
        .expect("load");
    let result = hub
        .dispatch_plugin_surface_action(
            "project-pipelines",
            &ui_action_request(
                "big-upsert",
                "project-pipelines.home",
                "project-pipelines.big",
                "form",
                serde_json::json!({}),
                serde_json::json!({}),
            ),
        )
        .expect("surface action completes");
    assert_eq!(result.state, UiActionResultState::Accepted);
    let payload = result.payload.expect("payload");
    assert_eq!(payload["ok"], false);
    let err = payload["err"].as_str().unwrap_or_default();
    assert!(
        err.contains("entity_provider_frame_too_large") || err.contains("frame limit"),
        "expected frame limit error, got {err}"
    );
    assert!(
        hub.take_package_entity_fanout().is_empty(),
        "oversized mutation must not enter fanout"
    );
}

fn install_named_lua_package(
    name: &str,
    plugin_lua: &str,
    manifest: serde_json::Value,
) -> PackageRegistry {
    let root = unique_short_test_dir(name);
    fs::create_dir_all(&root).expect("create package");
    let mut manifest = manifest;
    manifest["source"] = serde_json::json!({
        "type": "path",
        "path": root.canonicalize().expect("path")
    });
    fs::write(root.join("botster-package.json"), manifest.to_string()).expect("manifest");
    fs::write(root.join("plugin.lua"), plugin_lua).expect("plugin");
    let mut policy = default_package_policy();
    policy
        .install_local_path(&root, "install")
        .expect("install");
    let package_name = manifest["name"].as_str().expect("name");
    policy.enable(package_name, "enable").expect("enable");
    policy.registry().clone()
}

fn scoped_command(
    plugin: &str,
    handler_id: &str,
    payload: serde_json::Value,
    scope_id: u64,
) -> PluginInvocationRequest {
    PluginInvocationRequest {
        request_id: RequestId(format!("lease-{handler_id}")),
        handler: PluginHandlerRef {
            plugin_key: PluginKey(plugin.to_string()),
            kind: PluginHandlerKind::Command,
            handler_id: handler_id.to_string(),
        },
        timeout_ms: 1_000,
        context: PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: Some("hub-lua-runtime-test".to_string()),
            metadata: Some(BoundaryJson(
                serde_json::json!({ "causal_scope_id": scope_id }),
            )),
        },
        payload: BoundaryJson(payload),
    }
}

fn lease_probe_plugin() -> &'static str {
    r#"
local provider_status = "none"
return botster.register({
  handlers = {
    {
      id = "runs",
      kind = "entity_provider",
      descriptor_id = "lease-probe.item",
      descriptor = { entity_type = "lease-probe.item", id_field = "id" },
      call = function()
        local emitted = events.emit("unused", { ok = true })
        provider_status = emitted.status
        return {
          type = "entity_snapshot",
          entity_type = "lease-probe.item",
          snapshot_seq = 0,
          items = {},
        }
      end,
    },
    {
      id = "last_provider",
      kind = "command",
      call = function()
        return { status = provider_status }
      end,
    },
    {
      id = "publish",
      kind = "command",
      call = function(args)
        return botster.entity_publish({
          type = "entity_upsert",
          entity_type = "lease-probe.item",
          snapshot_seq = args.seq,
          id = args.id or "item-1",
          entity = { id = args.id or "item-1" },
        })
      end,
    },
    {
      id = "bad",
      kind = "command",
      call = function()
        return botster.entity_publish({
          type = "entity_upsert",
          entity_type = "not-owned.item",
          snapshot_seq = 1,
          id = "x",
          entity = { id = "x" },
        })
      end,
    },
  },
})
"#
}

fn lease_probe_manifest() -> serde_json::Value {
    serde_json::json!({
        "name": "lease-probe",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "capabilities": [],
        "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
    })
}

#[test]
fn failed_lua_load_does_not_leave_router_subscriptions() {
    let registry = install_named_lua_package(
        "failed-load-subs",
        r#"
events.on("hub", "worktree_created", function(event)
  return { received = event.event }
end)
error("deliberate load failure after events.on")
"#,
        serde_json::json!({
            "name": "failed-load.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
        }),
    );
    let mut hub = explicit_runtime("failed-load-subs");
    assert!(
        hub.load_lua_plugin_package(&registry, "failed-load.plugin")
            .is_err()
    );
    assert_eq!(
        hub.package_event_router()
            .test_subscription_count("failed-load.plugin"),
        0
    );
    assert_eq!(
        hub.package_event_router()
            .try_ingress(
                "hub",
                "worktree_created",
                &serde_json::json!({
                    "event": "worktree_created",
                    "worktree_id": "wt",
                    "target_id": "tgt"
                }),
                std::time::Instant::now()
            )
            .as_str(),
        "accepted"
    );
    let batch = hub
        .package_event_router()
        .pull_ready_batch(
            8,
            64 * 1024,
            std::time::Instant::now(),
            std::time::Duration::from_millis(8),
        )
        .expect("batch");
    assert!(batch.is_empty(), "failed load must not leave a subscriber");
}

#[test]
fn held_router_load_fails_without_partial_contracts_or_subscriptions() {
    let registry = install_named_lua_package(
        "held-router-load",
        r#"
events.on("hub", "worktree_created", function(event)
  return { received = event.event }
end)
return botster.register({})
"#,
        serde_json::json!({
            "name": "held-router.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "events": {
                "emitted": [{
                    "name": "sample.ready",
                    "audience": ["plugins"],
                    "payload_schema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": { "ok": { "type": "boolean" } },
                        "required": ["ok"]
                    }
                }]
            }
        }),
    );
    let mut hub = explicit_runtime("held-router-load");
    let router = hub.package_event_router().clone();
    let result = router
        .test_with_inner_held(|| hub.load_lua_plugin_package(&registry, "held-router.plugin"));
    assert!(
        result.is_err(),
        "held router must fail the load, got {result:?}"
    );
    assert!(!router.test_has_contract("held-router.plugin", "sample.ready"));
    assert_eq!(router.test_subscription_count("held-router.plugin"), 0);
    assert_eq!(
        router
            .current_package_generation("held-router.plugin")
            .expect("generation"),
        0
    );
}

#[test]
fn held_router_reload_keeps_one_generation_until_owner_apply() {
    let registry = install_named_lua_package(
        "held-router-reload",
        r#"
events.on("hub", "worktree_created", function(event)
  return { received = event.event }
end)
return botster.register({})
"#,
        serde_json::json!({
            "name": "held-reload.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "events": {
                "emitted": [{
                    "name": "sample.ready",
                    "audience": ["plugins"],
                    "payload_schema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": { "ok": { "type": "boolean" } },
                        "required": ["ok"]
                    }
                }]
            }
        }),
    );
    let mut hub = explicit_runtime("held-router-reload");
    hub.load_lua_plugin_package(&registry, "held-reload.plugin")
        .expect("load");
    let router = hub.package_event_router().clone();
    let first = router
        .current_package_generation("held-reload.plugin")
        .expect("first");
    assert!(router.test_has_contract("held-reload.plugin", "sample.ready"));
    let result = router.test_with_inner_held(|| {
        hub.reload_lua_plugin_package(
            RequestId("held-reload".into()),
            &registry,
            "held-reload.plugin",
        )
    });
    assert!(
        result.is_err(),
        "held router must fail reload before replacing the worker"
    );
    assert_eq!(
        router
            .current_package_generation("held-reload.plugin")
            .expect("unchanged"),
        first
    );
    assert!(router.test_has_contract("held-reload.plugin", "sample.ready"));
    assert_eq!(router.test_subscription_count("held-reload.plugin"), 1);
    let generation = router
        .current_package_generation("held-reload.plugin")
        .expect("generation");
    hub.record_event_plane_owner_op(botster_hub::package_event_router::OwnerOp {
        kind: botster_hub::package_event_router::OwnerOpKind::Unload,
        owner: "held-reload.plugin".into(),
        generation,
    });
    let _ = hub.unload_plugin_package(
        RequestId("held-reload-disable".into()),
        "held-reload.plugin",
    );
    let _ = hub.apply_event_plane_owner_ops();
    assert!(
        !router.test_has_contract("held-reload.plugin", "sample.ready"),
        "disable after a failed reload must not resurrect contracts"
    );
}

#[test]
fn entity_lease_scope_closes_after_success_error_fanout_degradation_and_unload() {
    let registry =
        install_named_lua_package("lease-close", lease_probe_plugin(), lease_probe_manifest());
    let mut hub = explicit_runtime("lease-close");
    hub.load_lua_plugin_package(&registry, "lease-probe")
        .expect("load");
    let scopes = hub.causal_scopes().clone();

    let success = scopes.mint_with_lease(None).expect("success scope");
    let published = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 1 }),
        success,
    ));
    assert!(matches!(
        published.result,
        PluginInvocationResult::Completed(_)
    ));
    assert!(scopes.is_live(success), "admitted mutation holds the scope");
    assert_eq!(
        scopes.identities(success),
        Some(std::collections::BTreeSet::from([
            LeaseIdentity::AdmittedEntityMutation {
                family: "lease-probe.item".into(),
                seq: 1,
            }
        ]))
    );
    let taken = hub.take_leased_package_entity_fanout();
    assert_eq!(taken.len(), 1);
    assert!(
        scopes.is_live(success),
        "drain must keep the mutation lease until fanout finishes"
    );
    hub.finish_package_entity_mutation_fanout(&taken[0], true);
    assert!(
        scopes.is_live(success),
        "fanout-created resync must keep the mutation scope"
    );
    hub.plugin_entity_snapshot("lease-probe.item", "fanout-resync")
        .expect("provider after fanout resync");
    let provider = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "last_provider",
        serde_json::json!({}),
        success,
    ));
    let PluginInvocationResult::Completed(PluginInvocationSuccess {
        payload: Some(payload),
        ..
    }) = provider.result
    else {
        panic!("provider status command should complete");
    };
    assert_eq!(payload.0["status"], "rejected_causal_scope");

    let errored = scopes.mint_with_lease(None).expect("error scope");
    let failed = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "bad",
        serde_json::json!({}),
        errored,
    ));
    assert!(matches!(failed.result, PluginInvocationResult::Failed(_)));
    assert!(
        !scopes.is_live(errored),
        "ownership error must release pending"
    );

    let degraded = scopes.mint_with_lease(None).expect("degraded scope");
    let far = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 32, "id": "far" }),
        degraded,
    ));
    assert!(matches!(far.result, PluginInvocationResult::Completed(_)));
    assert_eq!(
        scopes.identities(degraded),
        Some(std::collections::BTreeSet::from([
            LeaseIdentity::ProviderResyncNeed {
                family: "lease-probe.item".into(),
            }
        ]))
    );
    let mut entered_degraded = false;
    for _ in 0..8 {
        entered_degraded = hub.record_package_entity_resync_attempt("lease-probe.item");
    }
    assert!(entered_degraded);
    assert!(
        !scopes.is_live(degraded),
        "max attempts must release ProviderResyncNeed"
    );

    let unloaded = scopes.mint_with_lease(None).expect("unload scope");
    let gap = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 3, "id": "gap" }),
        unloaded,
    ));
    assert!(matches!(gap.result, PluginInvocationResult::Completed(_)));
    assert!(scopes.is_live(unloaded));
    let _ = hub.unload_plugin_package(RequestId("lease-unload".into()), "lease-probe");
    assert!(
        !scopes.is_live(unloaded),
        "unload must release remaining family leases"
    );
}

#[test]
fn production_fanout_finish_returns_the_513th_op_without_spinning() {
    let registry =
        install_named_lua_package("lease-nospin", lease_probe_plugin(), lease_probe_manifest());
    let mut hub = explicit_runtime("lease-nospin");
    hub.load_lua_plugin_package(&registry, "lease-probe")
        .expect("load");
    let scopes = hub.causal_scopes().clone();
    let success = scopes.mint_with_lease(None).expect("success scope");
    let published = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 1 }),
        success,
    ));
    assert!(matches!(
        published.result,
        PluginInvocationResult::Completed(_)
    ));
    let taken = hub.take_leased_package_entity_fanout();
    assert_eq!(taken.len(), 1);

    let capacity = CAUSAL_PENDING_MAX;
    let mut fillers = Vec::new();
    for index in 0..capacity {
        let scope = scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: format!("fill{index}"),
            }))
            .expect("mint filler");
        fillers.push(scope);
    }
    scopes.test_with_inner_held(|| {
        for (index, scope) in fillers.iter().enumerate() {
            assert_eq!(
                scopes.transfer(
                    *scope,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("fill{index}"),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        family: "f".into(),
                        seq: index as u64,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        }
        let started = Instant::now();
        hub.finish_package_entity_mutation_fanout(&taken[0], true);
        assert!(
            started.elapsed() < Duration::from_millis(20),
            "production finish must return without spinning: {:?}",
            started.elapsed()
        );
        assert!(
            hub.event_plane_owner_ops_pending(),
            "unsent finish must stay on the owner retry machine"
        );
    });
    assert_eq!(
        scopes.identities(success),
        Some(std::collections::BTreeSet::from([
            LeaseIdentity::AdmittedEntityMutation {
                family: "lease-probe.item".into(),
                seq: 1,
            }
        ])),
        "finish must not commit the transfer before retry ownership is durable"
    );
    let first = scopes.flush_pending();
    assert!(first > 0);
    assert!(
        first <= CAUSAL_FLUSH_MAX,
        "one owner turn must not drain without a bound: {first}"
    );
    assert_eq!(
        scopes.identities(fillers[0]),
        Some(std::collections::BTreeSet::from([
            LeaseIdentity::AdmittedEntityMutation {
                family: "f".into(),
                seq: 0,
            }
        ]))
    );
    while scopes.pending_ops() {
        let _ = scopes.flush_pending();
    }
    let _ = hub.apply_event_plane_owner_ops();
    while scopes.pending_ops() {
        let _ = scopes.flush_pending();
    }
    assert!(
        !hub.event_plane_owner_ops_pending(),
        "owner turn must admit the parked production transfer"
    );
    assert_eq!(
        scopes.identities(success),
        Some(std::collections::BTreeSet::from([
            LeaseIdentity::ProviderResyncNeed {
                family: "lease-probe.item".into(),
            }
        ]))
    );
}

#[test]
fn never_queued_publish_releases_after_full_causal_path() {
    let registry = install_named_lua_package(
        "lease-neverqueued",
        lease_probe_plugin(),
        lease_probe_manifest(),
    );
    let mut hub = explicit_runtime("lease-neverqueued");
    hub.load_lua_plugin_package(&registry, "lease-probe")
        .expect("load");
    let scopes = hub.causal_scopes().clone();
    let live = scopes.mint_with_lease(None).expect("live scope");
    let capacity = CAUSAL_PENDING_MAX;
    let mut fillers = Vec::new();
    for index in 0..capacity {
        let scope = scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: format!("fill{index}"),
            }))
            .expect("mint filler");
        fillers.push(scope);
    }
    scopes.test_with_inner_held(|| {
        for (index, scope) in fillers.iter().enumerate() {
            assert_eq!(
                scopes.transfer(
                    *scope,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("fill{index}"),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        family: "f".into(),
                        seq: index as u64,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        }
    });
    hub.entity_publish_bridge().reject_next_publish();
    let failed = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 1 }),
        live,
    ));
    assert!(matches!(failed.result, PluginInvocationResult::Failed(_)));
    let _ = hub.apply_event_plane_owner_ops();
    while scopes.pending_ops() {
        let _ = scopes.flush_pending();
    }
    assert!(
        !scopes.is_live(live),
        "NeverQueued must retract or later close the pending publish lease"
    );
}

#[test]
fn never_queued_release_stays_owned_when_release_queue_is_full() {
    let registry = install_named_lua_package(
        "lease-bridge-full",
        lease_probe_plugin(),
        lease_probe_manifest(),
    );
    let mut hub = explicit_runtime("lease-bridge-full");
    hub.load_lua_plugin_package(&registry, "lease-probe")
        .expect("load");
    let scopes = hub.causal_scopes().clone();
    let live = scopes
        .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
            plugin_key: "lease-probe".into(),
        }))
        .expect("live");
    let mut fillers = Vec::new();
    for index in 0..CAUSAL_PENDING_MAX {
        let scope = scopes
            .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                plugin_key: format!("fill{index}"),
            }))
            .expect("mint");
        fillers.push(scope);
    }
    let bridge = hub.entity_publish_bridge();
    scopes.test_with_inner_held(|| {
        for (index, scope) in fillers.iter().enumerate() {
            assert_eq!(
                scopes.transfer(
                    *scope,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("fill{index}"),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        family: "f".into(),
                        seq: index as u64,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        }
        for (index, scope) in fillers.iter().enumerate() {
            assert_eq!(
                bridge.park_release(CausalOp::Release {
                    scope_id: *scope,
                    identity: LeaseIdentity::AdmittedEntityMutation {
                        family: "f".into(),
                        seq: index as u64,
                    },
                }),
                CausalAdmitResult::Applied
            );
        }
        assert_eq!(bridge.release_count(), CAUSAL_PENDING_MAX);
        let overflow = release_or_retract(
            &scopes,
            live,
            LeaseIdentity::PendingEntityPublish {
                plugin_key: "lease-probe".into(),
            },
        );
        let CausalAdmitResult::Retry(overflow) = overflow else {
            panic!("full table and held inner must return the release");
        };
        assert_eq!(bridge.park_release(overflow), CausalAdmitResult::Applied);
        assert_eq!(bridge.release_count(), CAUSAL_PENDING_MAX + 1);
    });
    let _ = hub.apply_event_plane_owner_ops();
    while scopes.pending_ops() || hub.event_plane_owner_ops_pending() {
        let _ = hub.apply_event_plane_owner_ops();
        let _ = scopes.flush_pending();
    }
    assert!(!scopes.is_live(live));
}

#[test]
fn unfinished_finishes_are_bounded_and_sliced() {
    let registry = install_named_lua_package(
        "lease-unfinished",
        lease_probe_plugin(),
        lease_probe_manifest(),
    );
    let mut hub = explicit_runtime("lease-unfinished");
    hub.load_lua_plugin_package(&registry, "lease-probe")
        .expect("load");
    let scopes = hub.causal_scopes().clone();
    let first_scope = scopes.mint_with_lease(None).expect("first");
    let published = hub.invoke_plugin(scoped_command(
        "lease-probe",
        "publish",
        serde_json::json!({ "seq": 1 }),
        first_scope,
    ));
    assert!(matches!(
        published.result,
        PluginInvocationResult::Completed(_)
    ));
    let taken = hub.take_leased_package_entity_fanout();
    assert_eq!(taken.len(), 1);
    let mut fillers = Vec::new();
    for index in 0..CAUSAL_PENDING_MAX {
        fillers.push(
            scopes
                .mint_with_lease(Some(LeaseIdentity::PendingEntityPublish {
                    plugin_key: format!("fill{index}"),
                }))
                .expect("mint"),
        );
    }
    scopes.test_with_inner_held(|| {
        for (index, scope) in fillers.iter().enumerate() {
            assert_eq!(
                scopes.transfer(
                    *scope,
                    LeaseIdentity::PendingEntityPublish {
                        plugin_key: format!("fill{index}"),
                    },
                    [LeaseIdentity::AdmittedEntityMutation {
                        family: "f".into(),
                        seq: index as u64,
                    }],
                ),
                CausalAdmitResult::Applied
            );
        }
        for _ in 0..CAUSAL_PENDING_MAX {
            hub.finish_package_entity_mutation_fanout(&taken[0], false);
        }
        assert_eq!(hub.unfinished_finish_count(), CAUSAL_PENDING_MAX);
        hub.finish_package_entity_mutation_fanout(&taken[0], false);
        assert_eq!(hub.unfinished_finish_count(), CAUSAL_PENDING_MAX + 1);
    });
    let before = hub.unfinished_finish_count();
    let _ = hub.apply_event_plane_owner_ops();
    assert!(
        hub.unfinished_finish_count() < before,
        "owner turn must slice unfinished work: before={before} after={}",
        hub.unfinished_finish_count()
    );
    while scopes.pending_ops() || hub.event_plane_owner_ops_pending() {
        let _ = hub.apply_event_plane_owner_ops();
        let _ = scopes.flush_pending();
    }
    assert_eq!(hub.unfinished_finish_count(), 0);
    assert!(!scopes.is_live(first_scope));
}
