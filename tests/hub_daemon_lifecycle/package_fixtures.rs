#![allow(dead_code, unused_imports)]

use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, Capability, CapabilitySurface, CoreSessionMetadata,
    ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, HostProfileMetadata,
    HostProfilePolicySection, PackageSource, ProcessIdentity, RequestId, ResizePayload, SessionId,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, decrypt_aes_gcm,
    encrypt_aes_gcm,
};
use botster_core_daemon::{RegistryRecord, SessionRegistry};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, FileHubStateStore, HostIdentityOptions, HubClientApi,
    HubClientEvent, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState,
    HubPackageManifest, HubStartupOptions, HubStateLoadSource, HubStateStore,
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET, PackageAdmissionPolicy, PackageProvenance,
    PackageRegistry, RuntimeEnvironment, SessionDefaults, SpawnTarget, TransportBindings,
};
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{Receiver as AsyncReceiver, Sender as AsyncSender, channel, default_runtime};

use crate::support::{
    ensure_session_worker_binary, recovering_mutex_guard, validate_cli_daemon_shutdown,
    wait_for_cli_daemon_shutdown,
};

use super::*;

pub(crate) fn package_provenance() -> PackageProvenance {
    PackageProvenance {
        source: "https://example.invalid/botster/packages/provider".to_string(),
        checksum: Some("sha256:daemon-test".to_string()),
    }
}

pub(crate) fn provider_manifest() -> HubPackageManifest {
    let capabilities = vec![Capability {
        surface: CapabilitySurface::Surfaces,
        scope: None,
    }];

    HubPackageManifest {
        name: "daemon.provider".to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Provider,
        botster: ">=0.1.0".to_string(),
        source: Some(PackageSource::Git {
            repo: "https://example.invalid/botster/provider.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities: capabilities.clone(),
        entrypoints: vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Process,
            path: "bin/provider".to_string(),
            bootstrap: true,
        }],
        dependencies: Vec::new(),
        features: Vec::new(),
        host_profile: Some(HostProfileMetadata {
            profile_id: "daemon-provider".to_string(),
            compatibility: ">=0.1.0".to_string(),
            precedence: 10,
            required_providers: Vec::new(),
            required_capabilities: capabilities,
            policy_sections: vec![HostProfilePolicySection::Providers],
        }),
        configuration: None,
        surfaces: Vec::new(),
        runnable_entrypoints: Vec::new(),
        navigation: Vec::new(),
        events: botster_hub::HubPackageEvents::default(),
    }
}

pub(crate) fn write_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create local package root");
    fs::create_dir_all(root.join("bin")).expect("create local package bin");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(root.join("bin/botster-web"), "#!/bin/sh\n")
        .expect("write runnable package entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "runnable_entrypoints": [
    {
      "id": "web",
      "kind": "web_app",
      "command": "bin/botster-web",
      "args": ["--host", "127.0.0.1"],
      "working_directory": { "policy": "package_root" },
      "environment": [
        { "name": "BOTSTER_WEB_PORT", "required": false, "default": "5173" }
      ],
      "launch_mode": "background",
      "capabilities": [
        { "surface": "network", "scope": "localhost" }
      ],
      "may_supervise": true
    }
  ]
}

"#,
    )
    .expect("write local package manifest");
}

pub(crate) fn write_entity_provider_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create entity provider package root");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "name": "botster.plugin-contract-matrix", "version": "1.0.0", "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().expect("provider package path") },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "surfaces": [{ "id": "home", "kind": "app", "title": "Pipelines", "supports": ["render"] }]
        }))
        .expect("serialize entity provider manifest"),
    )
    .expect("write entity provider manifest");
    fs::write(
        root.join("plugin.lua"),
        r#"
local generation = 0
local family = "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run"
return botster.register({ handlers = {
  { id = "home", kind = "surface_route", descriptor_id = "home", call = function()
      return { type = "panel", id = "home", children = {{ ["$kind"] = "bind_list",
        source = "/" .. family, item_template = { type = "text",
          id = { ["$bind"] = "@/id" }, props = { text = { ["$bind"] = "@/status" } } } }} }
    end },
  { id = "runs", kind = "entity_provider", descriptor_id = family,
    descriptor = { entity_type = family, id_field = "id" }, call = function()
      generation = generation + 1
      return { type = "entity_snapshot", entity_type = family, snapshot_seq = generation,
        items = {{ id = "run-1", status = "generation-" .. generation }} }
    end },
} })
"#,
    )
    .expect("write entity provider plugin");
}

pub(crate) fn write_resource_bound_plugin_package(root: &Path, package_name: &str) {
    fs::create_dir_all(root).expect("create resource-bound package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write resource-bound plugin entrypoint");
    let manifest = serde_json::json!({
        "name": package_name,
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize resource-bound package manifest"),
    )
    .expect("write resource-bound package manifest");
}

pub(crate) fn write_managed_git_session_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create managed Git package root");
    fs::write(
        root.join("plugin.lua"),
        r#"
return botster.register({
  tools = {{
    name = "managed_git.live_spawn",
    description = "Exercise the live Hub managed Git session path.",
    handler = "live_spawn",
    call = function(args)
      return botster.capabilities.session_types.ensure_worktree_and_spawn(args)
    end,
  }},
})
"#,
    )
    .expect("write managed Git plugin");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'live-managed\\n' > live-managed.txt\n",
    )
    .expect("write managed Git session command");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))
        .expect("make managed Git session command executable");
    let source_root = fs::canonicalize(root).expect("canonical managed Git package root");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "name": "managed-git.live-plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": source_root },
            "capabilities": [
                { "surface": "mcp" },
                {
                    "surface": "session_actions",
                    "scope": "session_type_managed_git_spawn"
                }
            ],
            "entrypoints": [
                { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
            ],
            "session_types": [{
                "id": "init",
                "label": "Managed agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "traits": ["test"],
                "lifecycle": "task",
                "command": "bin/init.sh",
                "target_id": "tgt_live_managed"
            }]
        }))
        .expect("serialize managed Git package"),
    )
    .expect("write managed Git package manifest");
}

pub(crate) fn write_configurable_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create configurable package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "configurable.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "config.home",
    "kind": "app",
    "title": "Config Home",
    "description": "Configuration workbench",
    "icon": "settings",
    "order": 10,
    "category": "configuration",
    "supports": ["render", "action"]
  }],
  "configuration": {
    "fields": [
      {
        "key": "endpoint",
        "type": "url",
        "label": "Endpoint",
        "required": true
      },
      {
        "key": "mode",
        "type": "select",
        "label": "Mode",
        "default": { "type": "select", "value": "read" },
        "options": [
          { "value": "read", "label": "Read" }
        ]
      },
      {
        "key": "api_token",
        "type": "secret",
        "label": "API token",
        "required": true,
        "default": { "type": "secret", "state": "unset" }
      }
    ]
  }
}
"#,
    )
    .expect("write configurable package manifest");
}

pub(crate) fn write_explicit_navigation_local_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create navigation package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "navigation.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "workbench",
    "kind": "app",
    "title": "Workbench",
    "description": "Navigation workbench",
    "icon": "workflow",
    "order": 100,
    "category": "workflows",
    "supports": ["render", "action"]
  }],
  "navigation": [{
    "id": "primary",
    "label": "Primary Workbench",
    "icon": "workflow",
    "description": "Open the workbench",
    "target": { "kind": "surface", "surface_id": "workbench" }
  }]
}
"#,
    )
    .expect("write explicit navigation package manifest");
}

pub(crate) fn write_iframe_surface_local_plugin_package(root: &Path) {
    fs::create_dir_all(root.join("assets")).expect("create iframe package assets");
    fs::write(root.join("assets/preview.html"), "<main>Preview</main>\n")
        .expect("write iframe asset");
    fs::write(
        root.join("plugin.lua"),
        r#"local function render_preview(_arguments)
  return {
    type = "iframe",
    id = "preview-frame",
    props = {
      src = "/packages/iframe.plugin/assets/preview.html",
      title = "Preview"
    }
  }
end

return botster.register({
  handlers = {
    {
      id = "preview_surface",
      kind = "surface_route",
      descriptor_id = "preview",
      descriptor = {
        title = "Preview",
        surface_id = "preview",
      },
      call = render_preview,
    },
  },
})
"#,
    )
    .expect("write iframe plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "iframe.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [{
    "id": "preview",
    "kind": "app",
    "title": "Preview",
    "description": "Iframe preview",
    "icon": "panel-top",
    "order": 30,
    "category": "previews",
    "supports": ["render"]
  }]
}
"#,
    )
    .expect("write iframe package manifest");
}

pub(crate) fn write_project_pipelines_availability_package(root: &Path) {
    fs::create_dir_all(root).expect("create project pipelines package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "project-pipelines",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" },
    { "surface": "mcp" },
    { "surface": "plugin_db", "scope": "project-pipelines" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "dependencies": [
    {
      "id": "github-provider",
      "package": "github-provider",
      "kind": "optional",
      "feature": "github_pr_lifecycle",
      "requirements": [
        { "type": "provider", "provider": "github-provider" }
      ]
    }
  ],
  "features": [
    {
      "id": "local_pipelines",
      "label": "Local pipelines"
    },
    {
      "id": "github_pr_lifecycle",
      "label": "GitHub PR lifecycle",
      "dependencies": ["github-provider"],
      "requirements": [
        { "type": "config", "key": "github_app_id" },
        { "type": "auth", "key": "github_token" }
      ]
    }
  ]
}
"#,
    )
    .expect("write project pipelines availability manifest");
}

pub(crate) fn write_required_dependency_package(root: &Path) {
    fs::create_dir_all(root).expect("create required dependency package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write required dependency plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "dependency-blocked.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "dependencies": [
    {
      "id": "github-provider",
      "package": "github-provider",
      "kind": "required",
      "requirements": [
        { "type": "provider", "provider": "github-provider" }
      ]
    }
  ]
}
"#,
    )
    .expect("write required dependency package manifest");
}

pub(crate) fn write_supervised_package(
    root: &Path,
    package_name: &str,
    command: &str,
    args: &[&str],
) {
    fs::create_dir_all(root).expect("create supervised package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let manifest = serde_json::json!({
        "name": package_name,
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": command,
            "args": args,
            "working_directory": { "policy": "package_root" },
            "launch_mode": "background",
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize supervised manifest"),
    )
    .expect("write supervised package manifest");
}

pub(crate) fn write_session_type_context_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create session type package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write session type plugin entrypoint");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'started\\n' > context-started.txt\n\"$BOTSTER_HUB_BIN\" context --key prompt > context-output.json 2> context-error.txt\nsleep 1\n",
    )
    .expect("write session type script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session type script");
    let manifest = serde_json::json!({
        "name": "runtime.session-type",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "session_types": [{
            "id": "init",
            "label": "Daemon agent",
            "role": "botster.agent",
            "interaction": "interactive",
            "traits": ["test"],
            "lifecycle": "task",
            "command": "bin/init.sh",
            "context": ["prompt"],
            "allowed_environment_overrides": ["BOTSTER_MODE"],
            "environment": { "BOTSTER_MODE": "daemon" }
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize session type manifest"),
    )
    .expect("write session type package manifest");
}

pub(crate) fn write_session_type_execution_package(root: &Path) {
    fs::create_dir_all(root.join("bin")).expect("create execution package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write execution plugin entrypoint");
    let relative_script = root.join("bin/relative.sh");
    fs::write(
        &relative_script,
        "#!/bin/sh\nprintf 'relative:%s\\n' \"$1\" > relative-output.txt\nsleep 30\n",
    )
    .expect("write relative executable");
    fs::set_permissions(&relative_script, fs::Permissions::from_mode(0o755))
        .expect("make relative executable runnable");
    let manifest = serde_json::json!({
        "name": "runtime.session-type-execution",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "session_types": [
            {
                "id": "relative",
                "label": "Relative executable",
                "role": "botster.agent",
                "interaction": "interactive",
                "lifecycle": "task",
                "execution": { "mode": "relative_executable" },
                "command": "bin/relative.sh",
                "args": ["explicit"]
            },
            {
                "id": "shell",
                "label": "Shell command",
                "role": "botster.agent",
                "interaction": "interactive",
                "lifecycle": "task",
                "execution": { "mode": "shell_command" },
                "command": "printf 'shell:%s:%s\\n' \"$1\" \"$2\" > shell-output.txt; sleep 30",
                "args": ["alpha", "beta"]
            },
            {
                "id": "not-inferred",
                "label": "Shell-looking relative executable",
                "role": "botster.agent",
                "interaction": "interactive",
                "lifecycle": "task",
                "command": "printf shell-must-not-run > inferred-shell-output.txt"
            }
        ]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize execution package"),
    )
    .expect("write execution package manifest");
}

pub(crate) fn write_app_registry_package(root: &Path) {
    fs::create_dir_all(root).expect("create app registry package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let manifest = serde_json::json!({
        "name": "runtime.apps",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [
            {
                "id": "web",
                "kind": "web_app",
                "command": "sh",
                "args": ["-c", "echo 'http://127.0.0.1:59999'; printf '%s\n' '{\"entrypoint_id\":\"web\",\"process_state\":\"running\",\"local_url\":\"http://127.0.0.1:49152\"}' > \"$BOTSTER_ENTRYPOINT_LAUNCH_RESULT\"; while true; do sleep 1; done"],
                "working_directory": { "policy": "package_root" },
                "launch_mode": "background",
                "readiness": { "result_fields": ["local_url"] },
                "capabilities": [{ "surface": "network", "scope": "localhost" }],
                "may_supervise": true
            },
            {
                "id": "terminal",
                "kind": "terminal_app",
                "command": "sh",
                "args": ["-c", "echo terminal"],
                "working_directory": { "policy": "package_root" },
                "launch_mode": "foreground_stdio",
                "may_supervise": true
            }
        ]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize app registry manifest"),
    )
    .expect("write app registry package manifest");
}

pub(crate) fn write_reloadable_app_package(root: &Path, version: &str, local_url: &str) {
    write_reloadable_app_package_named(root, "runtime.reloadable", version, local_url);
}

pub(crate) fn write_reloadable_app_package_named(
    root: &Path,
    name: &str,
    version: &str,
    local_url: &str,
) {
    fs::create_dir_all(root).expect("create reloadable app package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write reloadable app plugin entrypoint");
    let command = format!(
        "printf '%s\n' '{{\"entrypoint_id\":\"web\",\"process_state\":\"running\",\"local_url\":\"{local_url}\"}}' > \"$BOTSTER_ENTRYPOINT_LAUNCH_RESULT\"; while true; do sleep 1; done"
    );
    let manifest = serde_json::json!({
        "name": name,
        "version": version,
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": "sh",
            "args": ["-c", command],
            "working_directory": { "policy": "package_root" },
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize reloadable app package manifest"),
    )
    .expect("write reloadable app package manifest");
}

pub(crate) fn write_hub_env_web_app_package(root: &Path) {
    fs::create_dir_all(root).expect("create hub-env web package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write hub-env web package core entrypoint");
    fs::write(
        root.join("verify-hub-connection.mjs"),
        r#"import fs from 'node:fs';

const connection = JSON.parse(process.env.BOTSTER_HUB_CONNECTION || 'null');
if (connection?.transport?.type !== 'unix_socket') {
  throw new Error('BOTSTER_HUB_CONNECTION must declare a unix_socket transport');
}
if (!connection.transport.path.startsWith('/') || !fs.existsSync(connection.transport.path)) {
  throw new Error('BOTSTER_HUB_CONNECTION must carry the active absolute socket path');
}
if (!process.env.PACKAGE_DATA_DIR || !fs.statSync(process.env.PACKAGE_DATA_DIR).isDirectory()) {
  throw new Error('PACKAGE_DATA_DIR must carry the active Hub data directory');
}
if (process.env.BOTSTER_WEB_MODE !== 'daemon-default') {
  throw new Error('manifest environment defaults must be preserved');
}
fs.writeFileSync(process.env.BOTSTER_ENTRYPOINT_LAUNCH_RESULT, JSON.stringify({
  entrypoint_id: 'web',
  process_state: 'running',
  local_url: 'http://127.0.0.1:49153',
}));
setInterval(() => {}, 1000);
"#,
    )
    .expect("write hub connection verifier");
    let manifest = serde_json::json!({
        "name": "runtime.hub-env",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [
            { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
        ],
        "runnable_entrypoints": [{
            "id": "web",
            "kind": "web_app",
            "command": "node",
            "args": [
                "verify-hub-connection.mjs"
            ],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "PACKAGE_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_WEB_MODE", "required": false, "default": "daemon-default" }
            ],
            "launch_mode": "background",
            "readiness": { "result_fields": ["local_url"] },
            "capabilities": [{ "surface": "network", "scope": "localhost" }],
            "may_supervise": true
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize hub-env web package manifest"),
    )
    .expect("write hub-env web package manifest");
}

pub(crate) fn write_botster_tui_package(root: &Path) {
    write_botster_tui_package_with_script(
        root,
        "test -n \"$BOTSTER_HUB_CONNECTION\" && test -n \"$BOTSTER_HUB_DATA_DIR\" && printf 'botster-tui-fixture\\n'",
    );
}

pub(crate) fn write_botster_tui_package_with_script(root: &Path, script: &str) {
    fs::create_dir_all(root).expect("create botster-tui package root");
    let manifest = serde_json::json!({
        "name": "botster-tui",
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "surfaces" }],
        "entrypoints": [],
        "runnable_entrypoints": [{
            "id": "botster-tui",
            "kind": "terminal_app",
            "command": "sh",
            "args": ["-c", script],
            "working_directory": { "policy": "package_root" },
            "injections": [
                {
                    "kind": "hub_connection",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_CONNECTION"
                    },
                    "required": true
                },
                {
                    "kind": "data_dir",
                    "target": {
                        "type": "environment",
                        "name": "BOTSTER_HUB_DATA_DIR"
                    },
                    "required": true
                }
            ],
            "environment": [
                { "name": "BOTSTER_TUI_MODE", "required": false, "default": "headless" }
            ],
            "launch_mode": "foreground_stdio"
        }]
    });
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize botster-tui manifest"),
    )
    .expect("write botster-tui manifest");
}

pub(crate) fn enable_supervised_package(data_dir: &Path, package_dir: &Path) {
    let response = botster_hub::daemon_transport_request(
        &explicit_config(data_dir),
        botster_hub::DaemonRequest::EnablePackageLocalPath {
            path: package_dir.to_path_buf(),
        },
    )
    .expect("enable supervised package");
    assert_eq!(
        response.kind,
        botster_hub::DaemonResponseKind::PackageDecision
    );
}

pub(crate) fn package_entrypoint<'a>(
    response: &'a botster_hub::DaemonResponse,
    package_name: &str,
) -> &'a botster_hub::DaemonPackageRunnableEntrypoint {
    response
        .packages
        .iter()
        .find(|package| package.package_name == package_name)
        .expect("response includes package")
        .runnable_entrypoints
        .iter()
        .find(|entrypoint| entrypoint.id == "web")
        .expect("response includes web entrypoint")
}

pub(crate) fn app_row<'a>(
    response: &'a botster_hub::DaemonResponse,
    entrypoint_id: &str,
) -> &'a botster_hub::DaemonApp {
    response
        .apps
        .iter()
        .find(|app| app.entrypoint_id == entrypoint_id)
        .unwrap_or_else(|| panic!("response includes app for entrypoint {entrypoint_id}"))
}

pub(crate) fn package_route<'a>(
    routes: &'a [botster_hub_client::DaemonPackageRouteDescriptor],
    route_id: &str,
) -> &'a botster_hub_client::DaemonPackageRouteDescriptor {
    routes
        .iter()
        .find(|route| route.route_id == route_id)
        .unwrap_or_else(|| panic!("response includes package route {route_id}"))
}

pub(crate) fn package_navigation<'a>(
    entries: &'a [botster_hub_client::DaemonPackageNavigationEntry],
    package_name: &str,
    item_id: &str,
) -> &'a botster_hub_client::DaemonPackageNavigationEntry {
    entries
        .iter()
        .find(|entry| entry.package_name == package_name && entry.item_id == item_id)
        .unwrap_or_else(|| panic!("response includes navigation {package_name}/{item_id}"))
}

pub(crate) fn package_action<'a>(
    actions: &'a [botster_hub::DaemonPackageActionState],
    action_id: &str,
) -> &'a botster_hub::DaemonPackageActionState {
    actions
        .iter()
        .find(|action| action.action_id == action_id)
        .unwrap_or_else(|| panic!("response includes {action_id} action"))
}

pub(crate) fn write_declared_surface_plugin_package(root: &Path) {
    fs::create_dir_all(root).expect("create declared surface package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.surface-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "surfaces": [
    {
      "id": "runtime.surface.home",
      "kind": "app",
      "title": "Runtime Surface",
      "description": "Surface descriptor fixture",
      "icon": "workflow",
      "order": 20,
      "category": "runtime",
      "supports": ["render", "action"]
    },
    {
      "id": "runtime.surface.settings",
      "kind": "settings",
      "title": "Runtime Settings",
      "supports": ["render"]
    }
  ]
}
"#,
    )
    .expect("write declared surface package manifest");
}

pub(crate) fn write_invalid_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create invalid package root");
    fs::write(root.join("botster-package.json"), "{ invalid json\n")
        .expect("write invalid manifest");
}

pub(crate) fn write_incompatible_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create incompatible package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.incompatible-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=999.0.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write incompatible package manifest");
}

pub(crate) fn write_denied_capability_local_package(root: &Path) {
    fs::create_dir_all(root).expect("create denied capability package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "runtime.denied-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "filesystem", "scope": "home" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
"#,
    )
    .expect("write denied capability package manifest");
}

pub(crate) fn write_botster_workspaces_local_package(root: &Path, plugin_db_scope: &str) {
    fs::create_dir_all(root).expect("create botster-workspaces package root");
    fs::write(
        root.join("plugin.lua"),
        r#"local function workspace_id(arguments)
  if type(arguments.workspace_id) == "string" and arguments.workspace_id ~= "" then
    return arguments.workspace_id
  end
  return "workspace-local-1"
end

local function create(arguments)
  local target_id = arguments.target_id
  local target_validation = nil
  if type(target_id) == "string" and target_id ~= "" then
    target_validation = botster.capabilities.spawn_targets.validate({ target_id = target_id })
    if not target_validation.ok then
      return { ok = false, status = target_validation.status, target_id = target_id }
    end
  else
    target_id = nil
  end
  local workspace = {
    id = workspace_id(arguments),
    name = arguments.name or "Local Workspace",
    status = "created",
    target_id = target_id,
  }
  botster.capabilities.plugin_db.set({
    key = "workspace/" .. workspace.id,
    schema_version = 1,
    payload = workspace,
  })
  return { ok = true, workspace = workspace }
end

local function use_workspace(arguments)
  local id = workspace_id(arguments)
  local record = botster.capabilities.plugin_db.get({ key = "workspace/" .. id })
  local workspace = record.record.payload
  if type(arguments.target_id) == "string" and arguments.target_id ~= "" then
    local validation = botster.capabilities.spawn_targets.validate({ target_id = arguments.target_id })
    if not validation.ok then
      return { ok = false, status = validation.status, target_id = arguments.target_id }
    end
    workspace.target_id = arguments.target_id
  end
  workspace.status = "used"
  botster.capabilities.plugin_db.set({
    key = "workspace/" .. workspace.id,
    schema_version = 1,
    payload = workspace,
  })
  return { ok = true, workspace = workspace }
end

local function validate_target(arguments)
  local target_id = arguments.target_id
  if type(target_id) ~= "string" or target_id == "" then
    return { ok = false, status = "missing_argument" }
  end
  return botster.capabilities.spawn_targets.validate({ target_id = target_id })
end

local function render_workspaces(_arguments)
  return {
    type = "panel",
    id = "botster-workspaces-panel",
    props = {
      title = "Workspaces",
    },
    children = {
      {
        type = "text",
        id = "botster-workspaces-title",
        props = {
          text = "Workspaces",
        },
      },
    },
  }
end

return botster.register({
  handlers = {
    {
      id = "workspaces_surface",
      kind = "surface_route",
      descriptor_id = "workspaces",
      descriptor = {
        title = "Workspaces",
        surface_id = "workspaces",
      },
      call = render_workspaces,
    },
  },
  tools = {
    {
      name = "botster_workspaces.create",
      description = "Create a constrained local workspace.",
      input_schema = {
        type = "object",
        properties = {
          workspace_id = { type = "string" },
          name = { type = "string" },
          target_id = { type = "string" },
        },
        additionalProperties = false,
      },
      handler = "create",
      call = create,
    },
    {
      name = "botster_workspaces.use",
      description = "Use a constrained local workspace.",
      input_schema = {
        type = "object",
        properties = {
          workspace_id = { type = "string" },
          target_id = { type = "string" },
        },
        additionalProperties = false,
      },
      handler = "use",
      call = use_workspace,
    },
    {
      name = "botster_workspaces.validate_target",
      description = "Validate a hub-owned spawn target reference for a workspace.",
      input_schema = {
        type = "object",
        properties = {
          target_id = { type = "string" },
        },
        required = { "target_id" },
        additionalProperties = false,
      },
      handler = "validate_target",
      call = validate_target,
    },
  },
})
"#,
    )
    .expect("write plugin entrypoint");
    fs::write(
        root.join("botster-package.json"),
        format!(
            r#"{{
  "name": "botster-workspaces",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": {{ "type": "path", "path": "." }},
  "capabilities": [
    {{ "surface": "mcp" }},
    {{ "surface": "plugin_db", "scope": "{plugin_db_scope}" }},
    {{ "surface": "surfaces" }},
    {{ "surface": "filesystem", "scope": "workspace" }}
  ],
  "surfaces": [
    {{
      "id": "workspaces",
      "kind": "app",
      "title": "Workspaces",
      "supports": ["render"]
    }}
  ],
  "entrypoints": [
    {{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }}
  ]
}}
"#
        ),
    )
    .expect("write botster-workspaces package manifest");
}

pub(crate) fn write_local_runtime_daemon_metadata(data_dir: &Path, pid: u32) {
    let config = explicit_config(data_dir);
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .expect("local socket binding")
        .path
        .clone();
    let metadata = serde_json::json!({
        "pid": pid,
        "data_directory": stable_path_string(data_dir),
        "data_directory_arg": data_dir.display().to_string(),
        "socket_path": socket_path.display().to_string(),
        "hub_bin": stable_path_string(Path::new(env!("CARGO_BIN_EXE_botster-hub"))),
    });
    let metadata_path = data_dir.join(".botster-hub-runtime-daemon.json");
    fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata).expect("serialize daemon metadata"),
    )
    .expect("write daemon metadata");
    assert!(metadata_path.exists(), "daemon metadata should exist");
}

pub(crate) fn write_python_wait_then_write_script(release_path: &Path, bytes: &[u8]) -> PathBuf {
    let script_path = unique_short_test_dir("live-output-script").join("write.py");
    fs::create_dir_all(script_path.parent().expect("script parent")).expect("create script dir");
    fs::write(
        &script_path,
        format!(
            "import os\nimport time\nprint({ready:?}, flush=True)\np = {path:?}\nwhile not os.path.exists(p):\n    time.sleep(0.01)\nos.write(1, bytes([{bytes}]))\n",
            ready = PRODUCER_READY_MARKER,
            path = release_path,
            bytes = python_bytes_literal(bytes),
        ),
    )
    .expect("write wait-then-write script");
    script_path
}

pub(crate) fn write_python_start_then_write_script(
    start_path: &Path,
    release_path: &Path,
    bytes: &[u8],
) -> PathBuf {
    let script_path = unique_short_test_dir("started-live-output-script").join("write.py");
    fs::create_dir_all(script_path.parent().expect("script parent")).expect("create script dir");
    fs::write(
        &script_path,
        format!(
            "import os\nimport time\ns = {start:?}\nwhile not os.path.exists(s):\n    time.sleep(0.01)\nprint({ready:?}, flush=True)\np = {release:?}\nwhile not os.path.exists(p):\n    time.sleep(0.01)\nos.write(1, bytes([{bytes}]))\n",
            start = start_path,
            ready = PRODUCER_READY_MARKER,
            release = release_path,
            bytes = python_bytes_literal(bytes),
        ),
    )
    .expect("write start-then-write script");
    script_path
}

pub(crate) fn write_python_held_live_script(
    release_path: &Path,
    exit_release_path: &Path,
    bytes: &[u8],
) -> PathBuf {
    let script_path = unique_short_test_dir("held-live-script").join("write.py");
    fs::create_dir_all(script_path.parent().expect("script parent")).expect("create script dir");
    fs::write(
        &script_path,
        format!(
            "import os\nimport time\nprint({ready:?}, flush=True)\np = {release:?}\nwhile not os.path.exists(p):\n    time.sleep(0.01)\nos.write(1, bytes([{bytes}]))\ne = {exit_release:?}\nwhile not os.path.exists(e):\n    time.sleep(0.01)\n",
            ready = PRODUCER_READY_MARKER,
            release = release_path,
            exit_release = exit_release_path,
            bytes = python_bytes_literal(bytes),
        ),
    )
    .expect("write held-live script");
    script_path
}

pub(crate) fn write_python_split_utf8_script(
    first_release: &Path,
    second_release: &Path,
) -> PathBuf {
    let script_path = unique_short_test_dir("live-split-script").join("write.py");
    fs::create_dir_all(script_path.parent().expect("script parent")).expect("create script dir");
    fs::write(
        &script_path,
        format!(
            "import os\nimport time\na = {first:?}\nb = {second:?}\nwhile not os.path.exists(a):\n    time.sleep(0.01)\nos.write(1, bytes([226]))\nwhile not os.path.exists(b):\n    time.sleep(0.01)\nos.write(1, bytes([130, 172]))\n",
            first = first_release,
            second = second_release,
        ),
    )
    .expect("write split UTF-8 script");
    script_path
}

pub(crate) fn incomplete_repo_session_types_json() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "session_types": [{
            "id": "acceptance",
            "command": "bin/acceptance-session.sh",
            "working_directory": { "policy": "package_root" }
        }]
    }))
    .expect("serialize incomplete session types")
}

pub(crate) fn complete_repo_session_types_json(id: &str) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "session_types": [{
            "id": id,
            "label": "Repo acceptance agent",
            "role": "botster.agent",
            "interaction": "interactive",
            "lifecycle": "task",
            "command": "bin/acceptance-session.sh",
            "working_directory": { "policy": "package_root" }
        }]
    }))
    .expect("serialize complete session types")
}

pub(crate) fn write_repo_session_types_file(root: &Path, body: &str) {
    fs::create_dir_all(root.join(".botster")).expect("create .botster");
    fs::write(root.join(".botster/session-types.json"), body)
        .expect("write repo session-types.json");
}

pub(crate) fn init_git_repo_with_main(root: &Path) {
    fs::create_dir_all(root).expect("create git root");
    run_fixture_git(None, &["init", "-b", "main", path_str(root)]);
    run_fixture_git(Some(root), &["config", "user.email", "test@example.com"]);
    run_fixture_git(Some(root), &["config", "user.name", "Test"]);
    fs::write(root.join("README.md"), "session-types fixture\n").expect("write readme");
    run_fixture_git(Some(root), &["add", "README.md"]);
    run_fixture_git(Some(root), &["commit", "-m", "init"]);
}

/// One canned HTTP response: status plus body.
pub(crate) type ManagedReleaseResponse = (u16, Vec<u8>);
/// The mutable route table the managed release origin serves from.
pub(crate) type ManagedReleaseRoutes = Arc<Mutex<BTreeMap<String, ManagedReleaseResponse>>>;

/// A loopback origin whose routes can be replaced between requests.
pub(crate) struct ManagedReleaseOrigin {
    pub(crate) base: String,
    pub(crate) routes: ManagedReleaseRoutes,
    pub(crate) stopping: Arc<AtomicBool>,
    pub(crate) address: String,
    pub(crate) handle: Option<thread::JoinHandle<()>>,
}

impl ManagedReleaseOrigin {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind managed release origin");
        let address = listener
            .local_addr()
            .expect("managed release origin address");
        let routes: ManagedReleaseRoutes = Arc::new(Mutex::new(BTreeMap::new()));
        let served = Arc::clone(&routes);
        let stopping = Arc::new(AtomicBool::new(false));
        let halt = Arc::clone(&stopping);
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                if halt.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut buffer = [0_u8; 8192];
                let Ok(read) = stream.read(&mut buffer) else {
                    continue;
                };
                let request = String::from_utf8_lossy(&buffer[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let (status, body) = served
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&path)
                    .cloned()
                    .unwrap_or((404_u16, Vec::new()));
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(&body);
                let _ = stream.flush();
            }
        });
        Self {
            base: format!("http://{address}"),
            routes,
            stopping,
            address: address.to_string(),
            handle: Some(handle),
        }
    }

    pub(crate) fn serve(&self, path: &str, body: Vec<u8>) {
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(path.to_string(), (200_u16, body));
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

impl Drop for ManagedReleaseOrigin {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(&self.address);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The real, revision-coupled artifacts this repository produces.
pub(crate) struct RealRelease {
    pub(crate) hub_binary: PathBuf,
    pub(crate) worker_binary: PathBuf,
    pub(crate) hub_revision: String,
    pub(crate) core_revision: String,
}

pub(crate) fn release_tool_binary() -> PathBuf {
    Path::new(env!("CARGO_BIN_EXE_botster-hub"))
        .parent()
        .expect("hub binary directory")
        .join("botster-hub-release-tool")
}

pub(crate) fn sha256_of(path: &Path) -> String {
    let output = if Command::new("sha256sum").arg("--version").output().is_ok() {
        Command::new("sha256sum").arg(path).output()
    } else {
        Command::new("shasum")
            .args(["-a", "256"])
            .arg(path)
            .output()
    }
    .expect("compute artifact checksum");
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .expect("checksum output")
        .to_string()
}

/// Install the real built pair into an isolated prefix through the installer.
///
/// `HOME` points at the prefix, so the receipt, the generations, the pointer,
/// the entrypoint, and the lease are all isolated from the developer's machine.
pub(crate) fn install_real_release(
    label: &str,
    origin: &ManagedReleaseOrigin,
) -> (PathBuf, &'static RealRelease) {
    let release = build_real_release();
    let prefix = unique_short_test_dir(label);
    fs::create_dir_all(&prefix).expect("create managed prefix");

    let hub_bytes = fs::read(&release.hub_binary).expect("read built Hub");
    let worker_bytes = fs::read(&release.worker_binary).expect("read built worker");
    origin.serve("/artifacts/botster-hub", hub_bytes.clone());
    origin.serve("/artifacts/botster-session-worker", worker_bytes.clone());

    let manifest = serde_json::json!({
        "product_id": "botster-hub",
        "release_channel": "stable",
        "version": env!("CARGO_PKG_VERSION"),
        "build_revision": release.hub_revision,
        "source_revisions": {
            "botster_hub": release.hub_revision,
            "botster_core": release.core_revision,
        },
        "artifacts": [
            {
                "name": "botster-hub",
                "url": origin.url("/artifacts/botster-hub"),
                "size": hub_bytes.len(),
                "sha256": sha256_of(&release.hub_binary),
            },
            {
                "name": "botster-session-worker",
                "url": origin.url("/artifacts/botster-session-worker"),
                "size": worker_bytes.len(),
                "sha256": sha256_of(&release.worker_binary),
            },
        ],
    });

    let manifest_path = prefix.join("install-manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");
    let document_path = prefix.join("release.json");
    let signed =
        Command::new(release_tool_binary())
            .args(["sign", "--key"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "fixtures/release-signing/UNTRUSTED-TEST-ONLY-botster-hub-release-signing.pkcs8",
            ))
            .args(["--key-id", "test-only-do-not-trust", "--manifest"])
            .arg(&manifest_path)
            .arg("--out")
            .arg(&document_path)
            .output()
            .expect("sign the release document");
    assert!(signed.status.success(), "{}", command_output_text(&signed));
    origin.serve(
        "/botster-hub.json",
        fs::read(&document_path).expect("read signed document"),
    );

    let installed =
        Command::new(installer_binary())
            .arg("install")
            .arg("--prefix")
            .arg(&prefix)
            .arg("--source")
            .arg(origin.url("/botster-hub.json"))
            .arg("--trust-anchor")
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "fixtures/release-signing/UNTRUSTED-TEST-ONLY-botster-hub-release-signing.pub",
            ))
            .env("HOME", &prefix)
            .output()
            .expect("run the managed installer");
    assert!(
        installed.status.success(),
        "install_real_release failed: status={:?} {}",
        installed.status,
        command_output_text(&installed)
    );

    (prefix, release)
}

pub(crate) fn write_package_entity_mutation_plugin(root: &Path, provider_mode: &str) {
    fs::create_dir_all(root).expect("create mutation package");
    fs::write(
        root.join("botster-package.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "name": "project-pipelines",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": root.canonicalize().unwrap_or_else(|_| root.to_path_buf()) },
            "capabilities": [{ "surface": "surfaces" }],
            "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
            "surfaces": [{
                "id": "home",
                "kind": "app",
                "title": "Pipelines",
                "supports": ["render", "action"]
            }]
        }))
        .expect("serialize mutation manifest"),
    )
    .expect("write mutation manifest");
    // provider_mode:
    //  - "live": provider reflects rows + seq
    //  - "behind": provider always returns fixed behind snapshot (seq 0, empty)
    //  - "stale_then_live": first few calls behind, then live (via generation file)
    let plugin = match provider_mode {
        "behind" => r#"
local rows = {}
local seq = 0
local family = "project-pipelines.membership"
return botster.register({ handlers = {
  { id = "home", kind = "surface_route", descriptor_id = "home",
    call = function()
      return { type = "panel", id = "home", children = {} }
    end },
  { id = "runs", kind = "entity_provider", descriptor_id = family,
    descriptor = { entity_type = family, id_field = "id" },
    call = function()
      return { type = "entity_snapshot", entity_type = family, snapshot_seq = 0, items = {} }
    end },
  { id = "claim", kind = "ui_action", descriptor_id = "project-pipelines.claim",
    descriptor = { action_id = "project-pipelines.claim", surface_id = "home" },
    call = function(args)
      local id = (args.payload and args.payload.id) or "m-1"
      seq = seq + 1
      rows[id] = { id = id, status = "claimed", seq = seq }
      local published = botster.entity_publish({
        type = "entity_upsert", entity_type = family, snapshot_seq = seq,
        id = id, entity = rows[id],
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
  { id = "remove", kind = "ui_action", descriptor_id = "project-pipelines.remove",
    descriptor = { action_id = "project-pipelines.remove", surface_id = "home" },
    call = function(args)
      local id = (args.payload and args.payload.id) or "m-1"
      seq = seq + 1
      rows[id] = nil
      local published = botster.entity_publish({
        type = "entity_remove", entity_type = family, snapshot_seq = seq, id = id,
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
  { id = "publish_seq", kind = "ui_action", descriptor_id = "project-pipelines.publish_seq",
    descriptor = { action_id = "project-pipelines.publish_seq", surface_id = "home" },
    call = function(args)
      local target = (args.payload and args.payload.seq) or (seq + 1)
      local id = (args.payload and args.payload.id) or ("m-" .. tostring(target))
      local row = { id = id, status = "seq-" .. tostring(target), seq = target }
      if args.payload and args.payload.blob then row.blob = args.payload.blob end
      rows[id] = row
      if target > seq then seq = target end
      local published = botster.entity_publish({
        type = "entity_upsert", entity_type = family, snapshot_seq = target,
        id = id, entity = rows[id],
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
} })
"#
        .to_string(),
        _ => r#"
local rows = {}
local seq = 0
local family = "project-pipelines.membership"
return botster.register({ handlers = {
  { id = "home", kind = "surface_route", descriptor_id = "home",
    call = function()
      return { type = "panel", id = "home", children = {} }
    end },
  { id = "runs", kind = "entity_provider", descriptor_id = family,
    descriptor = { entity_type = family, id_field = "id" },
    call = function()
      local items = {}
      for _, row in pairs(rows) do items[#items + 1] = row end
      return {
        type = "entity_snapshot", entity_type = family, snapshot_seq = seq, items = items,
      }
    end },
  { id = "claim", kind = "ui_action", descriptor_id = "project-pipelines.claim",
    descriptor = { action_id = "project-pipelines.claim", surface_id = "home" },
    call = function(args)
      local id = (args.payload and args.payload.id) or "m-1"
      seq = seq + 1
      rows[id] = { id = id, status = "claimed", seq = seq }
      local published = botster.entity_publish({
        type = "entity_upsert", entity_type = family, snapshot_seq = seq,
        id = id, entity = rows[id],
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
  { id = "remove", kind = "ui_action", descriptor_id = "project-pipelines.remove",
    descriptor = { action_id = "project-pipelines.remove", surface_id = "home" },
    call = function(args)
      local id = (args.payload and args.payload.id) or "m-1"
      seq = seq + 1
      rows[id] = nil
      local published = botster.entity_publish({
        type = "entity_remove", entity_type = family, snapshot_seq = seq, id = id,
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
  { id = "publish_seq", kind = "ui_action", descriptor_id = "project-pipelines.publish_seq",
    descriptor = { action_id = "project-pipelines.publish_seq", surface_id = "home" },
    call = function(args)
      local target = (args.payload and args.payload.seq) or (seq + 1)
      local id = (args.payload and args.payload.id) or ("m-" .. tostring(target))
      local row = { id = id, status = "seq-" .. tostring(target), seq = target }
      if args.payload and args.payload.blob then row.blob = args.payload.blob end
      rows[id] = row
      if target > seq then seq = target end
      local published = botster.entity_publish({
        type = "entity_upsert", entity_type = family, snapshot_seq = target,
        id = id, entity = rows[id],
      })
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = published,
      }
    end },
  { id = "set_provider_seq", kind = "ui_action", descriptor_id = "project-pipelines.set_provider_seq",
    descriptor = { action_id = "project-pipelines.set_provider_seq", surface_id = "home" },
    call = function(args)
      seq = (args.payload and args.payload.seq) or seq
      return {
        request_id = args.request_id, surface_id = "home", action_id = args.action_id,
        node_id = args.node_id, state = "accepted", payload = { seq = seq },
      }
    end },
} })
"#
        .to_string(),
    };
    fs::write(root.join("plugin.lua"), plugin).expect("write mutation plugin");
}

pub(crate) fn enable_mutation_package(
    endpoint: &botster_hub_client::DaemonEndpoint,
    package_dir: PathBuf,
) {
    let enabled = botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::EnablePackageLocalPath { path: package_dir },
    )
    .expect("enable mutation package");
    assert_eq!(
        enabled.kind,
        botster_hub_client::DaemonResponseKind::PackageDecision
    );
}

pub(crate) fn mutation_action(
    endpoint: &botster_hub_client::DaemonEndpoint,
    action_id: &str,
    payload: serde_json::Value,
) -> botster_hub_client::DaemonResponse {
    botster_hub_client::request(
        endpoint,
        botster_hub_client::DaemonRequest::PluginSurfaceAction {
            package_name: "project-pipelines".to_string(),
            request: botster_ui_contract::UiActionRequest {
                request_id: botster_ui_contract::UiActionRequestId(format!(
                    "mutation-{}",
                    action_id
                )),
                surface_id: botster_ui_contract::UiSurfaceId("home".to_string()),
                action_id: botster_ui_contract::UiActionId(action_id.to_string()),
                node_id: Some(botster_ui_contract::UiNodeId("mutation-form".to_string())),
                kind: botster_ui_contract::UiActionKind::Submit,
                values: None,
                payload: Some(payload),
            },
        },
    )
    .expect("surface action")
}
