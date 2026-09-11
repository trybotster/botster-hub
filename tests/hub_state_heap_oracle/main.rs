//! Isolated 1.97 clone-heap oracle for G2. Not the Hub process allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use botster_hub::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
use botster_hub::test_internals::hub_state_heap::walk_hub_state;
use botster_hub::persistence::HubState;

struct Counter;

static RECORDING: AtomicBool = AtomicBool::new(false);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORDING.load(Ordering::Acquire) {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counter = Counter;

fn measure(name: &str, state: &HubState) -> Result<(), String> {
    let walk = walk_hub_state(state);
    ALLOCATED.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Release);
    let _clone = state.clone();
    RECORDING.store(false, Ordering::Release);
    let counted = ALLOCATED.load(Ordering::Relaxed);
    println!(
        "oracle {name}: counted {counted} walk.clone_heap {}",
        walk.clone_heap
    );
    if counted > walk.clone_heap {
        Err(format!(
            "{name}: counted {counted} > walk.clone_heap {}",
            walk.clone_heap
        ))
    } else {
        Ok(())
    }
}

fn nested_schema(depth: u8) -> serde_json::Value {
    if depth == 0 {
        serde_json::json!({ "type": "string" })
    } else {
        serde_json::json!({
            "type": "object",
            "properties": { "p": nested_schema(depth - 1) }
        })
    }
}

fn main() -> ExitCode {
    let config = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(
            std::path::PathBuf::from("/private/tmp/hub-state-heap-oracle"),
        ),
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("config");
    let mut empty = HubState::from_config(&config);
    let mut errors = Vec::new();
    if let Err(error) = measure("empty", &empty) {
        errors.push(error);
    }

    empty.spawn_targets.push(botster_hub::spawn_targets::SpawnTarget {
        target_id: "t1".into(),
        label: "t".into(),
        root: std::path::PathBuf::from("/tmp/t"),
        enabled: true,
        kind: "git".into(),
        base_ref: Some("main".into()),
        metadata: [("k".into(), "v".into())].into(),
    });
    empty.worktrees.push(botster_hub::worktrees::Worktree {
        worktree_id: "w1".into(),
        target_id: "t1".into(),
        label: "w".into(),
        path: std::path::PathBuf::from("/tmp/w"),
        status: "present".into(),
        management: "hub_managed_git".into(),
        git: None,
        metadata: [("k".into(), "v".into())].into(),
    });
    if let Err(error) = measure("metadata-maps", &empty) {
        errors.push(error);
    }

    let mut schema_state = HubState::from_config(&config);
    let schema_record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
        "manifest": {
            "name": "oracle.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": [],
            "events": { "emitted": [{
                "name": "e",
                "payload_schema": nested_schema(8),
                "audience": ["plugins"]
            }]}
        },
        "state": "enabled",
        "classification": "plugin",
        "trust": { "classification": "first_party", "first_party": true },
        "provenance": { "source": "oracle", "checksum": null },
        "update_policy": "manual",
        "last_audit_reason": "oracle"
    }))
    .expect("schema record");
    schema_state.package_registry.records.push(schema_record);
    if let Err(error) = measure("schema-heavy", &schema_state) {
        errors.push(error);
    }

    let skipped_record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
        "manifest": {
            "name": "skip.plugin",
            "version": "0.0.1",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": []
        },
        "state": "disabled",
        "classification": "plugin",
        "trust": { "classification": "third_party", "first_party": false },
        "provenance": { "source": "s", "checksum": null },
        "update_policy": "manual",
        "last_audit_reason": "skip"
    }))
    .expect("skipped record");
    let mut skipped = HubState::from_config(&config);
    skipped.package_registry.records.push(skipped_record);
    if let Err(error) = measure("skipped-options", &skipped) {
        errors.push(error);
    }

    let full_record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
        "manifest": {
            "name": "full.plugin",
            "version": "2.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "source": { "type": "path", "path": "/tmp/full" },
            "capabilities": [{"surface": "session_actions", "scope": "session_type_spawn"}],
            "entrypoints": [{"runtime": "lua", "path": "plugin.lua", "bootstrap": true}],
            "dependencies": [{"id": "d1", "package": "other", "kind": "required", "requirements": []}],
            "features": [{"id": "f1", "label": "F", "dependencies": ["d1"], "requirements": []}],
            "runnable_entrypoints": [{
                "id": "app",
                "kind": "web_app",
                "launch_mode": "background",
                "command": "bin/app",
                "args": ["--port"],
                "working_directory": { "policy": "relative", "path": "run" },
                "injections": [{"kind": "data_dir", "target": {"type": "environment", "name": "DATA"}, "required": true, "description": "data"}],
                "environment": [{"name": "FOO", "required": false, "default": "bar", "description": "foo"}],
                "readiness": { "result_fields": ["local_url"] }
            }],
            "surfaces": [{"id": "main", "kind": "app", "title": "Main", "description": "d", "icon": "i", "supports": ["render"]}],
            "navigation": [{"id": "n1", "label": "Go", "target": {"kind": "surface", "surface_id": "main"}}],
            "events": {
                "emitted": [{"name": "e", "payload_schema": {"type": "object"}, "audience": ["clients"]}],
                "notices": [{"name": "e", "subject_scope": "session", "text_pointer": "/text", "ttl_ms": 1000, "severity": "info"}]
            }
        },
        "state": "enabled",
        "classification": "plugin",
        "trust": { "classification": "first_party", "first_party": true },
        "provenance": { "source": "/tmp/full", "checksum": "abc" },
        "source_metadata": {
            "registry_id": "r",
            "registry_kind": "local_path",
            "entry_id": "e",
            "source_kind": "local_path",
            "source_label": "lab"
        },
        "pin": { "revision": "1", "update_policy": "manual" },
        "update_policy": "manual",
        "installed_at": "1",
        "updated_at": "2",
        "last_audit_reason": "full",
        "session_types": [{
            "id": "agent",
            "label": "Agent",
            "role": "botster.agent",
            "interaction": "interactive",
            "lifecycle": "task",
            "command": "bin/agent"
        }],
        "runnable_entrypoints": [{
            "id": "app",
            "kind": "web_app",
            "launch_mode": "background",
            "command": "bin/app",
            "args": ["--port"],
            "working_directory": { "policy": "relative", "path": "run" },
            "injections": [],
            "environment": [{"name": "FOO", "required": false, "default": "bar"}],
            "capabilities": [],
            "may_supervise": false
        }],
        "configuration": { "values": { "k": { "type": "string", "value": "v" } } }
    }))
    .expect("full record");
    let mut full = HubState::from_config(&config);
    full.package_registry.records.push(full_record);
    if let Err(error) = measure("fully-populated", &full) {
        errors.push(error);
    }

    if errors.is_empty() {
        ExitCode::SUCCESS
    } else {
        for error in errors {
            eprintln!("{error}");
        }
        ExitCode::from(1)
    }
}
