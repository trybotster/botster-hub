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
    let pretty = serde_json::to_vec_pretty(state).expect("pretty");
    ALLOCATED.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Release);
    let _clone = state.clone();
    RECORDING.store(false, Ordering::Release);
    let clone_counted = ALLOCATED.load(Ordering::Relaxed);
    ALLOCATED.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Release);
    let mut serializer = Vec::with_capacity(pretty.len());
    serde_json::to_writer_pretty(&mut serializer, state).expect("serialize");
    RECORDING.store(false, Ordering::Release);
    let serializer_counted = ALLOCATED.load(Ordering::Relaxed);
    let peak_walk = walk.clone_heap.saturating_add(pretty.len());
    let peak_counted = clone_counted.saturating_add(serializer_counted);
    println!(
        "g1 {name}: logical_pretty={} clone_walk={} clone_counted={} serializer_cap={} serializer_counted={} serializer_written={} peak_walk={} peak_counted={}",
        pretty.len(),
        walk.clone_heap,
        clone_counted,
        pretty.len(),
        serializer_counted,
        serializer.len(),
        peak_walk,
        peak_counted,
    );
    if clone_counted > walk.clone_heap {
        Err(format!(
            "{name}: counted {clone_counted} > walk.clone_heap {}",
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

    let mut wide_properties = serde_json::Map::new();
    for index in 0..32 {
        wide_properties.insert(format!("p{index}"), nested_schema(8));
    }
    let wide_schema = serde_json::json!({
        "type": "object",
        "properties": wide_properties
    });
    let wide_record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
        "manifest": {
            "name": "wide.plugin",
            "version": "1.0.0",
            "kind": "plugin",
            "botster": ">=0.1.0",
            "capabilities": [],
            "entrypoints": [],
            "events": { "emitted": [{
                "name": "wide",
                "payload_schema": wide_schema,
                "audience": ["plugins"]
            }]}
        },
        "state": "enabled",
        "classification": "plugin",
        "trust": { "classification": "first_party", "first_party": true },
        "provenance": { "source": "oracle", "checksum": null },
        "update_policy": "manual",
        "last_audit_reason": "wide"
    }))
    .expect("wide record");
    let mut wide_state = HubState::from_config(&config);
    wide_state.package_registry.records.push(wide_record);
    if let Err(error) = measure("wide-properties", &wide_state) {
        errors.push(error);
    }

    let mut leaf_properties = serde_json::Map::new();
    for name in "abcdefghijklmnopqrstuvwxyz012345".chars() {
        leaf_properties.insert(
            name.to_string(),
            serde_json::json!({ "type": "string" }),
        );
    }
    let shallow_wide = serde_json::json!({
        "type": "object",
        "properties": leaf_properties
    });
    let compact = serde_json::to_vec(&shallow_wide).expect("compact schema");
    assert!(
        compact.len() <= 8 * 1024,
        "shallow-wide schema {} exceeds 8 KiB compact",
        compact.len()
    );
    let mut emitted = Vec::new();
    for event_index in 0..8 {
        emitted.push(serde_json::json!({
            "name": format!("e{event_index}"),
            "payload_schema": shallow_wide,
            "audience": ["plugins"]
        }));
    }
    let mut shallow_state = HubState::from_config(&config);
    for record_index in 0..4 {
        let record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
            "manifest": {
                "name": format!("shallow{record_index}.plugin"),
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "capabilities": [],
                "entrypoints": [],
                "events": { "emitted": emitted }
            },
            "state": "enabled",
            "classification": "plugin",
            "trust": { "classification": "first_party", "first_party": true },
            "provenance": { "source": "oracle", "checksum": null },
            "update_policy": "manual",
            "last_audit_reason": "shallow"
        }))
        .expect("shallow record");
        shallow_state.package_registry.records.push(record);
    }
    if let Err(error) = measure("shallow-wide-32", &shallow_state) {
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
