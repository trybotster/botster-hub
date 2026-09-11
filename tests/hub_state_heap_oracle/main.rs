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
    let mut record: botster_hub::packages::PackageRecord = serde_json::from_value(serde_json::json!({
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
    .expect("package record");
    schema_state.package_registry.records.push(record.clone());
    if let Err(error) = measure("schema-heavy", &schema_state) {
        errors.push(error);
    }

    record.installed_at = None;
    record.updated_at = None;
    record.pin = None;
    record.source_metadata = None;
    let mut skipped = HubState::from_config(&config);
    skipped.package_registry.records.push(record);
    if let Err(error) = measure("skipped-options", &skipped) {
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
