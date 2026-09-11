//! G2 Host-state clone-heap walk.
//!
//! Reports **new** heap for one Prepare clone. Does not select a G1 budget
//! and is not wired into `prepare_shared` admission.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::path::PathBuf;

use serde_json::Value;

use crate::persistence::HubState;

/// 64-bit `InternalNode<String, String>` from the G2 contract (1.92 node.rs).
const BTREE_INTERNAL_STRING_STRING: usize = 640;
/// Conservative internal-node size for `BTreeMap<String, Value>`.
const BTREE_INTERNAL_STRING_VALUE: usize = 640;
const BTREE_MIN_OCCUPANCY: usize = 5;

/// New-heap walk of one `HubState` clone, excluding the retained Arc view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapWalk {
    pub btree_nodes: usize,
    pub string_heaps: usize,
    pub vec_slots: usize,
    pub clone_heap: usize,
}

impl HeapWalk {
    fn add(&mut self, btree: usize, strings: usize, vecs: usize) {
        self.btree_nodes = self.btree_nodes.saturating_add(btree);
        self.string_heaps = self.string_heaps.saturating_add(strings);
        self.vec_slots = self.vec_slots.saturating_add(vecs);
        self.clone_heap = self
            .clone_heap
            .saturating_add(btree)
            .saturating_add(strings)
            .saturating_add(vecs);
    }

    /// `clone_heap + admitted pretty JSON capacity` (serializer buffer, not selected as G1).
    #[must_use]
    pub fn peak_new(self, admitted_pretty: usize) -> usize {
        self.clone_heap.saturating_add(admitted_pretty)
    }
}

fn btree_nodes(len: usize, internal_node: usize) -> usize {
    if len == 0 {
        0
    } else {
        internal_node.saturating_add(len.saturating_mul(internal_node / BTREE_MIN_OCCUPANCY))
    }
}

fn string_heap(value: &str) -> usize {
    value.len()
}

fn path_heap(path: &PathBuf) -> usize {
    path.as_os_str().len()
}

fn vec_slots<T>(len: usize) -> usize {
    len.saturating_mul(size_of::<T>())
}

fn add_btree_string_string(
    walk: &mut HeapWalk,
    map: &BTreeMap<String, String>,
) {
    let mut heaps: usize = 0;
    for (key, value) in map {
        heaps = heaps
            .saturating_add(string_heap(key))
            .saturating_add(string_heap(value));
    }
    walk.add(
        btree_nodes(map.len(), BTREE_INTERNAL_STRING_STRING),
        heaps,
        0,
    );
}

fn add_value(walk: &mut HeapWalk, value: &Value) {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
        Value::String(text) => walk.add(0, string_heap(text), 0),
        Value::Array(items) => {
            walk.add(0, 0, vec_slots::<Value>(items.len()));
            for item in items {
                add_value(walk, item);
            }
        }
        Value::Object(map) => {
            let mut heaps: usize = 0;
            for (key, nested) in map {
                heaps = heaps.saturating_add(string_heap(key));
                add_value(walk, nested);
            }
            walk.add(
                btree_nodes(map.len(), BTREE_INTERNAL_STRING_VALUE),
                heaps,
                0,
            );
        }
    }
}

/// Walk `HubState` collections without allocating a side table of keys.
#[must_use]
pub fn walk_hub_state(state: &HubState) -> HeapWalk {
    let mut walk = HeapWalk {
        btree_nodes: 0,
        string_heaps: 0,
        vec_slots: 0,
        clone_heap: 0,
    };
    walk.add(
        0,
        0,
        vec_slots::<crate::packages::PackageRecord>(state.package_registry.records.len()),
    );
    walk.add(
        0,
        0,
        vec_slots::<crate::spawn_targets::SpawnTarget>(state.spawn_targets.len()),
    );
    walk.add(
        0,
        0,
        vec_slots::<crate::worktrees::Worktree>(state.worktrees.len()),
    );
    walk.add(
        0,
        0,
        vec_slots::<crate::persistence::HubAuditEntry>(state.audit_history.len()),
    );
    for target in &state.spawn_targets {
        walk.add(0, string_heap(&target.target_id).saturating_add(path_heap(&target.root)), 0);
        add_btree_string_string(&mut walk, &target.metadata);
    }
    for worktree in &state.worktrees {
        walk.add(0, string_heap(&worktree.worktree_id).saturating_add(path_heap(&worktree.path)), 0);
    }
    for record in &state.package_registry.records {
        walk.add(0, string_heap(&record.manifest.name), 0);
        for event in &record.manifest.events.emitted {
            add_value(&mut walk, &event.payload_schema);
        }
    }
    walk
}

/// Pretty-JSON logical length. Allocates; not part of the walk itself.
#[must_use]
pub fn admitted_pretty(state: &HubState) -> Result<usize, serde_json::Error> {
    serde_json::to_vec_pretty(state).map(|bytes| bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HubStartupOptions;
    use crate::RuntimeEnvironment;

    #[test]
    fn empty_btree_walk_is_zero_nodes() {
        assert_eq!(btree_nodes(0, BTREE_INTERNAL_STRING_STRING), 0);
        let map = BTreeMap::<String, String>::new();
        let mut walk = HeapWalk {
            btree_nodes: 0,
            string_heaps: 0,
            vec_slots: 0,
            clone_heap: 0,
        };
        add_btree_string_string(&mut walk, &map);
        assert_eq!(walk.btree_nodes, 0);
        assert_eq!(walk.clone_heap, 0);
    }

    #[test]
    fn one_entry_btree_includes_a_root_node() {
        let nodes = btree_nodes(1, BTREE_INTERNAL_STRING_STRING);
        assert!(nodes >= BTREE_INTERNAL_STRING_STRING);
        let mut map = BTreeMap::new();
        map.insert("k".into(), "v".into());
        let mut walk = HeapWalk {
            btree_nodes: 0,
            string_heaps: 0,
            vec_slots: 0,
            clone_heap: 0,
        };
        add_btree_string_string(&mut walk, &map);
        assert_eq!(walk.string_heaps, 2);
        assert_eq!(walk.btree_nodes, nodes);
    }

    #[test]
    fn payload_schema_shape_ratio_is_reported_not_selected() {
        let mut properties = serde_json::Map::new();
        properties.insert(
            "p".into(),
            serde_json::json!({"type": "string"}),
        );
        let schema = Value::Object({
            let mut root = serde_json::Map::new();
            root.insert("type".into(), Value::String("object".into()));
            root.insert("properties".into(), Value::Object(properties));
            root
        });
        let mut walk = HeapWalk {
            btree_nodes: 0,
            string_heaps: 0,
            vec_slots: 0,
            clone_heap: 0,
        };
        add_value(&mut walk, &schema);
        let pretty = serde_json::to_vec_pretty(&schema).unwrap().len();
        assert!(pretty > 0);
        assert!(walk.clone_heap > pretty, "heap {walk:?} pretty {pretty}");
        let ratio = walk.clone_heap as f64 / pretty as f64;
        assert!(
            ratio > 1.0,
            "report-only ratio {ratio} heap={} pretty={pretty}",
            walk.clone_heap
        );
    }

    #[test]
    fn empty_hub_state_walk_does_not_select_a_budget() {
        let config = HubStartupOptions {
            data_directory: crate::config::DataDirectoryOption::Explicit(
                std::path::PathBuf::from("/private/tmp/hub-state-heap-walk"),
            ),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let state = HubState::from_config(&config);
        let walk = walk_hub_state(&state);
        let pretty = admitted_pretty(&state).unwrap();
        let peak = walk.peak_new(pretty);
        assert!(pretty > 0);
        assert!(peak >= pretty);
        assert_ne!(peak, 64 * 1024 * 1024, "must not silently use the logical 64 MiB as G1");
    }
}
