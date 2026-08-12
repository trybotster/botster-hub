//! Entity-backed `ui.select` options: descriptor validation, family collection,
//! pure projection, and shared frame-timeline helpers for conformance fixtures.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{UiChild, UiNode, UiNodeKind, UiValidationError};

/// Authored descriptor for reactive entity-backed select options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiEntityOptionsSource {
    /// Discriminator for the entity-options producer.
    #[serde(rename = "$kind")]
    pub kind: UiEntityOptionsKind,
    /// Absolute entity family path (leading `/`).
    pub source: String,
    /// Exact top-level field name used as the option value (JSON string only).
    pub value_field: String,
    /// Ordered display fields; first present string becomes the option label.
    pub display_fields: Vec<String>,
    /// Deterministic sort keys; missing/non-string ranks after strings.
    pub order: Vec<String>,
    /// Optional exact top-level equality filters over source records.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub r#where: BTreeMap<String, Value>,
    /// Optional exclusion set drawn from another admitted family.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude: Option<UiEntityOptionsExclude>,
}

/// Wire kind token for entity-backed select options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UiEntityOptionsKind {
    /// Entity-backed options producer.
    #[serde(rename = "entity_options")]
    EntityOptions,
}

/// Exclusion descriptor: values drawn from another entity family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiEntityOptionsExclude {
    /// Absolute entity family path (leading `/`).
    pub source: String,
    /// Exact top-level field name whose string values are excluded.
    pub value_field: String,
    /// Optional exact top-level equality filters over exclusion records.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub r#where: BTreeMap<String, Value>,
}

/// One projected select option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityOption {
    /// Exact option value (JSON string contents).
    pub value: String,
    /// Display label (may be empty).
    pub label: String,
    /// String display fields copied from the source record.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// Pure projection result for entity-backed select options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityOptionsProjection {
    /// Ordered unique options after exclusion and duplicate collapse.
    pub options: Vec<EntityOption>,
    /// Whether the provided selection string is present among options.
    pub selection_valid: bool,
}

/// Canonical entity frame vocabulary for shared conformance timelines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EntityOptionsFrame {
    /// Authoritative whole-family replacement.
    Snapshot {
        entity_type: String,
        snapshot_seq: u64,
        items: Vec<EntityRecordItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resync_reason: Option<String>,
    },
    /// Insert or replace one record by id.
    Upsert {
        entity_type: String,
        id: String,
        fields: Map<String, Value>,
        seq: u64,
    },
    /// Merge fields into one record by id.
    Patch {
        entity_type: String,
        id: String,
        fields: Map<String, Value>,
        seq: u64,
    },
    /// Drop one record by id.
    Remove {
        entity_type: String,
        id: String,
        seq: u64,
    },
}

/// Snapshot item with explicit id and fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityRecordItem {
    pub id: String,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

/// Family store: entity_type → id → fields.
pub type EntityFamilyStore = BTreeMap<String, BTreeMap<String, Map<String, Value>>>;

/// Validate an authored entity-options descriptor.
pub fn validate_entity_options_source(
    kind: UiNodeKind,
    prop: &str,
    descriptor: &UiEntityOptionsSource,
) -> Result<(), UiValidationError> {
    if descriptor.kind != UiEntityOptionsKind::EntityOptions {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "options_source $kind must be entity_options".to_string(),
        });
    }
    validate_absolute_family_path(kind, prop, &descriptor.source, "source")?;
    validate_nonempty_field_name(kind, prop, &descriptor.value_field, "value_field")?;
    if descriptor.display_fields.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "display_fields must be non-empty".to_string(),
        });
    }
    for field in &descriptor.display_fields {
        validate_nonempty_field_name(kind, prop, field, "display_fields entry")?;
    }
    if descriptor.order.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "order must be non-empty".to_string(),
        });
    }
    for field in &descriptor.order {
        validate_nonempty_field_name(kind, prop, field, "order entry")?;
    }
    validate_exact_where(kind, prop, &descriptor.r#where)?;
    if let Some(exclude) = &descriptor.exclude {
        validate_absolute_family_path(kind, prop, &exclude.source, "exclude.source")?;
        validate_nonempty_field_name(kind, prop, &exclude.value_field, "exclude.value_field")?;
        validate_exact_where(kind, prop, &exclude.r#where)?;
    }
    Ok(())
}

fn validate_absolute_family_path(
    kind: UiNodeKind,
    prop: &str,
    path: &str,
    field: &str,
) -> Result<(), UiValidationError> {
    if path.is_empty() || !path.starts_with('/') || path == "/" {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: format!("{field} must be an absolute entity family path"),
        });
    }
    // Family sources are single-segment absolute paths: `/session` or
    // `/project-pipelines.run`. Nested field paths (`/session/id/field`) are
    // not valid family sources for entity options.
    let rest = &path[1..];
    if rest.is_empty() || rest.contains('/') {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: format!(
                "{field} must be an absolute entity family path without nested segments"
            ),
        });
    }
    Ok(())
}

fn validate_nonempty_field_name(
    kind: UiNodeKind,
    prop: &str,
    name: &str,
    field: &str,
) -> Result<(), UiValidationError> {
    if name.trim().is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: format!("{field} cannot be empty"),
        });
    }
    if name.contains('/') || name.contains('.') {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: format!("{field} must be an exact top-level field name"),
        });
    }
    Ok(())
}

fn validate_exact_where(
    kind: UiNodeKind,
    prop: &str,
    r#where: &BTreeMap<String, Value>,
) -> Result<(), UiValidationError> {
    for (key, value) in r#where {
        if key.trim().is_empty() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "where field cannot be empty".to_string(),
            });
        }
        if key.contains('/') || key.contains('.') {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "where filters exact top-level fields only".to_string(),
            });
        }
        // Smallest exact dual-runtime domain: JSON strings only (UTF-8 byte
        // equality). Objects/numbers/bools would diverge under JSON.stringify
        // vs serde_json::Value identity.
        if !value.is_string() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "where values must be JSON strings".to_string(),
            });
        }
    }
    Ok(())
}

/// Strip the leading `/` from an authored absolute family path for SubscribeEntities.
#[must_use]
pub fn entity_family_subscription_id(authored_path: &str) -> Option<String> {
    authored_path
        .strip_prefix('/')
        .filter(|rest| !rest.is_empty() && !rest.contains('/'))
        .map(str::to_string)
}

/// Walk a UiNode tree and collect distinct SubscribeEntities family ids
/// referenced by entity-options producers (source + exclude), slash-stripped.
#[must_use]
pub fn collect_entity_option_families(node: &UiNode) -> Vec<String> {
    let mut families = BTreeSet::new();
    collect_entity_option_families_node(node, &mut families);
    families.into_iter().collect()
}

fn collect_entity_option_families_node(node: &UiNode, families: &mut BTreeSet<String>) {
    if node.kind == UiNodeKind::Select
        && let Some(value) = node.props.get("options_source")
        && let Ok(descriptor) = serde_json::from_value::<UiEntityOptionsSource>(value.clone())
    {
        if let Some(id) = entity_family_subscription_id(&descriptor.source) {
            families.insert(id);
        }
        if let Some(exclude) = &descriptor.exclude
            && let Some(id) = entity_family_subscription_id(&exclude.source)
        {
            families.insert(id);
        }
    }
    for child in &node.children {
        collect_entity_option_families_child(child, families);
    }
    for children in node.slots.values() {
        for child in children {
            collect_entity_option_families_child(child, families);
        }
    }
}

fn collect_entity_option_families_child(child: &UiChild, families: &mut BTreeSet<String>) {
    match child {
        UiChild::Node(node) => collect_entity_option_families_node(node, families),
        UiChild::Conditional(conditional) => match conditional {
            crate::UiConditional::When { node, .. } | crate::UiConditional::Hidden { node, .. } => {
                collect_entity_option_families_node(node, families)
            }
        },
        UiChild::BindList(bind_list) => match bind_list {
            crate::UiBindList::BindList {
                item_template,
                empty_template,
                ..
            } => {
                collect_entity_option_families_node(item_template, families);
                if let Some(template) = empty_template {
                    collect_entity_option_families_node(template, families);
                }
            }
        },
        UiChild::BindIf(bind_if) => match bind_if {
            crate::UiBindIf::BindIf { node, .. } | crate::UiBindIf::PresentationIf { node, .. } => {
                collect_entity_option_families_node(node, families)
            }
        },
    }
}

/// Project entity-backed select options from whole-family record maps.
///
/// Records are maps of entity id → field object. Callers apply the shared
/// frame timeline (or equivalent store) before invoking this pure projector.
#[must_use]
pub fn project_entity_options(
    descriptor: &UiEntityOptionsSource,
    source_records: &BTreeMap<String, Map<String, Value>>,
    exclude_records: &BTreeMap<String, Map<String, Value>>,
    selection: Option<&str>,
) -> EntityOptionsProjection {
    let excluded = build_exclusion_set(descriptor, exclude_records);
    let mut ranked = Vec::new();
    for (record_id, record) in source_records {
        if !matches_where(record, &descriptor.r#where) {
            continue;
        }
        let Some(value) = string_field(record, &descriptor.value_field) else {
            continue;
        };
        if excluded.contains(&value) {
            continue;
        }
        let mut metadata = BTreeMap::new();
        let mut label = String::new();
        let mut label_set = false;
        for field in &descriptor.display_fields {
            if let Some(text) = string_field(record, field) {
                if !label_set {
                    label = text.clone();
                    label_set = true;
                }
                metadata.insert(field.clone(), text);
            }
        }
        // Order keys may reference fields outside display_fields. Capture
        // present string values for sort ranking; missing / non-string rank last.
        let mut order_keys = Vec::with_capacity(descriptor.order.len());
        for key in &descriptor.order {
            order_keys.push(string_field(record, key));
        }
        ranked.push((
            EntityOption {
                value,
                label,
                metadata,
            },
            order_keys,
            record_id.as_str(),
        ));
    }

    // order keys → option value → record id (UTF-8 bytes). Record id makes
    // first-after-sort independent of map insertion order across runtimes.
    ranked.sort_by(
        |(left, left_keys, left_id), (right, right_keys, right_id)| {
            for (left_key, right_key) in left_keys.iter().zip(right_keys.iter()) {
                let cmp = compare_optional_utf8_strings(left_key.as_deref(), right_key.as_deref());
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            utf8_byte_cmp(&left.value, &right.value).then_with(|| utf8_byte_cmp(left_id, right_id))
        },
    );

    // First-after-sort wins for duplicate values.
    let mut seen = BTreeSet::new();
    let mut options = Vec::with_capacity(ranked.len());
    for (option, _, _) in ranked {
        if seen.insert(option.value.clone()) {
            options.push(option);
        }
    }

    let selection_valid = match selection {
        Some(selected) => options.iter().any(|option| option.value == selected),
        None => true,
    };

    EntityOptionsProjection {
        options,
        selection_valid,
    }
}

fn build_exclusion_set(
    descriptor: &UiEntityOptionsSource,
    exclude_records: &BTreeMap<String, Map<String, Value>>,
) -> BTreeSet<String> {
    let Some(exclude) = &descriptor.exclude else {
        return BTreeSet::new();
    };
    let mut set = BTreeSet::new();
    for record in exclude_records.values() {
        if !matches_where(record, &exclude.r#where) {
            continue;
        }
        if let Some(value) = string_field(record, &exclude.value_field) {
            set.insert(value);
        }
    }
    set
}

fn matches_where(record: &Map<String, Value>, r#where: &BTreeMap<String, Value>) -> bool {
    r#where.iter().all(|(key, expected)| {
        let Some(expected_str) = expected.as_str() else {
            return false;
        };
        record.get(key).and_then(Value::as_str) == Some(expected_str)
    })
}

fn string_field(record: &Map<String, Value>, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn compare_optional_utf8_strings(left: Option<&str>, right: Option<&str>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(a), Some(b)) => utf8_byte_cmp(a, b),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn utf8_byte_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.as_bytes().cmp(right.as_bytes())
}

/// Apply one canonical entity frame to a multi-family store.
pub fn apply_entity_options_frame(store: &mut EntityFamilyStore, frame: &EntityOptionsFrame) {
    match frame {
        EntityOptionsFrame::Snapshot {
            entity_type, items, ..
        } => {
            let mut family = BTreeMap::new();
            for item in items {
                let mut fields = item.fields.clone();
                // Ensure id is present as a field when producers put it only
                // on the envelope; projection reads value_field from fields.
                if !fields.contains_key("id") {
                    fields.insert("id".to_string(), Value::String(item.id.clone()));
                }
                family.insert(item.id.clone(), fields);
            }
            store.insert(entity_type.clone(), family);
        }
        EntityOptionsFrame::Upsert {
            entity_type,
            id,
            fields,
            ..
        } => {
            let family = store.entry(entity_type.clone()).or_default();
            let mut merged = fields.clone();
            if !merged.contains_key("id") {
                merged.insert("id".to_string(), Value::String(id.clone()));
            }
            family.insert(id.clone(), merged);
        }
        EntityOptionsFrame::Patch {
            entity_type,
            id,
            fields,
            ..
        } => {
            let family = store.entry(entity_type.clone()).or_default();
            let entry = family.entry(id.clone()).or_default();
            for (key, value) in fields {
                entry.insert(key.clone(), value.clone());
            }
            if !entry.contains_key("id") {
                entry.insert("id".to_string(), Value::String(id.clone()));
            }
        }
        EntityOptionsFrame::Remove {
            entity_type, id, ..
        } => {
            if let Some(family) = store.get_mut(entity_type) {
                family.remove(id);
            }
        }
    }
}

/// Apply an ordered frame list and return the resulting store.
#[must_use]
pub fn apply_entity_options_frames(frames: &[EntityOptionsFrame]) -> EntityFamilyStore {
    let mut store = EntityFamilyStore::new();
    for frame in frames {
        apply_entity_options_frame(&mut store, frame);
    }
    store
}

/// Project using family maps keyed by subscription id (slash-stripped).
#[must_use]
pub fn project_entity_options_from_store(
    descriptor: &UiEntityOptionsSource,
    store: &EntityFamilyStore,
    selection: Option<&str>,
) -> EntityOptionsProjection {
    let source_key = entity_family_subscription_id(&descriptor.source).unwrap_or_default();
    let empty = BTreeMap::new();
    let source_records = store.get(&source_key).unwrap_or(&empty);
    let exclude_records = descriptor
        .exclude
        .as_ref()
        .and_then(|exclude| entity_family_subscription_id(&exclude.source))
        .and_then(|key| store.get(&key))
        .unwrap_or(&empty);
    project_entity_options(descriptor, source_records, exclude_records, selection)
}
