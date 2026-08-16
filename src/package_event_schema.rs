//! Bounded package-event payload schema admission.
//!
//! This module compiles a closed JSON Schema subset during package admission.
//! It does not fetch remote documents and does not accept `$ref`.

use serde_json::{Map, Value};

const SCHEMA_MAX_BYTES: usize = 8 * 1024;
const SCHEMA_MAX_DEPTH: usize = 8;
const SCHEMA_MAX_PROPERTIES: usize = 32;
const SCHEMA_MAX_ENUM: usize = 32;
const SCHEMA_MAX_REQUIRED: usize = 32;

const ALLOWED_KEYWORDS: &[&str] = &[
    "type",
    "properties",
    "required",
    "additionalProperties",
    "enum",
    "const",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "minItems",
    "maxItems",
    "items",
    "description",
    "title",
];

const REJECTED_KEYWORDS: &[&str] = &[
    "$ref",
    "$dynamicRef",
    "$recursiveRef",
    "$anchor",
    "$id",
    "$schema",
    "pattern",
    "patternProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "prefixItems",
    "contains",
    "propertyNames",
];

/// Compiled bounded payload schema.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledEventSchema {
    spec: Value,
}

impl CompiledEventSchema {
    /// Compile a closed schema subset. Rejects oversize, deep, or expanded schemas.
    pub fn compile(schema: &Value) -> Result<Self, String> {
        let encoded = serde_json::to_vec(schema).map_err(|error| error.to_string())?;
        if encoded.len() > SCHEMA_MAX_BYTES {
            return Err(format!(
                "payload schema exceeds {SCHEMA_MAX_BYTES} byte admission limit"
            ));
        }
        validate_schema_node(schema, 0)?;
        Ok(Self {
            spec: schema.clone(),
        })
    }

    /// Validate one payload instance against the compiled subset.
    pub fn validate(&self, instance: &Value) -> Result<(), String> {
        validate_instance(&self.spec, instance)
    }

    #[must_use]
    pub fn spec(&self) -> &Value {
        &self.spec
    }
}

/// Built-in worktree lifecycle payload schema. Matches the sanitized host event.
#[must_use]
pub fn worktree_lifecycle_schema() -> CompiledEventSchema {
    let spec = serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "event": { "type": "string" },
            "worktree_id": { "type": "string" },
            "target_id": { "type": "string" },
            "status": { "type": "string" },
            "label": { "type": "string" },
            "display_path": { "type": "string" },
            "failure_kind": { "type": "string" },
            "message": { "type": "string" }
        },
        "required": ["event"]
    });
    CompiledEventSchema::compile(&spec).expect("built-in worktree schema is in-subset")
}

fn validate_schema_node(schema: &Value, depth: usize) -> Result<(), String> {
    if depth > SCHEMA_MAX_DEPTH {
        return Err(format!(
            "payload schema exceeds nesting depth {SCHEMA_MAX_DEPTH}"
        ));
    }
    let Value::Object(object) = schema else {
        return Err("payload schema must be an object".to_string());
    };
    for key in object.keys() {
        if REJECTED_KEYWORDS.contains(&key.as_str()) {
            return Err(format!("payload schema rejects keyword {key}"));
        }
        if !ALLOWED_KEYWORDS.contains(&key.as_str()) {
            return Err(format!("payload schema rejects keyword {key}"));
        }
    }
    if let Some(properties) = object.get("properties") {
        let Value::Object(properties) = properties else {
            return Err("properties must be an object".to_string());
        };
        if properties.len() > SCHEMA_MAX_PROPERTIES {
            return Err(format!(
                "payload schema exceeds {SCHEMA_MAX_PROPERTIES} properties"
            ));
        }
        for child in properties.values() {
            validate_schema_node(child, depth + 1)?;
        }
    }
    if let Some(additional) = object.get("additionalProperties") {
        match additional {
            Value::Bool(_) => {}
            other => validate_schema_node(other, depth + 1)?,
        }
    }
    if let Some(items) = object.get("items") {
        if items.is_array() {
            return Err("items must be a single in-subset schema".to_string());
        }
        validate_schema_node(items, depth + 1)?;
    }
    if let Some(required) = object.get("required") {
        let Value::Array(required) = required else {
            return Err("required must be an array".to_string());
        };
        if required.len() > SCHEMA_MAX_REQUIRED {
            return Err(format!(
                "payload schema exceeds {SCHEMA_MAX_REQUIRED} required names"
            ));
        }
        if !required.iter().all(Value::is_string) {
            return Err("required names must be strings".to_string());
        }
    }
    if let Some(enum_values) = object.get("enum") {
        let Value::Array(enum_values) = enum_values else {
            return Err("enum must be an array".to_string());
        };
        if enum_values.len() > SCHEMA_MAX_ENUM {
            return Err(format!(
                "payload schema exceeds {SCHEMA_MAX_ENUM} enum values"
            ));
        }
    }
    Ok(())
}

fn validate_instance(schema: &Value, instance: &Value) -> Result<(), String> {
    let Value::Object(schema) = schema else {
        return Err("compiled schema must be an object".to_string());
    };
    if let Some(const_value) = schema.get("const")
        && instance != const_value
    {
        return Err("payload does not match const".to_string());
    }
    if let Some(Value::Array(enum_values)) = schema.get("enum")
        && !enum_values.iter().any(|value| value == instance)
    {
        return Err("payload is not an allowed enum value".to_string());
    }
    if let Some(type_value) = schema.get("type") {
        match_type(type_value, instance)?;
    }
    match instance {
        Value::Object(object) => validate_object(schema, object)?,
        Value::Array(items) => validate_array(schema, items)?,
        Value::String(text) => validate_string(schema, text)?,
        Value::Number(number) => validate_number(schema, number)?,
        _ => {}
    }
    Ok(())
}

fn match_type(type_value: &Value, instance: &Value) -> Result<(), String> {
    let expected = type_value
        .as_str()
        .ok_or_else(|| "type must be a string".to_string())?;
    let matches = match expected {
        "object" => instance.is_object(),
        "array" => instance.is_array(),
        "string" => instance.is_string(),
        "number" => instance.is_number(),
        "integer" => instance.as_i64().is_some() || instance.as_u64().is_some(),
        "boolean" => instance.is_boolean(),
        "null" => instance.is_null(),
        other => return Err(format!("unsupported schema type {other}")),
    };
    if matches {
        Ok(())
    } else {
        Err(format!("payload is not a {expected}"))
    }
}

fn validate_object(schema: &Map<String, Value>, object: &Map<String, Value>) -> Result<(), String> {
    if let Some(Value::Array(required)) = schema.get("required") {
        for name in required {
            let Some(name) = name.as_str() else {
                continue;
            };
            if !object.contains_key(name) {
                return Err(format!("payload is missing required field {name}"));
            }
        }
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    for (key, value) in object {
        if let Some(property_schema) = properties.and_then(|properties| properties.get(key)) {
            validate_instance(property_schema, value)?;
            continue;
        }
        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => {
                return Err(format!("payload has undeclared field {key}"));
            }
            Some(additional) if additional.is_object() => {
                validate_instance(additional, value)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_array(schema: &Map<String, Value>, items: &[Value]) -> Result<(), String> {
    if let Some(min_items) = schema.get("minItems").and_then(Value::as_u64)
        && (items.len() as u64) < min_items
    {
        return Err("payload array is shorter than minItems".to_string());
    }
    if let Some(max_items) = schema.get("maxItems").and_then(Value::as_u64)
        && (items.len() as u64) > max_items
    {
        return Err("payload array is longer than maxItems".to_string());
    }
    if let Some(item_schema) = schema.get("items") {
        for item in items {
            validate_instance(item_schema, item)?;
        }
    }
    Ok(())
}

fn validate_string(schema: &Map<String, Value>, text: &str) -> Result<(), String> {
    if let Some(min_length) = schema.get("minLength").and_then(Value::as_u64)
        && (text.chars().count() as u64) < min_length
    {
        return Err("payload string is shorter than minLength".to_string());
    }
    if let Some(max_length) = schema.get("maxLength").and_then(Value::as_u64)
        && (text.chars().count() as u64) > max_length
    {
        return Err("payload string is longer than maxLength".to_string());
    }
    Ok(())
}

fn validate_number(schema: &Map<String, Value>, number: &serde_json::Number) -> Result<(), String> {
    let value = number
        .as_f64()
        .ok_or_else(|| "payload number is not finite".to_string())?;
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
        && value < minimum
    {
        return Err("payload is below minimum".to_string());
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
        && value > maximum
    {
        return Err("payload is above maximum".to_string());
    }
    if let Some(exclusive_minimum) = schema.get("exclusiveMinimum").and_then(Value::as_f64)
        && value <= exclusive_minimum
    {
        return Err("payload is not above exclusiveMinimum".to_string());
    }
    if let Some(exclusive_maximum) = schema.get("exclusiveMaximum").and_then(Value::as_f64)
        && value >= exclusive_maximum
    {
        return Err("payload is not below exclusiveMaximum".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversize_ref_and_unsupported_keywords() {
        let huge = Value::Object(
            (0..400)
                .map(|index| (format!("k{index}"), Value::String("x".repeat(20))))
                .collect(),
        );
        assert!(CompiledEventSchema::compile(&huge).is_err());
        assert!(CompiledEventSchema::compile(&serde_json::json!({ "$ref": "#/defs/x" })).is_err());
        assert!(
            CompiledEventSchema::compile(&serde_json::json!({ "allOf": [{ "type": "object" }] }))
                .is_err()
        );
        assert!(CompiledEventSchema::compile(&serde_json::json!({ "pattern": "^a+$" })).is_err());
        let mut nested = serde_json::json!({ "type": "object" });
        for _ in 0..10 {
            nested = serde_json::json!({
                "type": "object",
                "properties": { "child": nested }
            });
        }
        assert!(CompiledEventSchema::compile(&nested).is_err());
    }

    #[test]
    fn accepts_worktree_payload_and_rejects_extra_fields() {
        let schema = worktree_lifecycle_schema();
        schema
            .validate(&serde_json::json!({
                "event": "worktree_created",
                "worktree_id": "wt_1"
            }))
            .expect("valid worktree payload");
        assert!(
            schema
                .validate(&serde_json::json!({
                    "event": "worktree_created",
                    "absolute_path": "/tmp/secret"
                }))
                .is_err()
        );
    }
}
