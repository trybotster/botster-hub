use std::collections::{BTreeMap, BTreeSet};

use botster_ui_contract::{
    PackageNavigationEntry, PackageSurfaceDescriptor, PackageSurfaceKind, PackageSurfaceOperation,
    UiActionKind, UiActionRequest, UiActionResult, UiActionResultState, UiBindIf,
    UiCapabilityFallback, UiColorToken, UiDensity, UiDialogPresentation, UiFieldKind,
    UiHeightClass, UiIframePermission, UiIframeSandboxToken, UiMetricTrendDirection, UiNode,
    UiNodeKind, UiOrientation, UiPointer, UiSelectionMode, UiSpaceToken, UiTableColumnAlign,
    UiToolbarOverflow, UiVariant, UiWidthClass, conformance_fixtures_json, json_schema,
    realize_bind_list_descendant_id, typescript_declarations,
};
use serde::Serialize;
use serde_json::{Value, json};

#[test]
fn generated_assets_match_checked_in_package() {
    assert_eq!(
        typescript_declarations(),
        include_str!("../../../packages/ui-contract/index.d.ts")
    );
    assert_eq!(
        format!(
            "{}\n",
            serde_json::to_string_pretty(&json_schema()).expect("serialize schema")
        ),
        include_str!("../../../packages/ui-contract/schema.json")
    );
    assert_eq!(
        format!(
            "{}\n",
            serde_json::to_string_pretty(&conformance_fixtures_json()).expect("serialize fixtures")
        ),
        include_str!("../../../packages/ui-contract/conformance-fixtures.json")
    );
}

#[test]
fn generated_fixtures_deserialize_and_validate_through_rust_authority() {
    let conformance = conformance_fixtures_json();
    let fixtures = conformance["fixtures"].clone();

    for vector in conformance["bind_list_descendant_identity_vectors"]
        .as_array()
        .expect("identity vectors")
    {
        assert_eq!(
            realize_bind_list_descendant_id(
                vector["row"].as_str().expect("row"),
                vector["key"].as_str().expect("key"),
            )
            .expect("realize identity")
            .0,
            vector["realized_id"].as_str().expect("realized id")
        );
    }

    let surfaces: Vec<PackageSurfaceDescriptor> =
        serde_json::from_value(fixtures["package_presentation"]["surfaces"].clone())
            .expect("package surfaces");
    let navigation: Vec<PackageNavigationEntry> =
        serde_json::from_value(fixtures["package_presentation"]["navigation"].clone())
            .expect("package navigation");
    botster_ui_contract::validate_package_presentation(&surfaces, &navigation)
        .expect("package presentation fixture validates");

    let dialog: UiBindIf =
        serde_json::from_value(fixtures["dialog_presence"].clone()).expect("dialog binding");
    let equality: UiBindIf =
        serde_json::from_value(fixtures["selected_workspace_equality"].clone())
            .expect("equality binding");
    let form: UiNode = serde_json::from_value(fixtures["form"].clone()).expect("form node");
    let bound_row_identity: UiNode = serde_json::from_value(fixtures["bound_row_identity"].clone())
        .expect("bound row identity node");
    let authored_required: Vec<UiNode> =
        serde_json::from_value(fixtures["required_bindable_fields"]["authored"].clone())
            .expect("authored required-bindable nodes");
    let realized_required: Vec<UiNode> =
        serde_json::from_value(fixtures["required_bindable_fields"]["realized"].clone())
            .expect("realized required-bindable nodes");
    let request: UiActionRequest =
        serde_json::from_value(fixtures["request"].clone()).expect("action request");
    let accepted: UiActionResult =
        serde_json::from_value(fixtures["accepted"].clone()).expect("accepted result");
    let rejected: UiActionResult =
        serde_json::from_value(fixtures["rejected"].clone()).expect("rejected result");

    for binding in [dialog, equality] {
        let value = serde_json::to_value(binding).expect("serialize binding");
        assert!(value.get("$kind").is_some());
    }
    form.validate().expect("form fixture validates");
    bound_row_identity
        .validate()
        .expect("bound row identity fixture validates");
    assert_eq!(authored_required.len(), 7);
    for node in authored_required {
        node.validate_authored()
            .expect("authored required binding fixture validates");
        node.validate_realized()
            .expect_err("authored required binding remains unresolved");
    }
    assert_eq!(realized_required.len(), 7);
    for node in realized_required {
        node.validate_realized()
            .expect("materialized required binding fixture validates");
    }
    accepted.validate().expect("accepted fixture validates");
    rejected.validate().expect("rejected fixture validates");
    assert!(request.values.is_some());
    assert!(request.payload.is_some());
}

#[test]
fn generated_schema_validates_required_binding_instances() {
    let schema = json_schema();
    let validator = jsonschema::validator_for(&schema).expect("compile generated JSON Schema");
    let conformance = conformance_fixtures_json();
    let required = &conformance["fixtures"]["required_bindable_fields"];

    for node in required["authored"].as_array().expect("authored nodes") {
        assert!(
            validator.is_valid(node),
            "generated schema rejected authored fixture: {node}"
        );
    }
    for node in required["realized"].as_array().expect("realized nodes") {
        assert!(
            validator.is_valid(node),
            "generated schema rejected realized fixture: {node}"
        );
    }

    for malformed in [
        json!({ "$bind": "" }),
        json!({ "$bind": "relative" }),
        json!({ "$bind": 42 }),
        json!({ "$bind": "@/value", "fallback": "value" }),
    ] {
        let node = json!({
            "type": "button",
            "props": {
                "label": malformed,
                "action": { "id": "contract.action" }
            }
        });
        assert!(
            !validator.is_valid(&node),
            "generated schema accepted malformed sentinel: {node}"
        );
    }

    for node in [
        json!({
            "type": "select_option",
            "props": { "value": { "$bind": "@/value" }, "label": "Open" }
        }),
        json!({
            "type": "table",
            "props": { "columns": { "$bind": "@/columns" } }
        }),
        json!({
            "type": "form",
            "props": {
                "action": { "$bind": "@/action" },
                "submit_label": "Submit"
            }
        }),
    ] {
        assert!(
            !validator.is_valid(&node),
            "generated schema accepted required non-bindable sentinel: {node}"
        );
    }
}

#[test]
fn typescript_and_schema_encode_wire_names_and_optionality() {
    let typescript = typescript_declarations();
    assert!(
        typescript
            .contains("export type UiAuthoredNodeId = UiNodeId | UiBind | UiBindListDescendantId;")
    );
    assert!(typescript.contains("export declare const packageVersion: string;"));
    assert!(typescript.contains("export declare const schema: JsonObject;"));
    assert!(typescript.contains("export declare const conformanceFixtures: JsonObject;"));
    assert!(typescript.contains(
        "export declare function realizeBindListDescendantId(rowId: string, key: string): UiNodeId;"
    ));
    assert!(typescript.contains("export interface UiNodeBase { id?: UiAuthoredNodeId;"));
    assert!(typescript.contains("export type UiBindableString = string | UiBind;"));
    assert!(typescript.contains("export type UiNonBindableValue ="));
    assert!(typescript.contains("UiRequiredNonBindableProps<\"columns\">"));
    assert!(typescript.contains("submit_label: UiBindableString"));
    assert!(typescript.contains("label: UiBindableString"));
    assert!(typescript.contains("text: UiAuthoredTextValue"));
    assert!(typescript.contains("src: UiBindableString; title: UiBindableString"));
    assert!(typescript.contains("name: UiNonBindableValue; label: UiNonBindableValue"));
    assert!(typescript.contains("value: UiNonBindableValue; label: UiNonBindableValue"));
    let request_fields = interface_fields(&typescript, "UiActionRequest");
    let result_fields = interface_fields(&typescript, "UiActionResult");
    assert_eq!(
        request_fields,
        BTreeMap::from([
            ("action_id", ("UiActionId", false)),
            ("kind", ("UiActionKind", false)),
            ("node_id", ("UiNodeId", true)),
            ("payload", ("JsonValue", true)),
            ("request_id", ("UiActionRequestId", false)),
            ("surface_id", ("UiSurfaceId", false)),
            ("values", ("UiFormValues", true)),
        ])
    );
    assert_eq!(
        result_fields,
        BTreeMap::from([
            ("action_id", ("UiActionId", false)),
            ("error", ("string", true)),
            ("field_errors", ("UiFieldErrors", true)),
            ("form_errors", ("string[]", true)),
            ("node_id", ("UiNodeId", true)),
            ("normalized_values", ("UiFormValues", true)),
            ("payload", ("JsonValue", true)),
            ("presentation", ("UiPresentationOperation[]", true)),
            ("replacement", ("UiNode", true)),
            ("request_id", ("UiActionRequestId", false)),
            ("state", ("UiActionResultState", false)),
            ("surface_id", ("UiSurfaceId", false)),
            ("warnings", ("string[]", true)),
        ])
    );
    let schema = json_schema();
    assert_eq!(
        schema.pointer("/$defs/UiNode/properties/id/$ref"),
        Some(&serde_json::json!("#/$defs/UiAuthoredNodeId"))
    );
    assert_eq!(
        schema.pointer("/$defs/UiActionRequest/properties/node_id/$ref"),
        Some(&serde_json::json!("#/$defs/UiNodeId"))
    );
    assert_eq!(
        schema.pointer("/$defs/UiActionResult/properties/node_id/$ref"),
        Some(&serde_json::json!("#/$defs/UiNodeId"))
    );
    assert_eq!(
        schema
            .pointer("/$defs/UiAuthoredNodeId/oneOf/1/description")
            .and_then(serde_json::Value::as_str),
        Some(
            "Schema validation is necessary but not sufficient: the Rust/Hub validator admits a bound id only on the direct UiBindList.item_template root, where row context exists."
        )
    );
    assert_eq!(
        schema.pointer("/$defs/UiBindableString/oneOf/1/$ref"),
        Some(&serde_json::json!("#/$defs/UiBind"))
    );
    assert_eq!(
        required_prop_schema(&schema, "form", "submit_label").and_then(|value| value.get("$ref")),
        Some(&serde_json::json!("#/$defs/UiBindableString"))
    );
    for kind in ["button", "icon_button", "menu_item"] {
        assert_eq!(
            required_prop_schema(&schema, kind, "label").and_then(|value| value.get("$ref")),
            Some(&serde_json::json!("#/$defs/UiBindableString"))
        );
    }
    assert_eq!(
        required_prop_schema(&schema, "text", "text").and_then(|value| value.get("$ref")),
        Some(&serde_json::json!("#/$defs/UiAuthoredTextValue"))
    );
    for field in ["src", "title"] {
        assert_eq!(
            required_prop_schema(&schema, "iframe", field).and_then(|value| value.get("$ref")),
            Some(&serde_json::json!("#/$defs/UiBindableString"))
        );
    }
    for (kind, field) in [
        ("stack", "direction"),
        ("metric", "label"),
        ("table", "columns"),
        ("text_input", "name"),
        ("select_option", "value"),
        ("terminal_view", "session_id"),
    ] {
        assert_eq!(
            required_prop_schema(&schema, kind, field).and_then(|value| value.get("$ref")),
            Some(&serde_json::json!("#/$defs/UiNonBindableValue"))
        );
    }

    assert_serde_fields_match_typescript::<UiActionRequest>(
        serde_json::json!({
            "request_id": "request",
            "surface_id": "surface",
            "action_id": "action",
            "node_id": "node",
            "kind": "submit",
            "values": { "field": "value" },
            "payload": { "source": "toolbar" }
        }),
        serde_json::json!({
            "request_id": "request",
            "surface_id": "surface",
            "action_id": "action",
            "kind": "submit"
        }),
        &request_fields,
    );
    assert_serde_fields_match_typescript::<UiActionResult>(
        serde_json::json!({
            "request_id": "request",
            "surface_id": "surface",
            "action_id": "action",
            "node_id": "node",
            "state": "accepted",
            "field_errors": { "field": ["message"] },
            "form_errors": ["message"],
            "warnings": ["warning"],
            "normalized_values": { "field": "value" },
            "presentation": [{ "kind": "clear", "key": "dialog" }],
            "replacement": { "type": "text", "props": { "text": "done" } },
            "payload": { "ticket_id": "ticket" },
            "error": "detail"
        }),
        serde_json::json!({
            "request_id": "request",
            "surface_id": "surface",
            "action_id": "action",
            "state": "accepted"
        }),
        &result_fields,
    );

    let schema = json_schema();
    macro_rules! assert_wire_enum {
        ($name:literal, [$($variant:path),+ $(,)?]) => {{
            let expected = serialized_variants([$($variant),+]);
            assert_eq!(string_union(&typescript, $name), expected);
            assert_eq!(schema_enum(&schema, $name), expected);
        }};
    }
    assert_wire_enum!(
        "PackageSurfaceKind",
        [
            PackageSurfaceKind::App,
            PackageSurfaceKind::Settings,
            PackageSurfaceKind::DashboardWidget,
            PackageSurfaceKind::Diagnostics,
        ]
    );
    assert_wire_enum!(
        "PackageSurfaceOperation",
        [
            PackageSurfaceOperation::Render,
            PackageSurfaceOperation::Action,
        ]
    );
    assert_wire_enum!(
        "UiNodeKind",
        [
            UiNodeKind::Stack,
            UiNodeKind::Inline,
            UiNodeKind::Form,
            UiNodeKind::FormSection,
            UiNodeKind::FormField,
            UiNodeKind::Panel,
            UiNodeKind::Metric,
            UiNodeKind::MetricGrid,
            UiNodeKind::Toolbar,
            UiNodeKind::StatusBadge,
            UiNodeKind::Section,
            UiNodeKind::ScrollArea,
            UiNodeKind::Text,
            UiNodeKind::Icon,
            UiNodeKind::Badge,
            UiNodeKind::StatusDot,
            UiNodeKind::EmptyState,
            UiNodeKind::List,
            UiNodeKind::ListItem,
            UiNodeKind::Tree,
            UiNodeKind::TreeItem,
            UiNodeKind::Table,
            UiNodeKind::Button,
            UiNodeKind::IconButton,
            UiNodeKind::Menu,
            UiNodeKind::MenuItem,
            UiNodeKind::Dialog,
            UiNodeKind::TextInput,
            UiNodeKind::Textarea,
            UiNodeKind::Checkbox,
            UiNodeKind::Select,
            UiNodeKind::SelectOption,
            UiNodeKind::TerminalView,
            UiNodeKind::ConnectionCodeView,
            UiNodeKind::Iframe,
            UiNodeKind::Custom,
        ]
    );
    assert_wire_enum!(
        "UiWidthClass",
        [
            UiWidthClass::Compact,
            UiWidthClass::Regular,
            UiWidthClass::Expanded,
        ]
    );
    assert_wire_enum!(
        "UiHeightClass",
        [
            UiHeightClass::Short,
            UiHeightClass::Regular,
            UiHeightClass::Tall,
        ]
    );
    assert_wire_enum!(
        "UiPointer",
        [UiPointer::None, UiPointer::Coarse, UiPointer::Fine]
    );
    assert_wire_enum!(
        "UiOrientation",
        [UiOrientation::Portrait, UiOrientation::Landscape]
    );
    assert_wire_enum!(
        "UiDialogPresentation",
        [
            UiDialogPresentation::Auto,
            UiDialogPresentation::Inline,
            UiDialogPresentation::Overlay,
            UiDialogPresentation::Sheet,
            UiDialogPresentation::Fullscreen,
        ]
    );
    assert_wire_enum!(
        "UiCapabilityFallback",
        [
            UiCapabilityFallback::TableAsList,
            UiCapabilityFallback::DialogInline,
            UiCapabilityFallback::TerminalSelectionDisabled,
            UiCapabilityFallback::ConnectionCodeText,
            UiCapabilityFallback::IframeAsLink,
            UiCapabilityFallback::RichColorMuted,
            UiCapabilityFallback::ContextMenuAsMenu,
            UiCapabilityFallback::ClipboardManual,
            UiCapabilityFallback::HoverPersistentHints,
        ]
    );
    assert_wire_enum!(
        "UiSpaceToken",
        [
            UiSpaceToken::None,
            UiSpaceToken::Xs,
            UiSpaceToken::Sm,
            UiSpaceToken::Md,
            UiSpaceToken::Lg,
            UiSpaceToken::Xl,
        ]
    );
    assert_wire_enum!(
        "UiColorToken",
        [
            UiColorToken::Default,
            UiColorToken::Muted,
            UiColorToken::Accent,
            UiColorToken::Success,
            UiColorToken::Warning,
            UiColorToken::Danger,
        ]
    );
    assert_wire_enum!(
        "UiFieldKind",
        [
            UiFieldKind::Text,
            UiFieldKind::Textarea,
            UiFieldKind::Checkbox,
            UiFieldKind::Select,
        ]
    );
    assert_wire_enum!(
        "UiIframeSandboxToken",
        [
            UiIframeSandboxToken::AllowForms,
            UiIframeSandboxToken::AllowModals,
            UiIframeSandboxToken::AllowPopups,
            UiIframeSandboxToken::AllowSameOrigin,
            UiIframeSandboxToken::AllowScripts,
            UiIframeSandboxToken::AllowDownloads,
        ]
    );
    assert_wire_enum!(
        "UiIframePermission",
        [
            UiIframePermission::Fullscreen,
            UiIframePermission::ClipboardWrite,
            UiIframePermission::Camera,
            UiIframePermission::Microphone,
            UiIframePermission::Geolocation,
            UiIframePermission::Payment,
        ]
    );
    assert_wire_enum!(
        "UiDensity",
        [UiDensity::Compact, UiDensity::Regular, UiDensity::Spacious]
    );
    assert_wire_enum!(
        "UiVariant",
        [UiVariant::Plain, UiVariant::Subtle, UiVariant::Emphasized]
    );
    assert_wire_enum!(
        "UiToolbarOverflow",
        [
            UiToolbarOverflow::Auto,
            UiToolbarOverflow::Never,
            UiToolbarOverflow::Always,
        ]
    );
    assert_wire_enum!(
        "UiMetricTrendDirection",
        [
            UiMetricTrendDirection::Up,
            UiMetricTrendDirection::Down,
            UiMetricTrendDirection::Flat,
        ]
    );
    assert_wire_enum!(
        "UiSelectionMode",
        [
            UiSelectionMode::None,
            UiSelectionMode::Single,
            UiSelectionMode::Multiple,
        ]
    );
    assert_wire_enum!(
        "UiTableColumnAlign",
        [
            UiTableColumnAlign::Start,
            UiTableColumnAlign::Center,
            UiTableColumnAlign::End,
        ]
    );
    assert_wire_enum!(
        "UiActionKind",
        [
            UiActionKind::Submit,
            UiActionKind::Reset,
            UiActionKind::Validate,
            UiActionKind::Cancel,
        ]
    );
    assert_wire_enum!(
        "UiActionResultState",
        [
            UiActionResultState::Accepted,
            UiActionResultState::Rejected,
            UiActionResultState::Deferred,
            UiActionResultState::Error,
        ]
    );
    assert!(!typescript.contains("UiTreeUpdateRef"));
    assert!(!typescript.contains("tree_update"));

    assert_eq!(
        schema["$defs"]["UiActionRequest"]["additionalProperties"],
        false
    );
    assert_eq!(
        schema["$defs"]["UiActionResult"]["additionalProperties"],
        false
    );
    assert_eq!(
        kind_schema_branch(&schema, "form").expect("form schema")["then"]["properties"]["props"]["required"],
        serde_json::json!(["action", "submit_label"])
    );
    assert_eq!(
        kind_schema_branch(&schema, "dialog").expect("dialog schema")["then"]["properties"]["props"]
            ["not"]["required"],
        serde_json::json!(["open"])
    );
}

fn required_prop_schema<'a>(schema: &'a Value, kind: &str, prop: &str) -> Option<&'a Value> {
    kind_schema_branch(schema, kind)
        .and_then(|branch| branch["then"]["properties"]["props"]["properties"].get(prop))
}

fn kind_schema_branch<'a>(schema: &'a Value, kind: &str) -> Option<&'a Value> {
    schema["$defs"]["UiNode"]["allOf"]
        .as_array()?
        .iter()
        .find(|branch| {
            let kind_schema = &branch["if"]["properties"]["type"];
            kind_schema["const"] == kind
                || kind_schema["enum"]
                    .as_array()
                    .is_some_and(|kinds| kinds.iter().any(|candidate| candidate == kind))
        })
}

fn interface_fields<'a>(typescript: &'a str, name: &str) -> BTreeMap<&'a str, (&'a str, bool)> {
    let prefix = format!("export interface {name} {{");
    let declaration = typescript
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("missing TypeScript interface {name}"));
    declaration
        .trim_start_matches(&prefix)
        .trim_end_matches('}')
        .split(';')
        .filter_map(|field| {
            let (name, value_type) = field.trim().split_once(':')?;
            let optional = name.ends_with('?');
            Some((name.trim_end_matches('?'), (value_type.trim(), optional)))
        })
        .collect()
}

fn assert_serde_fields_match_typescript<T>(
    full: serde_json::Value,
    minimal: serde_json::Value,
    typescript: &BTreeMap<&str, (&str, bool)>,
) where
    T: Serialize + serde::de::DeserializeOwned,
{
    let serialize_keys = |value| {
        let typed: T = serde_json::from_value(value).expect("fixture must deserialize");
        serde_json::to_value(typed)
            .expect("fixture must serialize")
            .as_object()
            .expect("contract DTO must serialize as an object")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
    };
    let all_typescript = typescript
        .keys()
        .map(|field| (*field).to_string())
        .collect::<BTreeSet<_>>();
    let required_typescript = typescript
        .iter()
        .filter(|(_, (_, optional))| !optional)
        .map(|(field, _)| (*field).to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(serialize_keys(full), all_typescript);
    assert_eq!(serialize_keys(minimal), required_typescript);
}

fn string_union(typescript: &str, name: &str) -> BTreeSet<String> {
    let prefix = format!("export type {name} = ");
    typescript
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("missing TypeScript union {name}"))
        .trim_start_matches(&prefix)
        .trim_end_matches(';')
        .split('|')
        .map(|variant| variant.trim().trim_matches('"').to_string())
        .collect()
}

fn schema_enum(schema: &serde_json::Value, name: &str) -> BTreeSet<String> {
    schema["$defs"][name]["enum"]
        .as_array()
        .unwrap_or_else(|| panic!("missing schema enum {name}"))
        .iter()
        .map(|variant| {
            variant
                .as_str()
                .unwrap_or_else(|| panic!("schema enum {name} contains a non-string"))
                .to_string()
        })
        .collect()
}

fn serialized_variants<T: Serialize, const N: usize>(variants: [T; N]) -> BTreeSet<String> {
    variants
        .into_iter()
        .map(|variant| {
            serde_json::to_value(variant)
                .expect("enum variant must serialize")
                .as_str()
                .expect("enum variant must serialize as a string")
                .to_string()
        })
        .collect()
}
