//! UI contract serialization and validation tests.

use std::collections::{BTreeMap, BTreeSet};

use botster_ui_contract::{
    PackageNavigationEntry, PackageNavigationTarget, PackagePresentationValidationError,
    PackageSurfaceDescriptor, PackageSurfaceKind, PackageSurfaceOperation, UiAction,
    UiActionRequestId, UiToolbarOverflow as FlatUiToolbarOverflow,
};
use botster_ui_contract::{
    UiActionId, UiActionKind, UiActionRequest, UiActionResult, UiActionResultState,
    UiActionResultValidationError, UiAuthoredNodeId, UiBind, UiBindIf, UiBindList,
    UiCapabilityFallback, UiCapabilitySet, UiChild, UiCondition, UiConditional, UiDensity,
    UiDialogPresentation, UiFieldErrors, UiFieldKind, UiFieldOption, UiFieldSchema,
    UiFieldValidationHints, UiFormValues, UiHeightClass, UiIframeBridge, UiIframePermission,
    UiIframeSandboxToken, UiKeyboardCapability, UiMetricTrend, UiMetricTrendDirection, UiNode,
    UiNodeId, UiNodeKind, UiPointer, UiPresentationKey, UiPresentationOperation,
    UiPresentationPredicate, UiResponsiveHeight, UiResponsiveValue, UiResponsiveWidth, UiSelection,
    UiSelectionMode, UiSurfaceId, UiTableCell, UiTableColumn, UiTableColumnDescriptor, UiTableRow,
    UiToolbarOverflow, UiValidationError, UiVariant, UiWidthClass, validate_package_presentation,
    validate_ui_node, validate_ui_node_with_capabilities,
};
use serde_json::{Map, Value, json};

fn node(kind: UiNodeKind, props: Value) -> UiNode {
    UiNode {
        kind,
        id: Some(UiNodeId(format!("{kind:?}").to_lowercase()).into()),
        props: props.as_object().cloned().unwrap_or_default(),
        children: Vec::new(),
        slots: BTreeMap::new(),
    }
}

fn text_node(value: &str) -> UiNode {
    node(UiNodeKind::Text, json!({ "text": value }))
}

fn text(value: &str) -> UiChild {
    UiChild::Node(Box::new(text_node(value)))
}

fn custom_node(fallback: UiNode) -> UiNode {
    let mut custom = node(
        UiNodeKind::Custom,
        json!({
            "namespace": "project-pipelines",
            "component": "ticket-card",
            "reason": "first-party package experiment before shared vocabulary promotion"
        }),
    );
    custom.slots.insert(
        "fallback".to_string(),
        vec![UiChild::Node(Box::new(fallback))],
    );
    custom
}

fn valid_standalone_node(kind: UiNodeKind) -> UiNode {
    match kind {
        UiNodeKind::Metric => node(kind, json!({ "label": "Open", "value": 2 })),
        UiNodeKind::Toolbar => node(kind, json!({ "label": "Actions" })),
        UiNodeKind::StatusBadge => node(kind, json!({ "label": "Open" })),
        UiNodeKind::Section => node(kind, json!({ "title": "Section" })),
        UiNodeKind::Panel => node(kind, json!({ "title": "Panel" })),
        UiNodeKind::TerminalView => node(kind, json!({ "session_id": "sess_1" })),
        UiNodeKind::ConnectionCodeView => node(kind, json!({ "code": "pair" })),
        UiNodeKind::ListItem => {
            let mut item = node(kind, json!({ "value": "ticket_1" }));
            item.slots.insert("title".to_string(), vec![text("Ticket")]);
            item
        }
        UiNodeKind::TreeItem => {
            let mut item = node(kind, json!({ "value": "ticket_1" }));
            item.slots.insert("title".to_string(), vec![text("Ticket")]);
            item
        }
        UiNodeKind::MenuItem => node(
            kind,
            json!({ "label": "Open", "action": { "id": "ticket.open" } }),
        ),
        UiNodeKind::SelectOption => node(kind, json!({ "label": "Open", "value": "open" })),
        UiNodeKind::FormSection => node(kind, json!({ "title": "Details" })),
        UiNodeKind::FormField => node(
            kind,
            json!({ "schema": { "kind": "text", "name": "title", "label": "Title" } }),
        ),
        UiNodeKind::Custom => custom_node(node(UiNodeKind::Text, json!({ "text": "Nested" }))),
        _ => node(kind, json!({})),
    }
}

fn idless_node(kind: UiNodeKind, props: Value) -> UiNode {
    UiNode {
        kind,
        id: None,
        props: props.as_object().cloned().unwrap_or_default(),
        children: Vec::new(),
        slots: BTreeMap::new(),
    }
}

fn assert_error_contains(node: UiNode, expected: &str) {
    let message = node
        .validate()
        .expect_err("node should fail validation")
        .to_string();
    assert!(
        message.contains(expected),
        "expected `{message}` to contain `{expected}`"
    );
}

fn rich_capabilities() -> UiCapabilitySet {
    UiCapabilitySet {
        width_classes: BTreeMap::from([
            (UiWidthClass::Compact, ()),
            (UiWidthClass::Regular, ()),
            (UiWidthClass::Expanded, ()),
        ])
        .into_keys()
        .collect(),
        height_classes: BTreeMap::from([
            (UiHeightClass::Short, ()),
            (UiHeightClass::Regular, ()),
            (UiHeightClass::Tall, ()),
        ])
        .into_keys()
        .collect(),
        pointer: UiPointer::Fine,
        keyboard: UiKeyboardCapability {
            text_entry: true,
            shortcuts: true,
            focus_traversal: true,
        },
        hover: true,
        clipboard: true,
        context_menu: true,
        dialog_presentations: BTreeMap::from([
            (UiDialogPresentation::Inline, ()),
            (UiDialogPresentation::Overlay, ()),
            (UiDialogPresentation::Sheet, ()),
            (UiDialogPresentation::Fullscreen, ()),
        ])
        .into_keys()
        .collect(),
        table: true,
        terminal_selection: true,
        qr_code: true,
        iframe: true,
        rich_color: true,
        fallbacks: BTreeSet::new(),
    }
}

fn package_surface(id: &str) -> PackageSurfaceDescriptor {
    PackageSurfaceDescriptor {
        id: id.to_string(),
        kind: PackageSurfaceKind::App,
        title: "Tickets".to_string(),
        description: None,
        icon: None,
        order: None,
        category: None,
        supports: vec![
            PackageSurfaceOperation::Render,
            PackageSurfaceOperation::Action,
        ],
    }
}

#[test]
fn package_presentation_validates_ids_operations_and_navigation_targets() {
    let surface = package_surface("tickets");
    let navigation = PackageNavigationEntry {
        id: "tickets".to_string(),
        label: "Tickets".to_string(),
        icon: None,
        description: None,
        target: PackageNavigationTarget::Surface {
            surface_id: "tickets".to_string(),
        },
    };
    validate_package_presentation(
        std::slice::from_ref(&surface),
        std::slice::from_ref(&navigation),
    )
    .expect("valid package presentation");

    assert_eq!(
        validate_package_presentation(&[surface.clone(), surface.clone()], &[]),
        Err(PackagePresentationValidationError::DuplicateSurfaceId {
            id: "tickets".to_string()
        })
    );
    assert_eq!(
        validate_package_presentation(
            std::slice::from_ref(&surface),
            &[PackageNavigationEntry {
                target: PackageNavigationTarget::Surface {
                    surface_id: "missing".to_string(),
                },
                ..navigation.clone()
            }],
        ),
        Err(
            PackagePresentationValidationError::UnknownNavigationSurface {
                navigation_id: "tickets".to_string(),
                surface_id: "missing".to_string(),
            }
        )
    );
    assert_eq!(
        validate_package_presentation(
            &[PackageSurfaceDescriptor {
                id: "   ".to_string(),
                ..surface.clone()
            }],
            &[],
        ),
        Err(PackagePresentationValidationError::EmptyId { field: "surface" })
    );
    assert_eq!(
        validate_package_presentation(
            std::slice::from_ref(&surface),
            &[PackageNavigationEntry {
                id: "\t".to_string(),
                ..navigation.clone()
            }],
        ),
        Err(PackagePresentationValidationError::EmptyId {
            field: "navigation"
        })
    );
    assert_eq!(
        validate_package_presentation(
            std::slice::from_ref(&surface),
            &[PackageNavigationEntry {
                target: PackageNavigationTarget::Surface {
                    surface_id: " \n".to_string(),
                },
                ..navigation.clone()
            }],
        ),
        Err(PackagePresentationValidationError::EmptyId {
            field: "navigation surface target"
        })
    );
    assert_eq!(
        validate_package_presentation(
            std::slice::from_ref(&surface),
            &[navigation.clone(), navigation.clone()],
        ),
        Err(PackagePresentationValidationError::DuplicateNavigationId {
            id: "tickets".to_string()
        })
    );
    assert_eq!(
        validate_package_presentation(
            &[PackageSurfaceDescriptor {
                supports: vec![
                    PackageSurfaceOperation::Render,
                    PackageSurfaceOperation::Render,
                ],
                ..surface
            }],
            &[],
        ),
        Err(
            PackagePresentationValidationError::DuplicateSurfaceOperation {
                surface_id: "tickets".to_string(),
                operation: PackageSurfaceOperation::Render,
            }
        )
    );
}

#[test]
fn ui_node_serializes_minimal_and_populated_wire_shape() {
    let minimal = UiNode {
        kind: UiNodeKind::Stack,
        id: None,
        props: Map::new(),
        children: Vec::new(),
        slots: BTreeMap::new(),
    };
    assert_eq!(
        serde_json::to_value(&minimal).expect("serialize minimal node"),
        json!({ "type": "stack" })
    );

    let mut slots = BTreeMap::new();
    slots.insert("title".to_string(), vec![text("Row title")]);

    let node = UiNode {
        kind: UiNodeKind::ListItem,
        id: Some(UiNodeId("ticket-row".to_string()).into()),
        props: Map::from_iter([("value".to_string(), json!("ticket_123"))]),
        children: vec![text("Child")],
        slots,
    };

    let value = serde_json::to_value(&node).expect("serialize populated node");
    assert_eq!(
        value,
        json!({
            "type": "list_item",
            "id": "ticket-row",
            "props": { "value": "ticket_123" },
            "children": [{
                "type": "text",
                "id": "text",
                "props": { "text": "Child" }
            }],
            "slots": {
                "title": [{
                    "type": "text",
                    "id": "text",
                    "props": { "text": "Row title" }
                }]
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<UiNode>(value).expect("deserialize populated node"),
        node
    );
    node.validate().expect("populated node should validate");
}

#[test]
fn required_props_fail_clearly() {
    assert_error_contains(node(UiNodeKind::Stack, json!({})), "direction");
    assert_error_contains(node(UiNodeKind::Text, json!({})), "text");
    assert_error_contains(
        node(UiNodeKind::Button, json!({ "label": "Run" })),
        "action",
    );
    assert_error_contains(
        node(UiNodeKind::SelectOption, json!({ "label": "Open" })),
        "value",
    );
    assert_error_contains(
        node(UiNodeKind::SelectOption, json!({ "value": "open" })),
        "label",
    );
}

#[test]
fn existing_action_nodes_reject_empty_action_ids() {
    assert_error_contains(
        node(
            UiNodeKind::Button,
            json!({ "label": "Run", "action": { "id": "" } }),
        ),
        "action id cannot be empty",
    );
}

#[test]
fn required_slots_fail_clearly() {
    assert_error_contains(node(UiNodeKind::ListItem, json!({})), "title");
    assert_error_contains(node(UiNodeKind::TreeItem, json!({})), "title");
    assert_error_contains(node(UiNodeKind::Menu, json!({})), "items");
    assert_error_contains(
        node(UiNodeKind::Dialog, json!({ "title": "Confirm" })),
        "body",
    );
}

#[test]
fn renderer_specific_props_are_rejected() {
    for (kind, props, expected) in [
        (
            UiNodeKind::Stack,
            json!({ "direction": "vertical", "className": "flex" }),
            "className",
        ),
        (UiNodeKind::Panel, json!({ "padding": "lg" }), "padding"),
        (UiNodeKind::Panel, json!({ "radius": "xl" }), "radius"),
        (
            UiNodeKind::Button,
            json!({ "label": "Run", "action": { "id": "run" }, "leadingIcon": "play" }),
            "leadingIcon",
        ),
        (
            UiNodeKind::Button,
            json!({ "label": "Run", "action": { "id": "run" }, "leading_icon": "play" }),
            "leading_icon",
        ),
        (
            UiNodeKind::Button,
            json!({ "label": "Run", "action": { "id": "run" }, "disabled": true }),
            "disabled",
        ),
        (UiNodeKind::Tree, json!({ "density": "compact" }), "density"),
        (
            UiNodeKind::Text,
            json!({ "text": "Hi", "foo": true }),
            "foo",
        ),
        (
            UiNodeKind::Text,
            json!({ "text": "Hi", "when": { "$kind": "viewport", "viewport": "regular" } }),
            "when",
        ),
    ] {
        assert_error_contains(node(kind, props), expected);
    }
}

#[test]
fn iframe_node_validates_sandboxed_webview_contract() {
    let iframe = node(
        UiNodeKind::Iframe,
        json!({
            "src": "/plugin-assets/vault/graph.html",
            "title": "Vault graph",
            "sandbox": ["allow_scripts"],
            "allow": ["fullscreen"],
            "bridge": {
                "actions": ["vault.graph.open_note"],
                "messages": ["vault.graph.ready"]
            }
        }),
    );

    iframe
        .validate()
        .expect("safe iframe contract should validate");
    assert_eq!(
        serde_json::to_value(&iframe).expect("serialize iframe")["props"]["sandbox"],
        json!(["allow_scripts"])
    );
    assert_eq!(
        serde_json::from_value::<UiNode>(
            serde_json::to_value(&iframe).expect("serialize iframe for round trip"),
        )
        .expect("deserialize iframe"),
        iframe
    );
}

#[test]
fn iframe_omitted_sandbox_and_bridge_mean_restrictive_defaults() {
    let iframe = node(
        UiNodeKind::Iframe,
        json!({
            "src": "/plugin-assets/vault/graph.html",
            "title": "Vault graph"
        }),
    );

    iframe
        .validate()
        .expect("omitted sandbox/allow/bridge should be a valid deny-by-default declaration");

    let empty_policy = node(
        UiNodeKind::Iframe,
        json!({
            "src": "/plugin-assets/vault/graph.html",
            "title": "Vault graph",
            "sandbox": [],
            "allow": [],
            "bridge": {}
        }),
    );
    empty_policy
        .validate()
        .expect("empty sandbox/allow/bridge should remain restrictive");
}

#[test]
fn iframe_requires_nonblank_src_and_title() {
    assert_error_contains(node(UiNodeKind::Iframe, json!({ "title": "Graph" })), "src");
    assert_error_contains(
        node(UiNodeKind::Iframe, json!({ "src": "/graph.html" })),
        "title",
    );
    assert_error_contains(
        node(
            UiNodeKind::Iframe,
            json!({ "src": "   ", "title": "Graph" }),
        ),
        "value cannot be empty",
    );
    assert_error_contains(
        node(
            UiNodeKind::Iframe,
            json!({ "src": "/graph.html", "title": "   " }),
        ),
        "value cannot be empty",
    );

    node(
        UiNodeKind::Iframe,
        json!({
            "src": { "$bind": "/vault.graph.src" },
            "title": { "$bind": "/vault.graph.title" }
        }),
    )
    .validate()
    .expect("bound iframe src/title should validate");
}

#[test]
fn iframe_rejects_raw_html_and_route_layout_props() {
    for forbidden in [
        "html",
        "raw_html",
        "inner_html",
        "srcdoc",
        "dangerouslySetInnerHTML",
        "className",
        "style",
        "layout",
        "padding",
        "sidebar",
        "local_navigation",
    ] {
        let mut props = json!({
            "src": "/plugin-assets/vault/graph.html",
            "title": "Vault graph"
        });
        props[forbidden] = json!("<strong>unsafe</strong>");
        assert_error_contains(node(UiNodeKind::Iframe, props), forbidden);
    }
}

#[test]
fn iframe_policy_props_use_typed_non_overlapping_vocabularies() {
    node(
        UiNodeKind::Iframe,
        json!({
            "src": "/graph.html",
            "title": "Graph",
            "sandbox": ["allow_scripts", "allow_same_origin"],
            "allow": ["fullscreen", "clipboard_write"],
            "bridge": {
                "actions": ["vault.graph.refresh"],
                "messages": ["vault.graph.ready"]
            }
        }),
    )
    .validate()
    .expect("typed iframe policy metadata should validate");

    assert_error_contains(
        node(
            UiNodeKind::Iframe,
            json!({
                "src": "/graph.html",
                "title": "Graph",
                "sandbox": ["scripts"]
            }),
        ),
        "sandbox",
    );
    assert_error_contains(
        node(
            UiNodeKind::Iframe,
            json!({
                "src": "/graph.html",
                "title": "Graph",
                "allow": ["botster_action"]
            }),
        ),
        "allow",
    );
    assert_error_contains(
        node(
            UiNodeKind::Iframe,
            json!({
                "src": "/graph.html",
                "title": "Graph",
                "bridge": { "actions": [" "] }
            }),
        ),
        "bridge action ids cannot be empty",
    );
}

#[test]
fn custom_node_validates_namespaced_escape_hatch_with_static_fallback_slot() {
    let custom = custom_node(node(
        UiNodeKind::EmptyState,
        json!({ "title": "Ticket unavailable" }),
    ));

    custom
        .validate()
        .expect("custom node with fallback should validate");
    assert_eq!(
        serde_json::to_value(&custom).expect("serialize custom")["type"],
        json!("custom")
    );
    assert_eq!(
        custom.custom_fallback().expect("custom fallback").kind,
        UiNodeKind::EmptyState
    );
    assert!(
        node(UiNodeKind::Text, json!({ "text": "Plain" }))
            .custom_fallback()
            .is_none()
    );
    assert_eq!(
        serde_json::from_value::<UiNode>(
            serde_json::to_value(&custom).expect("serialize custom for round trip"),
        )
        .expect("deserialize custom"),
        custom
    );
}

#[test]
fn custom_node_allows_freeform_component_payload_props() {
    let mut custom = custom_node(node(
        UiNodeKind::EmptyState,
        json!({ "title": "Ticket unavailable" }),
    ));
    custom
        .props
        .insert("ticket_id".to_string(), json!("ticket_123"));
    custom.props.insert(
        "data".to_string(),
        json!({ "$bind": "/project-pipelines.ticket/ticket_123" }),
    );
    custom.props.insert(
        "action".to_string(),
        json!({ "packageSpecific": true, "id": 123 }),
    );
    custom.props.insert(
        "render_hint".to_string(),
        json!({ "packageSpecific": true }),
    );

    custom
        .validate()
        .expect("custom payload props should remain package-owned");
    let value = serde_json::to_value(&custom).expect("serialize custom");
    assert_eq!(value["props"]["ticket_id"], json!("ticket_123"));
    assert_eq!(
        serde_json::from_value::<UiNode>(value).expect("deserialize custom"),
        custom
    );

    let mut invalid_bind = custom.clone();
    invalid_bind
        .props
        .insert("data".to_string(), json!({ "$bind": "relative.path" }));
    assert_error_contains(invalid_bind, "path must start");

    let mut fallback_prop = custom;
    fallback_prop
        .props
        .insert("fallback".to_string(), json!({ "type": "text" }));
    assert_error_contains(fallback_prop, "fallback must be declared");
}

#[test]
fn custom_payload_props_do_not_inherit_schema_owned_capability_rules() {
    let mut custom = custom_node(node(
        UiNodeKind::EmptyState,
        json!({ "title": "Ticket unavailable" }),
    ));
    custom
        .props
        .insert("shortcut".to_string(), json!("mod+shift+p"));
    custom
        .props
        .insert("hover_label".to_string(), json!("Hover copy"));
    custom
        .props
        .insert("copy_value".to_string(), json!("ticket_123"));
    custom.props.insert(
        "context_menu".to_string(),
        json!({ "packageSpecific": true }),
    );
    custom
        .props
        .insert("tone".to_string(), json!({ "packageSpecific": true }));

    let mut capabilities = rich_capabilities();
    capabilities.keyboard.shortcuts = false;
    capabilities.hover = false;
    capabilities.clipboard = false;
    capabilities.context_menu = false;
    capabilities.rich_color = false;

    validate_ui_node_with_capabilities(&custom, &capabilities)
        .expect("custom payload prop names should not trigger shared capability gates");
}

#[test]
fn custom_payload_props_do_not_inherit_default_controlled_prop_combination_rules() {
    for controlled_prop in ["value", "checked", "selected"] {
        let mut custom = custom_node(node(
            UiNodeKind::EmptyState,
            json!({ "title": "Ticket unavailable" }),
        ));
        custom
            .props
            .insert("default".to_string(), json!({ "packageDefault": true }));
        custom
            .props
            .insert(controlled_prop.to_string(), json!({ "packageValue": true }));

        validate_ui_node_with_capabilities(&custom, &rich_capabilities())
            .expect("custom payload default/value names should remain package-owned");
    }
}

#[test]
fn custom_node_requires_namespace_component_reason_and_fallback_slot() {
    assert_error_contains(
        node(
            UiNodeKind::Custom,
            json!({
                "component": "ticket-card",
                "reason": "package experiment"
            }),
        ),
        "namespace",
    );
    assert_error_contains(
        node(
            UiNodeKind::Custom,
            json!({
                "namespace": "project-pipelines",
                "reason": "package experiment"
            }),
        ),
        "component",
    );
    assert_error_contains(
        node(
            UiNodeKind::Custom,
            json!({
                "namespace": "project-pipelines",
                "component": "ticket-card"
            }),
        ),
        "reason",
    );
    assert_error_contains(
        node(
            UiNodeKind::Custom,
            json!({
                "namespace": "project-pipelines",
                "component": "ticket-card",
                "reason": "package experiment"
            }),
        ),
        "fallback",
    );
}

#[test]
fn custom_node_rejects_invalid_owner_component_and_reason_values() {
    for (prop, value, expected) in [
        ("namespace", json!("Project Pipelines"), "lowercase ASCII"),
        ("namespace", json!(".project-pipelines"), "separator"),
        ("namespace", json!("project//pipelines"), "lowercase ASCII"),
        ("component", json!("ticket--card"), "adjacent separators"),
        ("component", json!(""), "cannot be empty"),
        ("reason", json!("   "), "cannot be empty"),
        ("reason", json!({ "$bind": "/reason" }), "must be a string"),
    ] {
        let mut custom = custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })));
        custom.props.insert(prop.to_string(), value);
        assert_error_contains(custom, expected);
    }
}

#[test]
fn custom_node_rejects_positional_and_non_static_fallback_shapes() {
    let mut custom = custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })));
    custom.children.push(text("not allowed"));
    assert_error_contains(custom, "custom nodes must put their fallback");

    let mut empty_fallback = custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })));
    empty_fallback
        .slots
        .insert("fallback".to_string(), Vec::new());
    assert_error_contains(empty_fallback, "exactly one static node");

    let mut multi_fallback = custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })));
    multi_fallback.slots.insert(
        "fallback".to_string(),
        vec![
            text("first"),
            UiChild::Node(Box::new(node(
                UiNodeKind::Badge,
                json!({ "label": "second" }),
            ))),
        ],
    );
    assert_error_contains(multi_fallback, "exactly one static node");

    let mut bound_fallback = custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })));
    bound_fallback.slots.insert(
        "fallback".to_string(),
        vec![UiChild::BindIf(UiBindIf::BindIf {
            path: "/project-pipelines.ticket/1/visible".to_string(),
            node: Box::new(node(UiNodeKind::Text, json!({ "text": "Fallback" }))),
        })],
    );
    assert_error_contains(bound_fallback.clone(), "exactly one static node");
    assert!(bound_fallback.custom_fallback().is_none());
}

#[test]
fn custom_node_fallback_is_limited_to_kernel_primitives_or_iframe() {
    custom_node(node(UiNodeKind::Text, json!({ "text": "Fallback" })))
        .validate()
        .expect("kernel primitive fallback should validate");
    custom_node(node(
        UiNodeKind::Iframe,
        json!({ "src": "/plugin-assets/custom.html", "title": "Custom app" }),
    ))
    .validate()
    .expect("iframe fallback should validate as the full custom-app escape");

    for app_kind in [
        UiNodeKind::Metric,
        UiNodeKind::MetricGrid,
        UiNodeKind::Toolbar,
        UiNodeKind::StatusBadge,
        UiNodeKind::Section,
        UiNodeKind::Panel,
        UiNodeKind::TerminalView,
        UiNodeKind::ConnectionCodeView,
        UiNodeKind::ListItem,
        UiNodeKind::TreeItem,
        UiNodeKind::MenuItem,
        UiNodeKind::SelectOption,
        UiNodeKind::FormSection,
        UiNodeKind::FormField,
        UiNodeKind::Custom,
    ] {
        assert_error_contains(
            custom_node(valid_standalone_node(app_kind)),
            "not allowed as a custom fallback",
        );
    }
}

#[test]
fn custom_node_is_the_hatch_for_deferred_high_level_views() {
    let err = serde_json::from_value::<UiNode>(json!({
        "type": "chart",
        "props": { "title": "Tickets" }
    }))
    .expect_err("bare chart must remain outside the shared UiNode vocabulary")
    .to_string();
    assert!(err.contains("unknown variant"));

    let mut custom_chart = custom_node(node(
        UiNodeKind::EmptyState,
        json!({ "title": "Chart unavailable" }),
    ));
    custom_chart
        .props
        .insert("component".to_string(), json!("chart"));
    custom_chart.props.insert(
        "series".to_string(),
        json!({ "$bind": "/project-pipelines.chart/tickets" }),
    );
    custom_chart
        .validate()
        .expect("custom chart with fallback is the sanctioned hatch");
}

#[test]
fn custom_fallback_slot_is_capability_validated() {
    let custom = custom_node(node(
        UiNodeKind::Iframe,
        json!({
            "src": "/plugin-assets/custom.html",
            "title": "Custom app"
        }),
    ));
    let mut capabilities = rich_capabilities();
    capabilities.iframe = false;

    let err = validate_ui_node_with_capabilities(&custom, &capabilities)
        .expect_err("iframe fallback should require iframe capability or fallback");
    assert!(err.to_string().contains("iframe"));

    capabilities
        .fallbacks
        .insert(UiCapabilityFallback::IframeAsLink);
    validate_ui_node_with_capabilities(&custom, &capabilities)
        .expect("slot-based custom fallback should be walked by capability validation");
}

#[test]
fn ui_node_v1_primitive_inventory_is_explicit() {
    let primitives = [
        UiNodeKind::Stack,
        UiNodeKind::Inline,
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
        UiNodeKind::Form,
        UiNodeKind::FormSection,
        UiNodeKind::FormField,
        UiNodeKind::TextInput,
        UiNodeKind::Textarea,
        UiNodeKind::Checkbox,
        UiNodeKind::Select,
        UiNodeKind::SelectOption,
        UiNodeKind::TerminalView,
        UiNodeKind::ConnectionCodeView,
        UiNodeKind::Iframe,
        UiNodeKind::Custom,
    ];

    let wire_names: Vec<_> = primitives
        .into_iter()
        .map(|kind| serde_json::to_value(kind).expect("serialize kind"))
        .collect();

    assert_eq!(
        wire_names,
        vec![
            json!("stack"),
            json!("inline"),
            json!("panel"),
            json!("metric"),
            json!("metric_grid"),
            json!("toolbar"),
            json!("status_badge"),
            json!("section"),
            json!("scroll_area"),
            json!("text"),
            json!("icon"),
            json!("badge"),
            json!("status_dot"),
            json!("empty_state"),
            json!("list"),
            json!("list_item"),
            json!("tree"),
            json!("tree_item"),
            json!("table"),
            json!("button"),
            json!("icon_button"),
            json!("menu"),
            json!("menu_item"),
            json!("dialog"),
            json!("form"),
            json!("form_section"),
            json!("form_field"),
            json!("text_input"),
            json!("textarea"),
            json!("checkbox"),
            json!("select"),
            json!("select_option"),
            json!("terminal_view"),
            json!("connection_code_view"),
            json!("iframe"),
            json!("custom"),
        ]
    );
}

#[test]
fn metric_and_metric_grid_round_trip_semantic_values() {
    let trend = UiMetricTrend {
        direction: UiMetricTrendDirection::Up,
        value: Some(json!("12%")),
        label: Some("Up 12 percent this week".to_string()),
    };
    let mut grid = node(
        UiNodeKind::MetricGrid,
        json!({ "density": "compact", "variant": "subtle", "compact": true }),
    );
    grid.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::Metric,
        json!({
            "label": "Active runs",
            "value": 7,
            "caption": "Across assigned projects",
            "tone": "success",
            "status": "healthy",
            "trend": trend,
            "delta": "+2",
            "action": { "id": "project-pipelines.runs.open" },
            "ref": "/project-pipelines.run"
        }),
    ))));

    let value = serde_json::to_value(&grid).expect("serialize metric grid");
    let decoded = serde_json::from_value::<UiNode>(value).expect("deserialize metric grid");
    assert_eq!(decoded, grid);
    decoded.validate().expect("metric grid should validate");
}

#[test]
fn toolbar_declares_commands_filters_search_and_actions_without_renderer_props() {
    let mut toolbar = node(
        UiNodeKind::Toolbar,
        json!({ "label": "Ticket tools", "density": "regular", "variant": "plain" }),
    );
    toolbar
        .slots
        .insert("commands".to_string(), vec![text("Commands")]);
    toolbar
        .slots
        .insert("filters".to_string(), vec![text("Filters")]);
    toolbar
        .slots
        .insert("search".to_string(), vec![text("Search")]);
    toolbar.slots.insert(
        "actions".to_string(),
        vec![UiChild::Node(Box::new(node(
            UiNodeKind::Button,
            json!({ "label": "Refresh", "action": { "id": "project-pipelines.refresh" } }),
        )))],
    );

    toolbar.validate().expect("toolbar should validate");
    assert_error_contains(
        node(
            UiNodeKind::Toolbar,
            json!({ "label": "Tools", "className": "ion-padding" }),
        ),
        "className",
    );
}

#[test]
fn status_badge_carries_status_without_reusing_renderer_style() {
    let badge = node(
        UiNodeKind::StatusBadge,
        json!({
            "label": "Review",
            "status": "waiting",
            "tone": "warning",
            "hover_label": "Waiting for plan review",
            "action": { "id": "project-pipelines.review.open" }
        }),
    );
    badge.validate().expect("status badge should validate");

    let plain_badge = node(
        UiNodeKind::Badge,
        json!({ "label": "Beta", "tone": "accent" }),
    );
    plain_badge
        .validate()
        .expect("generic badge should still validate");

    let status_dot = node(
        UiNodeKind::StatusDot,
        json!({ "label": "Online", "tone": "success" }),
    );
    status_dot
        .validate()
        .expect("status dot should still validate");

    assert_error_contains(
        node(
            UiNodeKind::Badge,
            json!({ "label": "Review", "status": "waiting" }),
        ),
        "status",
    );
}

#[test]
fn section_and_panel_named_slots_validate() {
    let mut section = node(
        UiNodeKind::Section,
        json!({
            "title": "Work queue",
            "description": "Operator-facing queue",
            "density": "spacious",
            "variant": "emphasized"
        }),
    );
    section
        .slots
        .insert("toolbar".to_string(), vec![text("Tools")]);
    section.slots.insert("body".to_string(), vec![text("Body")]);
    section
        .slots
        .insert("footer".to_string(), vec![text("Footer")]);
    section.validate().expect("section should validate");

    let mut header_only = node(
        UiNodeKind::Section,
        json!({ "density": "regular", "variant": "plain" }),
    );
    header_only
        .slots
        .insert("header".to_string(), vec![text("Header")]);
    header_only
        .validate()
        .expect("header slot should satisfy section identity");

    let mut panel = node(
        UiNodeKind::Panel,
        json!({ "title": "Frame", "tone": "accent", "density": "compact", "variant": "subtle" }),
    );
    panel
        .slots
        .insert("header".to_string(), vec![text("Panel")]);
    panel
        .slots
        .insert("toolbar".to_string(), vec![text("Tools")]);
    panel.slots.insert("body".to_string(), vec![text("Body")]);
    panel.slots.insert("empty".to_string(), vec![text("Empty")]);
    panel
        .slots
        .insert("actions".to_string(), vec![text("Actions")]);
    panel.validate().expect("panel slots should validate");

    assert_error_contains(node(UiNodeKind::Section, json!({})), "title");
    assert_error_contains(
        {
            let mut node = node(UiNodeKind::Panel, json!({ "title": "Panel" }));
            node.slots
                .insert("sidebar".to_string(), vec![text("Sidebar")]);
            node
        },
        "sidebar",
    );
}

#[test]
fn empty_state_accepts_primary_and_secondary_actions() {
    let empty = node(
        UiNodeKind::EmptyState,
        json!({
            "title": "No tickets",
            "description": "Create one to start the queue.",
            "icon": "inbox",
            "primary_action": { "id": "project-pipelines.ticket.new" },
            "secondary_action": { "id": "project-pipelines.docs.open" }
        }),
    );

    empty
        .validate()
        .expect("empty state actions should validate");
}

#[test]
fn table_round_trips_columns_rows_stable_ids_and_node_cells() {
    let table = node(
        UiNodeKind::Table,
        json!({
            "columns": [
                { "id": "title", "label": "Title", "align": "start" },
                "status"
            ],
            "rows": [{
                "id": "ticket_123",
                "cells": {
                    "title": "Fix pipeline",
                    "status": {
                        "type": "status_badge",
                        "id": "ticket_123_status",
                        "props": { "label": "Open", "status": "open", "tone": "success" }
                    }
                },
                "action": { "id": "project-pipelines.ticket.open", "payload": { "id": "ticket_123" } }
            }],
            "empty_state": {
                "type": "empty_state",
                "id": "tickets_empty",
                "props": { "title": "No tickets" }
            },
            "row_action": { "id": "project-pipelines.ticket.open" },
            "selection": { "mode": "multiple", "selected": ["ticket_123"] }
        }),
    );

    let value = serde_json::to_value(&table).expect("serialize table");
    let decoded = serde_json::from_value::<UiNode>(value).expect("deserialize table");
    decoded.validate().expect("typed table should validate");

    let columns = serde_json::from_value::<Vec<UiTableColumn>>(
        decoded.props.get("columns").expect("columns").clone(),
    )
    .expect("typed columns");
    assert!(matches!(
        &columns[0],
        UiTableColumn::Descriptor(UiTableColumnDescriptor { id, .. }) if id == "title"
    ));
    let rows =
        serde_json::from_value::<Vec<UiTableRow>>(decoded.props.get("rows").expect("rows").clone())
            .expect("typed rows");
    assert!(matches!(
        rows[0].cells.get("status").expect("status cell"),
        UiTableCell::Node(node) if node.kind == UiNodeKind::StatusBadge
    ));
}

#[test]
fn table_rejects_rows_without_stable_ids() {
    assert_error_contains(
        node(
            UiNodeKind::Table,
            json!({ "columns": ["title"], "rows": [{ "id": "", "cells": { "title": "Missing id" } }] }),
        ),
        "row ids cannot be empty",
    );
}

#[test]
fn table_selection_and_row_activation_are_semantic() {
    let table = node(
        UiNodeKind::Table,
        json!({
            "columns": ["title"],
            "rows": [{ "id": "ticket_1", "cells": { "title": "One" } }],
            "selection": { "mode": "single", "selected": ["ticket_1"] },
            "activation": { "id": "project-pipelines.ticket.activate" }
        }),
    );
    table.validate().expect("table selection should validate");

    assert_error_contains(
        node(
            UiNodeKind::Table,
            json!({
                "columns": ["title"],
                "selection": { "mode": "single", "selected": ["ticket_1", "ticket_2"] }
            }),
        ),
        "single selection cannot include multiple selected ids",
    );
}

#[test]
fn table_and_list_reject_bare_selected_selection_state() {
    assert_error_contains(
        node(
            UiNodeKind::Table,
            json!({ "columns": ["title"], "selected": ["ticket_1"] }),
        ),
        "selected",
    );
    assert_error_contains(
        node(UiNodeKind::List, json!({ "selected": ["ticket_1"] })),
        "selected",
    );
}

#[test]
fn list_selection_and_item_actions_match_table_semantics() {
    let mut list = node(
        UiNodeKind::List,
        json!({
            "aria_label": "Tickets",
            "selection": { "mode": "single", "selected": ["ticket_1"] }
        }),
    );
    let mut item = node(
        UiNodeKind::ListItem,
        json!({
            "value": "ticket_1",
            "selected": true,
            "action": { "id": "project-pipelines.ticket.open" },
            "activation": { "id": "project-pipelines.ticket.activate" }
        }),
    );
    item.slots
        .insert("title".to_string(), vec![text("Ticket 1")]);
    list.children.push(UiChild::Node(Box::new(item)));

    list.validate().expect("list selection should validate");
}

#[test]
fn renderer_specific_application_props_are_rejected() {
    for (kind, props, expected) in [
        (
            UiNodeKind::Metric,
            json!({ "label": "Runs", "value": 3, "className": "ion-card" }),
            "className",
        ),
        (UiNodeKind::MetricGrid, json!({ "columns": 3 }), "columns"),
        (
            UiNodeKind::Toolbar,
            json!({ "ionSlot": "fixed" }),
            "ionSlot",
        ),
        (
            UiNodeKind::StatusBadge,
            json!({ "label": "Open", "css": "green" }),
            "css",
        ),
        (
            UiNodeKind::Section,
            json!({ "title": "Work", "padding": "lg" }),
            "padding",
        ),
    ] {
        assert_error_contains(node(kind, props), expected);
    }
}

#[test]
fn toolbar_action_overflow_intent_round_trips_and_validates() {
    for (wire_value, expected) in [
        ("auto", UiToolbarOverflow::Auto),
        ("never", UiToolbarOverflow::Never),
        ("always", UiToolbarOverflow::Always),
    ] {
        let value = serde_json::to_value(expected).expect("serialize toolbar overflow intent");
        assert_eq!(value, json!(wire_value));
        assert_eq!(
            serde_json::from_value::<UiToolbarOverflow>(value)
                .expect("deserialize toolbar overflow intent"),
            expected
        );

        for kind in [
            UiNodeKind::Button,
            UiNodeKind::IconButton,
            UiNodeKind::MenuItem,
        ] {
            let mut props = json!({
                "label": "Toolbar action",
                "action": { "id": "toolbar.action" },
                "toolbar_overflow": wire_value
            });
            if kind == UiNodeKind::IconButton {
                props["icon"] = json!("more");
            }
            node(kind, props)
                .validate()
                .expect("toolbar action overflow intent should validate");
        }
    }

    assert_eq!(UiToolbarOverflow::default(), UiToolbarOverflow::Auto);
    let omitted = node(
        UiNodeKind::Button,
        json!({
            "label": "Refresh",
            "action": { "id": "toolbar.refresh" }
        }),
    );
    omitted
        .validate()
        .expect("omitted toolbar overflow intent should mean auto");
    assert!(!omitted.props.contains_key("toolbar_overflow"));

    for invalid in [json!("sometimes"), json!(2)] {
        assert_error_contains(
            node(UiNodeKind::Button, json!({ "toolbar_overflow": invalid })),
            "toolbar_overflow",
        );
    }

    assert_error_contains(
        node(UiNodeKind::Button, json!({ "toolbar_priority": 1 })),
        "toolbar_priority",
    );
}

#[test]
fn deferred_high_level_views_are_rejected_as_unknown_node_kinds() {
    for kind in ["data_grid", "kanban", "timeline", "graph", "action_bar"] {
        let error = serde_json::from_value::<UiNode>(json!({
            "type": kind,
            "id": "deferred"
        }))
        .expect_err("deferred primitive should not deserialize");
        assert!(
            error.to_string().contains("unknown variant"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn public_api_import_path_exposes_application_ui_contract_types() {
    let _density = UiDensity::Compact;
    let _variant = UiVariant::Plain;
    let _toolbar_overflow = UiToolbarOverflow::Auto;
    let _contract_toolbar_overflow = botster_ui_contract::UiToolbarOverflow::Auto;
    let _flat_toolbar_overflow = FlatUiToolbarOverflow::Auto;
    let _selection = UiSelection {
        mode: UiSelectionMode::None,
        selected: Vec::new(),
    };
    let _trend = UiMetricTrend {
        direction: UiMetricTrendDirection::Flat,
        value: None,
        label: None,
    };
    let _column = UiTableColumn::Descriptor(UiTableColumnDescriptor {
        id: "title".to_string(),
        label: Some("Title".to_string()),
        align: None,
    });
    let _row = UiTableRow {
        id: "ticket_1".to_string(),
        cells: BTreeMap::from([("title".to_string(), UiTableCell::Value(json!("One")))]),
        action: None,
    };
}

#[test]
fn form_and_form_section_round_trip_wire_shape() {
    let mut form = node(
        UiNodeKind::Form,
        json!({
            "action": { "id": "project-pipelines.ticket.save" },
            "submit_label": "Save ticket",
            "disabled": false,
            "loading": true,
            "error": { "message": "Save failed" }
        }),
    );
    form.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::FormSection,
        json!({
            "title": "Ticket",
            "description": "Visible in every renderer",
            "disabled": false,
            "loading": false,
            "error": "Section unavailable"
        }),
    ))));

    let value = serde_json::to_value(&form).expect("serialize form");
    assert_eq!(value["type"], "form");
    assert_eq!(value["children"][0]["type"], "form_section");
    assert_eq!(
        serde_json::from_value::<UiNode>(value).expect("deserialize form"),
        form
    );
    form.validate().expect("form tree should validate");

    assert_error_contains(
        idless_node(
            UiNodeKind::Form,
            json!({
                "action": { "id": "save" },
                "submit_label": "Save"
            }),
        ),
        "stable node id",
    );
    assert_error_contains(node(UiNodeKind::FormSection, json!({})), "title");
}

#[test]
fn form_field_schema_round_trips_for_v1_field_kinds() {
    let schemas = [
        UiFieldSchema {
            kind: UiFieldKind::Text,
            name: "title".to_string(),
            label: "Title".to_string(),
            description: Some("Short summary".to_string()),
            placeholder: Some("Ticket title".to_string()),
            required: true,
            default: Some(json!("Draft")),
            validation: Some(UiFieldValidationHints {
                min_length: Some(3),
                max_length: Some(120),
                pattern: Some("^[[:print:]]+$".to_string()),
                ..Default::default()
            }),
            options: Vec::new(),
        },
        UiFieldSchema {
            kind: UiFieldKind::Textarea,
            name: "body".to_string(),
            label: "Body".to_string(),
            description: None,
            placeholder: Some("Details".to_string()),
            required: false,
            default: Some(json!("")),
            validation: None,
            options: Vec::new(),
        },
        UiFieldSchema {
            kind: UiFieldKind::Checkbox,
            name: "notify".to_string(),
            label: "Notify watchers".to_string(),
            description: None,
            placeholder: None,
            required: false,
            default: Some(json!(true)),
            validation: None,
            options: Vec::new(),
        },
        UiFieldSchema {
            kind: UiFieldKind::Select,
            name: "status".to_string(),
            label: "Status".to_string(),
            description: Some("Workflow state".to_string()),
            placeholder: None,
            required: true,
            default: Some(json!("open")),
            validation: Some(UiFieldValidationHints {
                one_of: vec![json!("open"), json!("closed")],
                ..Default::default()
            }),
            options: vec![
                UiFieldOption {
                    value: json!("open"),
                    label: "Open".to_string(),
                    disabled: false,
                },
                UiFieldOption {
                    value: json!("closed"),
                    label: "Closed".to_string(),
                    disabled: true,
                },
            ],
        },
    ];

    for schema in schemas {
        let field = node(
            UiNodeKind::FormField,
            json!({
                "schema": schema,
                "default": schema.default,
                "disabled": false,
                "loading": false,
                "error": null
            }),
        );
        field.validate().expect("form field should validate");

        let value = serde_json::to_value(&field).expect("serialize field");
        assert_eq!(
            serde_json::from_value::<UiNode>(value).expect("deserialize field"),
            field
        );
    }
}

#[test]
fn form_field_schema_rejects_invalid_v1_field_shapes() {
    for (schema, expected) in [
        (
            json!({
                "kind": "text",
                "name": "   ",
                "label": "Title"
            }),
            "schema name cannot be empty",
        ),
        (
            json!({
                "kind": "text",
                "name": "title",
                "label": "   "
            }),
            "schema label cannot be empty",
        ),
        (
            json!({
                "kind": "select",
                "name": "status",
                "label": "Status"
            }),
            "select schema requires options",
        ),
        (
            json!({
                "kind": "text",
                "name": "title",
                "label": "Title",
                "options": [{ "value": "draft", "label": "Draft" }]
            }),
            "only select schema may define options",
        ),
    ] {
        assert_error_contains(
            node(UiNodeKind::FormField, json!({ "schema": schema })),
            expected,
        );
    }
}

#[test]
fn error_prop_rejects_non_renderer_neutral_shapes() {
    for props in [
        json!({ "error": 42 }),
        json!({ "error": { "code": "failed" } }),
        json!({ "error": { "message": false } }),
    ] {
        assert_error_contains(
            node(
                UiNodeKind::Form,
                json!({
                    "action": { "id": "save" },
                    "submit_label": "Save",
                    "error": props["error"].clone()
                }),
            ),
            "error must be a string or object with a string message",
        );
    }
}

#[test]
fn field_schema_accepts_metadata_without_renderer_props() {
    for (kind, props) in [
        (
            UiNodeKind::TextInput,
            json!({
                "name": "title",
                "label": "Title",
                "description": "Visible help",
                "placeholder": "Ticket title",
                "required": true,
                "default": "Draft",
                "disabled": false,
                "loading": false,
                "error": { "message": "Required" },
                "validation": { "minLength": 3, "maxLength": 120 }
            }),
        ),
        (
            UiNodeKind::Textarea,
            json!({
                "name": "body",
                "label": "Body",
                "description": "Markdown allowed",
                "placeholder": "Details",
                "required": false,
                "default": "",
                "disabled": false,
                "loading": false,
                "error": "Too long",
                "validation": { "maxLength": 1000 }
            }),
        ),
        (
            UiNodeKind::Checkbox,
            json!({
                "name": "notify",
                "label": "Notify watchers",
                "description": "Sends a generic notification",
                "required": false,
                "default": true,
                "disabled": false,
                "loading": false,
                "error": null,
                "validation": {}
            }),
        ),
    ] {
        node(kind, props)
            .validate()
            .expect("input metadata should validate");
    }

    let mut select = node(
        UiNodeKind::Select,
        json!({
            "name": "status",
            "label": "Status",
            "description": "Workflow state",
            "required": true,
            "default": "open",
            "disabled": false,
            "loading": false,
            "error": null,
            "validation": { "oneOf": ["open", "closed"] }
        }),
    );
    select.slots.insert(
        "options".to_string(),
        vec![UiChild::Node(Box::new(node(
            UiNodeKind::SelectOption,
            json!({ "value": "open", "label": "Open", "disabled": false }),
        )))],
    );
    select
        .validate()
        .expect("select metadata and option slot should validate");
}

#[test]
fn field_schema_validation_hints_are_metadata_not_policy() {
    let hints = UiFieldValidationHints {
        min_length: Some(10),
        max_length: Some(3),
        pattern: Some("[".to_string()),
        min: Some(10.0),
        max: Some(1.0),
        one_of: vec![json!("a"), json!({ "structured": true })],
    };

    node(
        UiNodeKind::TextInput,
        json!({
            "name": "code",
            "label": "Code",
            "default": "x",
            "validation": hints
        }),
    )
    .validate()
    .expect("core validates hint shape but not business policy");

    assert_error_contains(
        node(
            UiNodeKind::TextInput,
            json!({
                "name": "code",
                "label": "Code",
                "validation": { "minLength": "long" }
            }),
        ),
        "validation",
    );
}

#[test]
fn field_defaults_are_representable_for_each_v1_field_kind() {
    for (kind, props, controlled_prop) in [
        (
            UiNodeKind::TextInput,
            json!({ "name": "title", "label": "Title", "default": "Draft" }),
            "value",
        ),
        (
            UiNodeKind::Textarea,
            json!({ "name": "body", "label": "Body", "default": "Details" }),
            "value",
        ),
        (
            UiNodeKind::Checkbox,
            json!({ "name": "notify", "label": "Notify", "default": true }),
            "checked",
        ),
        (
            UiNodeKind::Select,
            json!({ "name": "status", "label": "Status", "default": "open" }),
            "selected",
        ),
    ] {
        let mut field = node(kind, props);
        if kind == UiNodeKind::Select {
            field.slots.insert(
                "options".to_string(),
                vec![UiChild::Node(Box::new(node(
                    UiNodeKind::SelectOption,
                    json!({ "value": "open", "label": "Open" }),
                )))],
            );
        }
        field.validate().expect("default should validate");

        field
            .props
            .insert(controlled_prop.to_string(), json!("controlled"));
        assert_error_contains(field, "default cannot be used");
    }

    let schema = UiFieldSchema {
        kind: UiFieldKind::Text,
        name: "title".to_string(),
        label: "Title".to_string(),
        description: None,
        placeholder: None,
        required: false,
        default: Some(json!("Draft")),
        validation: None,
        options: Vec::new(),
    };

    node(
        UiNodeKind::FormField,
        json!({ "schema": schema, "default": "Draft" }),
    )
    .validate()
    .expect("form_field node default may mirror schema default");

    assert_error_contains(
        node(
            UiNodeKind::FormField,
            json!({ "schema": schema, "default": "Different" }),
        ),
        "default must match schema default",
    );
}

#[test]
fn explicit_value_checked_or_selected_marks_field_controlled() {
    for (kind, props) in [
        (
            UiNodeKind::TextInput,
            json!({ "name": "title", "label": "Title", "value": "Plugin owned" }),
        ),
        (
            UiNodeKind::Textarea,
            json!({ "name": "body", "label": "Body", "value": "Plugin owned" }),
        ),
        (
            UiNodeKind::Checkbox,
            json!({ "name": "notify", "label": "Notify", "checked": true }),
        ),
    ] {
        node(kind, props)
            .validate()
            .expect("controlled field should validate with stable id");
    }

    let mut select = node(
        UiNodeKind::Select,
        json!({ "name": "status", "label": "Status", "selected": "open" }),
    );
    select.slots.insert(
        "options".to_string(),
        vec![UiChild::Node(Box::new(node(
            UiNodeKind::SelectOption,
            json!({ "value": "open", "label": "Open" }),
        )))],
    );
    select.validate().expect("selected alias should validate");
}

#[test]
fn renderer_local_fields_require_stable_node_ids() {
    assert_error_contains(
        idless_node(
            UiNodeKind::TextInput,
            json!({ "name": "title", "label": "Title", "default": "Draft" }),
        ),
        "stable node id",
    );
    assert_error_contains(
        idless_node(
            UiNodeKind::Checkbox,
            json!({ "name": "notify", "label": "Notify", "checked": false }),
        ),
        "stable node id",
    );

    idless_node(
        UiNodeKind::TextInput,
        json!({ "name": "title", "label": "Title" }),
    )
    .validate()
    .expect("static field metadata without state may omit id");
}

#[test]
fn form_field_and_action_state_props_validate() {
    let schema = UiFieldSchema {
        kind: UiFieldKind::Text,
        name: "title".to_string(),
        label: "Title".to_string(),
        description: None,
        placeholder: None,
        required: true,
        default: None,
        validation: None,
        options: Vec::new(),
    };

    node(
        UiNodeKind::FormField,
        json!({
            "schema": schema,
            "disabled": true,
            "loading": true,
            "error": { "message": "Unavailable" }
        }),
    )
    .validate()
    .expect("node-level state props should validate");

    let action = UiAction {
        id: UiActionId("save".to_string()),
        payload: None,
        disabled: true,
    };
    assert_eq!(
        serde_json::to_value(&action).expect("serialize action disabled")["disabled"],
        true
    );

    assert_error_contains(
        node(
            UiNodeKind::FormField,
            json!({
                "schema": schema,
                "disabled": "yes"
            }),
        ),
        "disabled",
    );
}

#[test]
fn action_emitters_require_stable_node_ids_for_pending_feedback() {
    assert_error_contains(
        idless_node(
            UiNodeKind::Button,
            json!({ "label": "Save", "action": { "id": "save" } }),
        ),
        "stable node id",
    );

    node(
        UiNodeKind::Button,
        json!({ "label": "Save", "action": { "id": "save" } }),
    )
    .validate()
    .expect("action emitter with id should validate");
}

#[test]
fn unknown_ui_node_kind_is_rejected() {
    let err = serde_json::from_value::<UiNode>(json!({
        "type": "overlay",
        "props": {}
    }));
    assert!(err.is_err());
}

#[test]
fn renderer_specific_form_props_are_rejected() {
    for (kind, props, expected) in [
        (
            UiNodeKind::Form,
            json!({
                "method": "post",
                "action": { "id": "save" },
                "submit_label": "Save"
            }),
            "method",
        ),
        (
            UiNodeKind::FormSection,
            json!({ "title": "Profile", "className": "gap-2" }),
            "className",
        ),
        (
            UiNodeKind::FormField,
            json!({
                "schema": {
                    "kind": "text",
                    "name": "title",
                    "label": "Title"
                },
                "component": "IonInput"
            }),
            "component",
        ),
    ] {
        assert_error_contains(node(kind, props), expected);
    }
}

#[test]
fn icon_button_requires_accessible_label() {
    assert_error_contains(
        node(
            UiNodeKind::IconButton,
            json!({ "icon": "play", "action": { "id": "run" } }),
        ),
        "label",
    );
    assert_error_contains(
        node(
            UiNodeKind::IconButton,
            json!({ "label": "", "icon": "play", "action": { "id": "run" } }),
        ),
        "label",
    );

    node(
        UiNodeKind::IconButton,
        json!({ "label": "Run", "icon": "play", "action": { "id": "run" } }),
    )
    .validate()
    .expect("labeled icon button should validate");
}

#[test]
fn binding_paths_serialize_exactly() {
    for path in ["/project-pipelines.ticket/ticket_123/title", "@/title"] {
        let bind = UiBind {
            path: path.to_string(),
        };
        let value = serde_json::to_value(&bind).expect("serialize bind");
        assert_eq!(value, json!({ "$bind": path }));
        assert_eq!(
            serde_json::from_value::<UiBind>(value).expect("deserialize bind"),
            bind
        );
    }

    let err = node(UiNodeKind::Text, json!({ "text": { "$bind": "title" } }))
        .validate()
        .expect_err("relative bind without @/ should fail");
    assert!(matches!(
        err,
        UiValidationError::Node {
            source,
            ..
        } if matches!(*source, UiValidationError::InvalidBindPath { .. })
    ));

    assert_error_contains(
        node(UiNodeKind::Text, json!({ "text": { "$bind": 123 } })),
        "$bind value must be a string",
    );
}

#[test]
fn bind_list_and_bind_if_wire_shapes_round_trip() {
    let bind_list = UiBindList::BindList {
        source: "/project-pipelines.ticket".to_string(),
        r#where: BTreeMap::from([("status".to_string(), json!("open"))]),
        item_template: Box::new(node(
            UiNodeKind::Text,
            json!({ "text": { "$bind": "@/title" } }),
        )),
        empty_template: Some(Box::new(node(
            UiNodeKind::EmptyState,
            json!({ "title": "No tickets" }),
        ))),
    };
    let value = serde_json::to_value(&bind_list).expect("serialize bind_list");
    assert_eq!(
        value,
        json!({
            "$kind": "bind_list",
            "source": "/project-pipelines.ticket",
            "where": { "status": "open" },
            "item_template": {
                "type": "text",
                "id": "text",
                "props": { "text": { "$bind": "@/title" } }
            },
            "empty_template": {
                "type": "empty_state",
                "id": "emptystate",
                "props": { "title": "No tickets" }
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<UiBindList>(value).expect("deserialize bind_list"),
        bind_list
    );

    let bind_if = UiBindIf::BindIf {
        path: "@/active".to_string(),
        node: Box::new(node(UiNodeKind::Text, json!({ "text": "Active" }))),
    };
    let value = serde_json::to_value(&bind_if).expect("serialize bind_if");
    assert_eq!(value["$kind"], "bind_if");
    assert_eq!(value["path"], "@/active");
    assert_eq!(
        serde_json::from_value::<UiBindIf>(value).expect("deserialize bind_if"),
        bind_if
    );
}

#[test]
fn bound_node_identity_is_valid_only_on_a_bind_list_item_template() {
    let bound_button = serde_json::from_value::<UiNode>(json!({
        "type": "button",
        "id": { "$bind": "@/session_uuid" },
        "props": {
            "label": "Select session",
            "action": { "id": "contract.action" }
        }
    }))
    .expect("bound-id button");
    assert_eq!(
        bound_button.id,
        Some(UiAuthoredNodeId::Bind(UiBind {
            path: "@/session_uuid".to_string()
        }))
    );
    assert_eq!(
        serde_json::to_value(&bound_button).expect("serialize bound-id button")["id"],
        json!({ "$bind": "@/session_uuid" })
    );

    for error in [
        bound_button
            .validate()
            .expect_err("detached root must fail"),
        validate_ui_node(&bound_button).expect_err("public root validator must fail"),
        validate_ui_node_with_capabilities(&bound_button, &rich_capabilities())
            .expect_err("capability validator must retain root semantics"),
    ] {
        assert!(error.to_string().contains("bind_list item_template"));
    }

    let valid_tree = serde_json::from_value::<UiNode>(json!({
        "type": "panel",
        "id": "sessions",
        "props": { "title": "Sessions" },
        "children": [{
            "$kind": "bind_list",
            "source": "/session",
            "item_template": {
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }
        }]
    }))
    .expect("bound-id BindList tree");
    valid_tree
        .validate()
        .expect("BindList item template supplies row context");

    for invalid in [
        json!({
            "type": "panel",
            "id": "sessions",
            "props": { "title": "Sessions" },
            "children": [{
                "type": "button",
                "id": { "$bind": "@/session_uuid" },
                "props": {
                    "label": "Select session",
                    "action": { "id": "contract.action" }
                }
            }]
        }),
        json!({
            "type": "panel",
            "id": "sessions",
            "props": { "title": "Sessions" },
            "children": [{
                "$kind": "bind_list",
                "source": "/session",
                "item_template": {
                    "type": "button",
                    "id": { "$bind": "/session/session-1/session_uuid" },
                    "props": {
                        "label": "Select session",
                        "action": { "id": "contract.action" }
                    }
                }
            }]
        }),
        json!({
            "type": "panel",
            "id": "sessions",
            "props": { "title": "Sessions" },
            "children": [{
                "$kind": "bind_list",
                "source": "/session",
                "item_template": {
                    "type": "text",
                    "id": "session",
                    "props": { "text": "Session" }
                },
                "empty_template": {
                    "type": "button",
                    "id": { "$bind": "@/session_uuid" },
                    "props": {
                        "label": "Select session",
                        "action": { "id": "contract.action" }
                    }
                }
            }]
        }),
    ] {
        let node = serde_json::from_value::<UiNode>(invalid).expect("invalid-context node");
        assert_error_contains(node, "bind_list item_template");
    }
}

#[test]
fn authored_node_identity_rejects_malformed_bind_sentinels() {
    for id in [
        json!({ "$bind": 1 }),
        json!({ "$bind": "@/session_uuid", "fallback": "session" }),
        json!({ "bind": "@/session_uuid" }),
    ] {
        serde_json::from_value::<UiNode>(json!({
            "type": "text",
            "id": id,
            "props": { "text": "Session" }
        }))
        .expect_err("malformed authored id must fail deserialization");
    }
}

#[test]
fn bind_list_filters_are_exact_top_level_fields() {
    let empty_field = UiBindList::BindList {
        source: "/project-pipelines.ticket".to_string(),
        r#where: BTreeMap::from([("".to_string(), json!("open"))]),
        item_template: Box::new(node(
            UiNodeKind::Text,
            json!({ "text": { "$bind": "@/title" } }),
        )),
        empty_template: None,
    };
    let mut parent = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    parent.children.push(UiChild::BindList(empty_field));
    assert_error_contains(parent, "field cannot be empty");

    for field in ["ticket.status", "ticket/status"] {
        let bind_list = UiBindList::BindList {
            source: "/project-pipelines.ticket".to_string(),
            r#where: BTreeMap::from([(field.to_string(), json!("open"))]),
            item_template: Box::new(node(
                UiNodeKind::Text,
                json!({ "text": { "$bind": "@/title" } }),
            )),
            empty_template: None,
        };
        let mut parent = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
        parent.children.push(UiChild::BindList(bind_list));

        assert_error_contains(parent, "top-level");
    }

    let bind_list = UiBindList::BindList {
        source: "@/children".to_string(),
        r#where: BTreeMap::new(),
        item_template: Box::new(text_node("Child")),
        empty_template: None,
    };
    let mut parent = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    parent.children.push(UiChild::BindList(bind_list));

    assert_error_contains(parent, "absolute entity family path");
}

#[test]
fn responsive_and_conditionals_wire_shapes_round_trip() {
    let responsive = UiResponsiveValue::Responsive {
        width: Some(UiResponsiveWidth {
            compact: Some(json!("vertical")),
            expanded: Some(json!("horizontal")),
            ..Default::default()
        }),
        height: Some(UiResponsiveHeight {
            short: Some(json!("sm")),
            tall: Some(json!("md")),
            ..Default::default()
        }),
    };
    let value = serde_json::to_value(&responsive).expect("serialize responsive");
    assert_eq!(
        value,
        json!({
            "$kind": "responsive",
            "width": {
                "compact": "vertical",
                "expanded": "horizontal"
            },
            "height": {
                "short": "sm",
                "tall": "md"
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<UiResponsiveValue>(value).expect("deserialize responsive"),
        responsive
    );

    let condition = UiCondition {
        width: Some(UiWidthClass::Compact),
        pointer: Some(UiPointer::Coarse),
        keyboard_occluded: Some(true),
        ..Default::default()
    };
    let conditional = UiConditional::Hidden {
        condition,
        node: Box::new(text_node("Metadata")),
    };
    let value = serde_json::to_value(&conditional).expect("serialize conditional");
    assert_eq!(
        value,
        json!({
            "$kind": "hidden",
            "condition": {
                "width": "compact",
                "pointer": "coarse",
                "keyboardOccluded": true
            },
            "node": {
                "type": "text",
                "id": "text",
                "props": { "text": "Metadata" }
            }
        })
    );
    assert_eq!(
        serde_json::from_value::<UiConditional>(value).expect("deserialize conditional"),
        conditional
    );

    let mut parent = node(
        UiNodeKind::Stack,
        json!({ "direction": { "$kind": "responsive", "width": { "compact": "vertical", "expanded": "horizontal" } } }),
    );
    parent
        .children
        .push(UiChild::Conditional(UiConditional::When {
            condition: UiCondition {
                height: Some(UiHeightClass::Tall),
                ..Default::default()
            },
            node: Box::new(text_node("Tall")),
        }));
    parent
        .children
        .push(UiChild::Conditional(UiConditional::When {
            condition: UiCondition::default(),
            node: Box::new(text_node("Always")),
        }));
    parent
        .validate()
        .expect("conditional child should validate");

    let unknown_child = serde_json::from_value::<UiChild>(json!({
        "$kind": "viewport",
        "viewport": "regular"
    }));
    assert!(unknown_child.is_err());
}

#[test]
fn ui_capability_set_serializes_renderer_neutral_wire_shape() {
    let capabilities = UiCapabilitySet {
        width_classes: BTreeMap::from([(UiWidthClass::Compact, ()), (UiWidthClass::Regular, ())])
            .into_keys()
            .collect(),
        height_classes: BTreeMap::from([(UiHeightClass::Regular, ())])
            .into_keys()
            .collect(),
        pointer: UiPointer::Coarse,
        keyboard: UiKeyboardCapability {
            text_entry: true,
            shortcuts: false,
            focus_traversal: true,
        },
        hover: false,
        clipboard: true,
        context_menu: false,
        dialog_presentations: BTreeMap::from([(UiDialogPresentation::Inline, ())])
            .into_keys()
            .collect(),
        table: false,
        terminal_selection: false,
        qr_code: false,
        iframe: false,
        rich_color: false,
        fallbacks: BTreeMap::from([
            (UiCapabilityFallback::TableAsList, ()),
            (UiCapabilityFallback::DialogInline, ()),
            (UiCapabilityFallback::ConnectionCodeText, ()),
        ])
        .into_keys()
        .collect(),
    };
    let value = serde_json::to_value(&capabilities).expect("serialize capabilities");

    assert_eq!(
        value,
        json!({
            "widthClasses": ["compact", "regular"],
            "heightClasses": ["regular"],
            "pointer": "coarse",
            "keyboard": {
                "textEntry": true,
                "focusTraversal": true
            },
            "clipboard": true,
            "dialogPresentations": ["inline"],
            "fallbacks": ["table_as_list", "dialog_inline", "connection_code_text"]
        })
    );
    assert_eq!(
        serde_json::from_value::<UiCapabilitySet>(value).expect("deserialize capabilities"),
        capabilities
    );
}

#[test]
fn capability_validation_accepts_supported_or_declared_downgrade_nodes() {
    let mut table = node(UiNodeKind::Table, json!({ "columns": ["title", "status"] }));
    table.children.push(text("Row"));
    rich_capabilities()
        .validate_node(&table)
        .expect("rich renderer supports table directly");

    let mut downgraded = rich_capabilities();
    downgraded.table = false;
    downgraded
        .fallbacks
        .insert(UiCapabilityFallback::TableAsList);
    validate_ui_node_with_capabilities(&table, &downgraded)
        .expect("declared table downgrade should pass");

    downgraded
        .fallbacks
        .remove(&UiCapabilityFallback::TableAsList);
    let err = validate_ui_node_with_capabilities(&table, &downgraded)
        .expect_err("missing table fallback should fail");
    assert!(matches!(
        err,
        UiValidationError::Node {
            source,
            ..
        } if matches!(*source, UiValidationError::UnsupportedCapability { capability: "table", .. })
    ));
}

#[test]
fn capability_validation_pins_dialog_terminal_qr_and_color_downgrades() {
    let mut capabilities = rich_capabilities();
    capabilities.dialog_presentations.clear();
    capabilities.terminal_selection = false;
    capabilities.qr_code = false;
    capabilities.rich_color = false;
    capabilities.fallbacks = BTreeMap::from([
        (UiCapabilityFallback::DialogInline, ()),
        (UiCapabilityFallback::TerminalSelectionDisabled, ()),
        (UiCapabilityFallback::ConnectionCodeText, ()),
        (UiCapabilityFallback::RichColorMuted, ()),
    ])
    .into_keys()
    .collect();

    let mut root = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    let mut dialog = node(
        UiNodeKind::Dialog,
        json!({ "title": "Confirm", "presentation": "sheet" }),
    );
    dialog.slots.insert("body".to_string(), vec![text("Body")]);
    root.children.push(UiChild::Node(Box::new(dialog)));
    root.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::TerminalView,
        json!({ "session_id": "sess_1" }),
    ))));
    root.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::ConnectionCodeView,
        json!({ "code": "pairing-code" }),
    ))));
    root.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::Text,
        json!({ "text": "Status", "tone": "success" }),
    ))));

    validate_ui_node_with_capabilities(&root, &capabilities)
        .expect("declared downgrades should cover unsupported capabilities");

    capabilities
        .fallbacks
        .remove(&UiCapabilityFallback::TerminalSelectionDisabled);
    let err = validate_ui_node_with_capabilities(&root, &capabilities)
        .expect_err("missing terminal-selection fallback should fail");
    assert!(err.to_string().contains("terminalSelection"));
}

#[test]
fn capability_validation_requires_iframe_support_or_link_fallback() {
    let iframe = node(
        UiNodeKind::Iframe,
        json!({
            "src": "/plugin-assets/vault/graph.html",
            "title": "Vault graph"
        }),
    );

    rich_capabilities()
        .validate_node(&iframe)
        .expect("rich renderer supports iframe directly");

    let mut downgraded = rich_capabilities();
    downgraded.iframe = false;
    let err = validate_ui_node_with_capabilities(&iframe, &downgraded)
        .expect_err("iframe without renderer support or fallback should fail");
    assert!(err.to_string().contains("iframe"));

    downgraded
        .fallbacks
        .insert(UiCapabilityFallback::IframeAsLink);
    validate_ui_node_with_capabilities(&iframe, &downgraded)
        .expect("declared iframe link fallback should pass");
}

#[test]
fn capability_validation_pins_shortcut_hover_clipboard_and_context_menu_downgrades() {
    let mut capabilities = rich_capabilities();
    capabilities.keyboard.shortcuts = false;
    capabilities.hover = false;
    capabilities.clipboard = false;
    capabilities.context_menu = false;
    capabilities.fallbacks = BTreeMap::from([
        (UiCapabilityFallback::HoverPersistentHints, ()),
        (UiCapabilityFallback::ClipboardManual, ()),
        (UiCapabilityFallback::ContextMenuAsMenu, ()),
    ])
    .into_keys()
    .collect();

    let mut root = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    root.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::Text,
        json!({
            "text": "Pairing code",
            "hover_label": "Visible hint when hover is unavailable",
            "copy_value": "pair-fixture"
        }),
    ))));
    root.children.push(UiChild::Node(Box::new(node(
        UiNodeKind::Button,
        json!({
            "label": "More",
            "action": { "id": "fixture.more" },
            "context_menu": [{ "id": "fixture.inspect" }]
        }),
    ))));

    validate_ui_node_with_capabilities(&root, &capabilities)
        .expect("declared hover/clipboard/context-menu fallbacks should pass");

    capabilities
        .fallbacks
        .remove(&UiCapabilityFallback::ContextMenuAsMenu);
    let err = validate_ui_node_with_capabilities(&root, &capabilities)
        .expect_err("missing context-menu fallback should fail");
    assert!(err.to_string().contains("contextMenu"));

    let shortcut = node(
        UiNodeKind::Button,
        json!({
            "label": "Run",
            "action": { "id": "fixture.run" },
            "shortcut": "mod+enter"
        }),
    );
    let err = validate_ui_node_with_capabilities(&shortcut, &capabilities)
        .expect_err("missing shortcut capability should fail");
    assert!(err.to_string().contains("keyboard.shortcuts"));

    capabilities.keyboard.shortcuts = true;
    validate_ui_node_with_capabilities(&shortcut, &capabilities)
        .expect("shortcut capability should permit shortcut metadata");
}

#[test]
fn capability_validation_keeps_controlled_and_renderer_local_state_expectations() {
    let mut capabilities = rich_capabilities();
    capabilities.keyboard.text_entry = false;

    let input = node(
        UiNodeKind::TextInput,
        json!({ "name": "title", "label": "Title", "value": "Owner authored" }),
    );

    let err = validate_ui_node_with_capabilities(&input, &capabilities)
        .expect_err("missing text-entry capability should fail");
    assert!(err.to_string().contains("textEntry"));

    capabilities.keyboard.text_entry = true;
    validate_ui_node_with_capabilities(&input, &capabilities)
        .expect("text entry capability should permit controlled text input");
}

#[test]
fn token_props_are_validated() {
    node(
        UiNodeKind::Stack,
        json!({ "direction": "vertical", "gap": "md" }),
    )
    .validate()
    .expect("valid spacing token should pass");

    node(UiNodeKind::Text, json!({ "text": "OK", "tone": "success" }))
        .validate()
        .expect("valid color token should pass");

    assert_error_contains(
        node(
            UiNodeKind::Stack,
            json!({ "direction": "vertical", "gap": "massive" }),
        ),
        "gap",
    );
    assert_error_contains(
        node(UiNodeKind::Text, json!({ "text": "OK", "tone": "brand" })),
        "tone",
    );
}

#[test]
fn ui_action_descriptor_serializes_semantic_id_and_payload() {
    let action = UiAction {
        id: UiActionId("project-pipelines.advance".to_string()),
        payload: Some(json!({ "ticket_id": "ticket_123" })),
        disabled: true,
    };
    assert_eq!(
        serde_json::to_value(&action).expect("serialize action"),
        json!({
            "id": "project-pipelines.advance",
            "payload": { "ticket_id": "ticket_123" },
            "disabled": true
        })
    );
}

#[test]
fn ui_action_submit_request_round_trips_form_values() {
    let request = UiActionRequest {
        request_id: UiActionRequestId("req_123".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: Some(UiNodeId("advance-button".to_string())),
        action_id: UiActionId("project-pipelines.ticket.submit".to_string()),
        kind: UiActionKind::Submit,
        values: Some(UiFormValues(Map::from_iter([
            ("title".to_string(), json!("Fix checkout flow")),
            ("notify".to_string(), json!(true)),
            ("priority".to_string(), json!("high")),
        ]))),
        payload: Some(json!({ "ticket_id": "ticket_123" })),
    };
    let value = serde_json::to_value(&request).expect("serialize submit request");
    assert_eq!(
        value,
        json!({
            "request_id": "req_123",
            "surface_id": "project-pipelines.ticket.form",
            "action_id": "project-pipelines.ticket.submit",
            "node_id": "advance-button",
            "kind": "submit",
            "values": {
                "title": "Fix checkout flow",
                "notify": true,
                "priority": "high"
            },
            "payload": { "ticket_id": "ticket_123" }
        })
    );
    assert_eq!(
        serde_json::from_value::<UiActionRequest>(value).expect("deserialize submit request"),
        request
    );
}

#[test]
fn ui_action_validate_round_trip_returns_field_and_form_errors() {
    let request = UiActionRequest {
        request_id: UiActionRequestId("req_123".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: Some(UiNodeId("ticket-form".to_string())),
        action_id: UiActionId("project-pipelines.ticket.validate".to_string()),
        kind: UiActionKind::Validate,
        values: Some(UiFormValues(Map::from_iter([
            ("title".to_string(), json!("")),
            ("priority".to_string(), json!("unknown")),
        ]))),
        payload: None,
    };
    let request_value = serde_json::to_value(&request).expect("serialize validate request");
    assert_eq!(
        serde_json::from_value::<UiActionRequest>(request_value)
            .expect("deserialize validate request"),
        request
    );

    let mut field_errors = UiFieldErrors::new();
    field_errors.insert("title".to_string(), vec!["Title is required".to_string()]);
    field_errors.insert(
        "priority".to_string(),
        vec!["Priority is not selectable".to_string()],
    );

    let result = UiActionResult {
        request_id: UiActionRequestId("req_123".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: Some(UiNodeId("ticket-form".to_string())),
        action_id: UiActionId("project-pipelines.ticket.validate".to_string()),
        state: UiActionResultState::Rejected,
        field_errors,
        form_errors: vec!["Fix the highlighted fields".to_string()],
        warnings: Vec::new(),
        normalized_values: None,
        presentation: None,
        replacement: None,
        payload: None,
        error: None,
    };
    let value = serde_json::to_value(&result).expect("serialize validation result");
    assert_eq!(
        value,
        json!({
            "request_id": "req_123",
            "surface_id": "project-pipelines.ticket.form",
            "action_id": "project-pipelines.ticket.validate",
            "node_id": "ticket-form",
            "state": "rejected",
            "field_errors": {
                "priority": ["Priority is not selectable"],
                "title": ["Title is required"]
            },
            "form_errors": ["Fix the highlighted fields"]
        })
    );
    assert_eq!(
        serde_json::from_value::<UiActionResult>(value).expect("deserialize validation result"),
        result
    );
}

#[test]
fn ui_action_result_returns_normalized_values_and_warnings() {
    let result = UiActionResult {
        request_id: UiActionRequestId("req_125".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: Some(UiNodeId("ticket-form".to_string())),
        action_id: UiActionId("project-pipelines.ticket.submit".to_string()),
        state: UiActionResultState::Accepted,
        field_errors: UiFieldErrors::new(),
        form_errors: Vec::new(),
        warnings: vec!["Title was trimmed".to_string()],
        normalized_values: Some(UiFormValues(Map::from_iter([
            ("title".to_string(), json!("Fix checkout flow")),
            ("notify".to_string(), json!(true)),
        ]))),
        presentation: None,
        replacement: None,
        payload: Some(json!({ "ticket_id": "ticket_123" })),
        error: None,
    };
    assert_eq!(
        serde_json::to_value(&result).expect("serialize accepted result"),
        json!({
            "request_id": "req_125",
            "surface_id": "project-pipelines.ticket.form",
            "action_id": "project-pipelines.ticket.submit",
            "node_id": "ticket-form",
            "state": "accepted",
            "warnings": ["Title was trimmed"],
            "normalized_values": {
                "notify": true,
                "title": "Fix checkout flow"
            },
            "payload": { "ticket_id": "ticket_123" }
        })
    );
}

#[test]
fn ui_action_rejected_result_preserves_request_correlation() {
    let result = UiActionResult {
        request_id: UiActionRequestId("req_124".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: Some(UiNodeId("advance-button".to_string())),
        action_id: UiActionId("project-pipelines.advance".to_string()),
        state: UiActionResultState::Rejected,
        field_errors: UiFieldErrors::new(),
        form_errors: Vec::new(),
        warnings: Vec::new(),
        normalized_values: None,
        presentation: None,
        replacement: None,
        payload: None,
        error: Some("gate unmet".to_string()),
    };
    let value = serde_json::to_value(&result).expect("serialize rejected result");
    let round_trip =
        serde_json::from_value::<UiActionResult>(value).expect("deserialize rejected result");
    assert_eq!(
        round_trip.request_id,
        UiActionRequestId("req_124".to_string())
    );
    assert_eq!(
        round_trip.surface_id,
        UiSurfaceId("project-pipelines.ticket.form".to_string())
    );
    assert_eq!(
        round_trip.action_id,
        UiActionId("project-pipelines.advance".to_string())
    );
    assert_eq!(
        round_trip.node_id,
        Some(UiNodeId("advance-button".to_string()))
    );
    assert_eq!(round_trip.state, UiActionResultState::Rejected);
}

#[test]
fn ui_action_deferred_and_error_states_are_distinct() {
    let deferred = UiActionResult {
        request_id: UiActionRequestId("req_126".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: None,
        action_id: UiActionId("project-pipelines.advance".to_string()),
        state: UiActionResultState::Deferred,
        field_errors: UiFieldErrors::new(),
        form_errors: Vec::new(),
        warnings: Vec::new(),
        normalized_values: None,
        presentation: None,
        replacement: None,
        payload: Some(json!({ "operation_id": "op_1" })),
        error: None,
    };
    let errored = UiActionResult {
        request_id: UiActionRequestId("req_127".to_string()),
        state: UiActionResultState::Error,
        error: Some("handler unavailable".to_string()),
        ..deferred.clone()
    };

    let deferred_value = serde_json::to_value(&deferred).expect("serialize deferred");
    let error_value = serde_json::to_value(&errored).expect("serialize error");
    assert_eq!(deferred_value["state"], json!("deferred"));
    assert!(deferred_value.get("error").is_none());
    assert_eq!(error_value["state"], json!("error"));
    assert_eq!(error_value["error"], json!("handler unavailable"));
}

#[test]
fn ui_action_result_applies_accepted_presentation_and_inline_replacement() {
    let result = UiActionResult {
        request_id: UiActionRequestId("req_128".to_string()),
        surface_id: UiSurfaceId("project-pipelines.ticket.form".to_string()),
        node_id: None,
        action_id: UiActionId("project-pipelines.refresh".to_string()),
        state: UiActionResultState::Accepted,
        field_errors: UiFieldErrors::new(),
        form_errors: Vec::new(),
        warnings: Vec::new(),
        normalized_values: None,
        presentation: Some(vec![UiPresentationOperation::Clear {
            key: UiPresentationKey("create-ticket".to_string()),
        }]),
        replacement: Some(Box::new(text_node("Updated"))),
        payload: None,
        error: None,
    };
    result.validate().expect("accepted effects should validate");
    let value = serde_json::to_value(&result).expect("serialize accepted effects");
    assert_eq!(
        serde_json::from_value::<UiActionResult>(value).expect("deserialize accepted effects"),
        result
    );

    let rejected = UiActionResult {
        state: UiActionResultState::Rejected,
        ..result.clone()
    };
    assert_eq!(
        rejected.validate(),
        Err(UiActionResultValidationError::EffectsRequireAcceptance)
    );

    let empty_presentation_key = UiActionResult {
        presentation: Some(vec![UiPresentationOperation::Clear {
            key: UiPresentationKey(" ".to_string()),
        }]),
        replacement: None,
        ..result.clone()
    };
    assert_eq!(
        empty_presentation_key.validate(),
        Err(UiActionResultValidationError::EmptyPresentationKey)
    );

    let invalid_replacement = UiActionResult {
        presentation: None,
        replacement: Some(Box::new(node(
            UiNodeKind::Form,
            json!({ "action": { "id": "ticket.create" } }),
        ))),
        ..result
    };
    assert_eq!(
        invalid_replacement.validate(),
        Err(UiActionResultValidationError::InvalidReplacement(
            UiValidationError::Node {
                id: Some(UiNodeId("form".to_string()).into()),
                kind: UiNodeKind::Form,
                source: Box::new(UiValidationError::MissingProp {
                    kind: UiNodeKind::Form,
                    prop: "submit_label",
                }),
            }
        ))
    );
    assert!(
        serde_json::from_value::<UiActionResult>(json!({
            "request_id": "req",
            "surface_id": "surface",
            "action_id": "action",
            "state": "accepted",
            "tree_update": {"kind": "replacement", "ref_id": "old"}
        }))
        .is_err()
    );
}

#[test]
fn dialog_visibility_uses_scoped_presentation_presence_and_equality() {
    let mut dialog = node(
        UiNodeKind::Dialog,
        json!({ "title": "Create ticket", "presentation": "auto" }),
    );
    dialog
        .slots
        .insert("body".to_string(), vec![text("Dialog body")]);
    let presence = UiBindIf::PresentationIf {
        predicate: UiPresentationPredicate::Present {
            key: UiPresentationKey("create-ticket".to_string()),
        },
        node: Box::new(dialog),
    };
    let equality = UiBindIf::PresentationIf {
        predicate: UiPresentationPredicate::Equals {
            key: UiPresentationKey("selected-workspace".to_string()),
            value: json!("workspace-alpha"),
        },
        node: Box::new(text_node("Selected workspace")),
    };

    for binding in [presence, equality] {
        let value = serde_json::to_value(&binding).expect("serialize presentation binding");
        let round_trip =
            serde_json::from_value::<UiBindIf>(value).expect("deserialize presentation binding");
        assert_eq!(round_trip, binding);
        validate_bind_if_for_test(&binding);
    }

    assert_error_contains(
        node(
            UiNodeKind::Dialog,
            json!({ "title": "Legacy", "open": true }),
        ),
        "open",
    );

    let empty_key = UiBindIf::PresentationIf {
        predicate: UiPresentationPredicate::Present {
            key: UiPresentationKey(" ".to_string()),
        },
        node: Box::new(text_node("Hidden")),
    };
    let mut root = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    root.children.push(UiChild::BindIf(empty_key));
    assert_eq!(
        root.validate(),
        Err(UiValidationError::Node {
            id: Some(UiNodeId("stack".to_string()).into()),
            kind: UiNodeKind::Stack,
            source: Box::new(UiValidationError::InvalidBindPath {
                path: " ".to_string(),
                reason: "presentation key cannot be empty".to_string(),
            }),
        })
    );
}

#[test]
fn form_requires_explicit_nonblank_submit_label() {
    assert_error_contains(
        node(
            UiNodeKind::Form,
            json!({ "action": { "id": "ticket.create" } }),
        ),
        "submit_label",
    );
    assert_error_contains(
        node(
            UiNodeKind::Form,
            json!({
                "action": { "id": "ticket.create" },
                "submit_label": " "
            }),
        ),
        "cannot be empty",
    );
}

fn validate_bind_if_for_test(binding: &UiBindIf) {
    let mut root = node(UiNodeKind::Stack, json!({ "direction": "vertical" }));
    root.children.push(UiChild::BindIf(binding.clone()));
    root.validate()
        .expect("presentation binding should validate");
}

#[test]
fn crate_root_form_schema_types_validate_a_form_field() {
    let schema = botster_ui_contract::UiFieldSchema {
        kind: botster_ui_contract::UiFieldKind::Select,
        name: "status".to_string(),
        label: "Status".to_string(),
        description: Some("Workflow state".to_string()),
        placeholder: None,
        required: true,
        default: Some(json!("open")),
        validation: Some(botster_ui_contract::UiFieldValidationHints {
            one_of: vec![json!("open")],
            ..Default::default()
        }),
        options: vec![botster_ui_contract::UiFieldOption {
            value: json!("open"),
            label: "Open".to_string(),
            disabled: false,
        }],
    };

    let field = botster_ui_contract::UiNode {
        kind: botster_ui_contract::UiNodeKind::FormField,
        id: Some(botster_ui_contract::UiNodeId("status-field".to_string()).into()),
        props: Map::from_iter([(
            "schema".to_string(),
            serde_json::to_value(schema).expect("serialize schema"),
        )]),
        children: Vec::new(),
        slots: BTreeMap::new(),
    };

    botster_ui_contract::validate_ui_node(&field)
        .expect("module import should validate form field");
}

#[test]
fn crate_root_iframe_policy_types_use_the_wire_vocabulary() {
    let bridge = UiIframeBridge {
        actions: vec![UiActionId("vault.graph.open_note".to_string())],
        messages: vec!["vault.graph.ready".to_string()],
    };
    assert_eq!(
        serde_json::to_value(bridge).expect("serialize iframe bridge"),
        json!({
            "actions": ["vault.graph.open_note"],
            "messages": ["vault.graph.ready"]
        })
    );
    assert_eq!(
        serde_json::to_value(UiIframeSandboxToken::AllowScripts)
            .expect("serialize iframe sandbox token"),
        json!("allow_scripts")
    );
    assert_eq!(
        serde_json::to_value(botster_ui_contract::UiIframePermission::ClipboardWrite)
            .expect("serialize iframe permission"),
        json!("clipboard_write")
    );
    assert_eq!(
        serde_json::from_value::<UiIframePermission>(json!("fullscreen"))
            .expect("deserialize iframe permission"),
        botster_ui_contract::UiIframePermission::Fullscreen
    );
}
