use crate::{
    PackageSurfaceKind, PackageSurfaceOperation, UiActionKind, UiActionResultState,
    UiCapabilityFallback, UiColorToken, UiDensity, UiDialogPresentation, UiFieldKind,
    UiHeightClass, UiIframePermission, UiIframeSandboxToken, UiMetricTrendDirection, UiNodeKind,
    UiOrientation, UiPointer, UiSelectionMode, UiSpaceToken, UiTableColumnAlign, UiToolbarOverflow,
    UiVariant, UiWidthClass,
};
use serde::Serialize;
use serde_json::{Value, json};

trait WireEnum: Copy + Serialize + 'static {
    fn variants() -> &'static [Self];
    fn assert_exhaustive(self);
}

macro_rules! wire_enum {
    ($enum:ty => [$($variant:path),+ $(,)?]) => {
        impl WireEnum for $enum {
            fn variants() -> &'static [Self] {
                &[$($variant),+]
            }

            fn assert_exhaustive(self) {
                match self {
                    $($variant => {}),+
                }
            }
        }
    };
}

wire_enum!(UiNodeKind => [
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
]);
wire_enum!(UiWidthClass => [UiWidthClass::Compact, UiWidthClass::Regular, UiWidthClass::Expanded]);
wire_enum!(UiHeightClass => [UiHeightClass::Short, UiHeightClass::Regular, UiHeightClass::Tall]);
wire_enum!(UiPointer => [UiPointer::None, UiPointer::Coarse, UiPointer::Fine]);
wire_enum!(UiOrientation => [UiOrientation::Portrait, UiOrientation::Landscape]);
wire_enum!(UiDialogPresentation => [
    UiDialogPresentation::Auto,
    UiDialogPresentation::Inline,
    UiDialogPresentation::Overlay,
    UiDialogPresentation::Sheet,
    UiDialogPresentation::Fullscreen,
]);
wire_enum!(UiCapabilityFallback => [
    UiCapabilityFallback::TableAsList,
    UiCapabilityFallback::DialogInline,
    UiCapabilityFallback::TerminalSelectionDisabled,
    UiCapabilityFallback::ConnectionCodeText,
    UiCapabilityFallback::IframeAsLink,
    UiCapabilityFallback::RichColorMuted,
    UiCapabilityFallback::ContextMenuAsMenu,
    UiCapabilityFallback::ClipboardManual,
    UiCapabilityFallback::HoverPersistentHints,
]);
wire_enum!(UiSpaceToken => [
    UiSpaceToken::None,
    UiSpaceToken::Xs,
    UiSpaceToken::Sm,
    UiSpaceToken::Md,
    UiSpaceToken::Lg,
    UiSpaceToken::Xl,
]);
wire_enum!(UiColorToken => [
    UiColorToken::Default,
    UiColorToken::Muted,
    UiColorToken::Accent,
    UiColorToken::Success,
    UiColorToken::Warning,
    UiColorToken::Danger,
]);
wire_enum!(UiFieldKind => [
    UiFieldKind::Text,
    UiFieldKind::Textarea,
    UiFieldKind::Checkbox,
    UiFieldKind::Select,
]);
wire_enum!(UiIframeSandboxToken => [
    UiIframeSandboxToken::AllowForms,
    UiIframeSandboxToken::AllowModals,
    UiIframeSandboxToken::AllowPopups,
    UiIframeSandboxToken::AllowSameOrigin,
    UiIframeSandboxToken::AllowScripts,
    UiIframeSandboxToken::AllowDownloads,
]);
wire_enum!(UiIframePermission => [
    UiIframePermission::Fullscreen,
    UiIframePermission::ClipboardWrite,
    UiIframePermission::Camera,
    UiIframePermission::Microphone,
    UiIframePermission::Geolocation,
    UiIframePermission::Payment,
]);
wire_enum!(UiDensity => [UiDensity::Compact, UiDensity::Regular, UiDensity::Spacious]);
wire_enum!(UiVariant => [UiVariant::Plain, UiVariant::Subtle, UiVariant::Emphasized]);
wire_enum!(UiToolbarOverflow => [
    UiToolbarOverflow::Auto,
    UiToolbarOverflow::Never,
    UiToolbarOverflow::Always,
]);
wire_enum!(UiMetricTrendDirection => [
    UiMetricTrendDirection::Up,
    UiMetricTrendDirection::Down,
    UiMetricTrendDirection::Flat,
]);
wire_enum!(UiSelectionMode => [
    UiSelectionMode::None,
    UiSelectionMode::Single,
    UiSelectionMode::Multiple,
]);
wire_enum!(UiTableColumnAlign => [
    UiTableColumnAlign::Start,
    UiTableColumnAlign::Center,
    UiTableColumnAlign::End,
]);
wire_enum!(UiActionKind => [
    UiActionKind::Submit,
    UiActionKind::Reset,
    UiActionKind::Validate,
    UiActionKind::Cancel,
]);
wire_enum!(UiActionResultState => [
    UiActionResultState::Accepted,
    UiActionResultState::Rejected,
    UiActionResultState::Deferred,
    UiActionResultState::Error,
]);
wire_enum!(PackageSurfaceKind => [
    PackageSurfaceKind::App,
    PackageSurfaceKind::Settings,
    PackageSurfaceKind::DashboardWidget,
    PackageSurfaceKind::Diagnostics,
]);
wire_enum!(PackageSurfaceOperation => [
    PackageSurfaceOperation::Render,
    PackageSurfaceOperation::Action,
]);

fn wire_names<T: WireEnum>() -> Vec<String> {
    T::variants()
        .iter()
        .copied()
        .map(|variant| {
            variant.assert_exhaustive();
            serde_json::to_value(variant)
                .expect("UI wire enum must serialize")
                .as_str()
                .expect("UI wire enum must serialize as a string")
                .to_string()
        })
        .collect()
}

fn typescript_union<T: WireEnum>() -> String {
    wire_names::<T>()
        .into_iter()
        .map(|variant| format!("\"{variant}\""))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Generate serde-shaped TypeScript declarations for the public UI contract.
#[must_use]
pub fn typescript_declarations() -> String {
    let mut declarations = TYPESCRIPT.trim_start().to_string();
    macro_rules! replace_union {
        ($name:literal, $enum:ty) => {
            declarations =
                declarations.replace(concat!("__", $name, "__"), &typescript_union::<$enum>());
        };
    }
    replace_union!("UiNodeKind", UiNodeKind);
    replace_union!("UiWidthClass", UiWidthClass);
    replace_union!("UiHeightClass", UiHeightClass);
    replace_union!("UiPointer", UiPointer);
    replace_union!("UiOrientation", UiOrientation);
    replace_union!("UiDialogPresentation", UiDialogPresentation);
    replace_union!("UiCapabilityFallback", UiCapabilityFallback);
    replace_union!("UiSpaceToken", UiSpaceToken);
    replace_union!("UiColorToken", UiColorToken);
    replace_union!("UiFieldKind", UiFieldKind);
    replace_union!("UiIframeSandboxToken", UiIframeSandboxToken);
    replace_union!("UiIframePermission", UiIframePermission);
    replace_union!("UiDensity", UiDensity);
    replace_union!("UiVariant", UiVariant);
    replace_union!("UiToolbarOverflow", UiToolbarOverflow);
    replace_union!("UiMetricTrendDirection", UiMetricTrendDirection);
    replace_union!("UiSelectionMode", UiSelectionMode);
    replace_union!("UiTableColumnAlign", UiTableColumnAlign);
    replace_union!("UiActionKind", UiActionKind);
    replace_union!("UiActionResultState", UiActionResultState);
    replace_union!("PackageSurfaceKind", PackageSurfaceKind);
    replace_union!("PackageSurfaceOperation", PackageSurfaceOperation);
    declarations
}

/// Generate the machine-readable schema shipped by `@trybotster/ui-contract`.
#[must_use]
pub fn json_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://trybotster.dev/schemas/ui-contract-0.2.0.json",
        "title": "Botster UI Contract",
        "oneOf": [
            { "$ref": "#/$defs/UiNode" },
            { "$ref": "#/$defs/UiActionRequest" },
            { "$ref": "#/$defs/UiActionResult" },
            { "$ref": "#/$defs/PackageSurfaceDescriptor" },
            { "$ref": "#/$defs/PackageNavigationEntry" }
        ],
        "$defs": {
            "JsonValue": {},
            "UiNodeId": { "type": "string" },
            "UiAuthoredNodeId": {
                "oneOf": [
                    { "$ref": "#/$defs/UiNodeId" },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["$bind"],
                        "properties": {
                            "$bind": { "type": "string", "pattern": "^@/.+" }
                        }
                    }
                ]
            },
            "UiActionId": { "type": "string" },
            "UiSurfaceId": { "type": "string" },
            "UiActionRequestId": { "type": "string" },
            "UiPresentationKey": { "type": "string", "minLength": 1 },
            "UiNodeKind": {
                "enum": wire_names::<UiNodeKind>()
            },
            "UiActionKind": {
                "enum": wire_names::<UiActionKind>()
            },
            "UiActionResultState": {
                "enum": wire_names::<UiActionResultState>()
            },
            "PackageSurfaceKind": {
                "enum": wire_names::<PackageSurfaceKind>()
            },
            "PackageSurfaceOperation": {
                "enum": wire_names::<PackageSurfaceOperation>()
            },
            "PackageSurfaceDescriptor": {
                "type": "object",
                "additionalProperties": false,
                "required": ["id", "kind", "title"],
                "properties": {
                    "id": { "type": "string", "pattern": "\\S" },
                    "kind": { "$ref": "#/$defs/PackageSurfaceKind" },
                    "title": { "type": "string" },
                    "description": { "type": "string" },
                    "icon": { "type": "string" },
                    "order": { "type": "integer" },
                    "category": { "type": "string" },
                    "supports": {
                        "type": "array",
                        "uniqueItems": true,
                        "items": { "$ref": "#/$defs/PackageSurfaceOperation" }
                    }
                }
            },
            "PackageNavigationTarget": {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "surface_id"],
                "properties": {
                    "kind": { "const": "surface" },
                    "surface_id": { "type": "string", "pattern": "\\S" }
                }
            },
            "PackageNavigationEntry": {
                "type": "object",
                "additionalProperties": false,
                "required": ["id", "label", "target"],
                "properties": {
                    "id": { "type": "string", "pattern": "\\S" },
                    "label": { "type": "string" },
                    "icon": { "type": "string" },
                    "description": { "type": "string" },
                    "target": { "$ref": "#/$defs/PackageNavigationTarget" }
                }
            },
            "UiWidthClass": { "enum": wire_names::<UiWidthClass>() },
            "UiHeightClass": { "enum": wire_names::<UiHeightClass>() },
            "UiPointer": { "enum": wire_names::<UiPointer>() },
            "UiOrientation": { "enum": wire_names::<UiOrientation>() },
            "UiDialogPresentation": { "enum": wire_names::<UiDialogPresentation>() },
            "UiCapabilityFallback": { "enum": wire_names::<UiCapabilityFallback>() },
            "UiSpaceToken": { "enum": wire_names::<UiSpaceToken>() },
            "UiColorToken": { "enum": wire_names::<UiColorToken>() },
            "UiFieldKind": { "enum": wire_names::<UiFieldKind>() },
            "UiIframeSandboxToken": { "enum": wire_names::<UiIframeSandboxToken>() },
            "UiIframePermission": { "enum": wire_names::<UiIframePermission>() },
            "UiDensity": { "enum": wire_names::<UiDensity>() },
            "UiVariant": { "enum": wire_names::<UiVariant>() },
            "UiToolbarOverflow": { "enum": wire_names::<UiToolbarOverflow>() },
            "UiMetricTrendDirection": { "enum": wire_names::<UiMetricTrendDirection>() },
            "UiSelectionMode": { "enum": wire_names::<UiSelectionMode>() },
            "UiTableColumnAlign": { "enum": wire_names::<UiTableColumnAlign>() },
            "UiPresentationOperation": {
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["kind", "key", "value"],
                        "properties": {
                            "kind": { "const": "set" },
                            "key": { "$ref": "#/$defs/UiPresentationKey" },
                            "value": { "$ref": "#/$defs/JsonValue" }
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["kind", "key"],
                        "properties": {
                            "kind": { "enum": ["clear", "toggle"] },
                            "key": { "$ref": "#/$defs/UiPresentationKey" }
                        }
                    }
                ]
            },
            "UiPresentationPredicate": {
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["kind", "key"],
                        "properties": {
                            "kind": { "enum": ["present", "truthy"] },
                            "key": { "$ref": "#/$defs/UiPresentationKey" }
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["kind", "key", "value"],
                        "properties": {
                            "kind": { "const": "equals" },
                            "key": { "$ref": "#/$defs/UiPresentationKey" },
                            "value": { "$ref": "#/$defs/JsonValue" }
                        }
                    }
                ]
            },
            "UiAction": {
                "type": "object",
                "additionalProperties": false,
                "required": ["id"],
                "properties": {
                    "id": { "$ref": "#/$defs/UiActionId" },
                    "payload": { "$ref": "#/$defs/JsonValue" },
                    "disabled": { "type": "boolean" }
                }
            },
            "UiNode": {
                "type": "object",
                "additionalProperties": false,
                "required": ["type"],
                "properties": {
                    "type": { "$ref": "#/$defs/UiNodeKind" },
                    "id": { "$ref": "#/$defs/UiAuthoredNodeId" },
                    "props": { "type": "object", "additionalProperties": { "$ref": "#/$defs/JsonValue" } },
                    "children": { "type": "array", "items": { "$ref": "#/$defs/UiChild" } },
                    "slots": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "array",
                            "items": { "$ref": "#/$defs/UiChild" }
                        }
                    }
                },
                "allOf": [
                    {
                        "if": {
                            "properties": { "type": { "const": "form" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["action", "submit_label"],
                                    "properties": {
                                        "action": { "$ref": "#/$defs/UiAction" },
                                        "submit_label": {
                                            "type": "string",
                                            "pattern": "\\S"
                                        }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "dialog" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["title"],
                                    "properties": {
                                        "title": { "type": "string" },
                                        "presentation": {
                                            "$ref": "#/$defs/UiDialogPresentation"
                                        }
                                    },
                                    "not": { "required": ["open"] }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "button" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["label", "action"],
                                    "properties": {
                                        "label": { "type": "string" },
                                        "action": { "$ref": "#/$defs/UiAction" }
                                    }
                                }
                            }
                        }
                    }
                ]
            },
            "UiChild": {
                "oneOf": [
                    { "$ref": "#/$defs/UiNode" },
                    { "$ref": "#/$defs/UiConditional" },
                    { "$ref": "#/$defs/UiBindList" },
                    { "$ref": "#/$defs/UiBindIf" }
                ]
            },
            "UiConditional": {
                "type": "object",
                "required": ["$kind", "condition", "node"],
                "properties": {
                    "$kind": { "enum": ["when", "hidden"] },
                    "condition": { "type": "object" },
                    "node": { "$ref": "#/$defs/UiNode" }
                }
            },
            "UiBindList": {
                "type": "object",
                "required": ["$kind", "source", "item_template"],
                "properties": {
                    "$kind": { "const": "bind_list" },
                    "source": { "type": "string" },
                    "where": { "type": "object" },
                    "item_template": { "$ref": "#/$defs/UiNode" },
                    "empty_template": { "$ref": "#/$defs/UiNode" }
                }
            },
            "UiBindIf": {
                "oneOf": [
                    {
                        "type": "object",
                        "required": ["$kind", "path", "node"],
                        "properties": {
                            "$kind": { "const": "bind_if" },
                            "path": { "type": "string" },
                            "node": { "$ref": "#/$defs/UiNode" }
                        }
                    },
                    {
                        "type": "object",
                        "required": ["$kind", "predicate", "node"],
                        "properties": {
                            "$kind": { "const": "presentation_if" },
                            "predicate": { "$ref": "#/$defs/UiPresentationPredicate" },
                            "node": { "$ref": "#/$defs/UiNode" }
                        }
                    }
                ]
            },
            "UiActionRequest": {
                "type": "object",
                "additionalProperties": false,
                "required": ["request_id", "surface_id", "action_id", "kind"],
                "properties": {
                    "request_id": { "$ref": "#/$defs/UiActionRequestId" },
                    "surface_id": { "$ref": "#/$defs/UiSurfaceId" },
                    "action_id": { "$ref": "#/$defs/UiActionId" },
                    "node_id": { "$ref": "#/$defs/UiNodeId" },
                    "kind": { "$ref": "#/$defs/UiActionKind" },
                    "values": { "type": "object", "additionalProperties": { "$ref": "#/$defs/JsonValue" } },
                    "payload": { "$ref": "#/$defs/JsonValue" }
                }
            },
            "UiActionResult": {
                "type": "object",
                "additionalProperties": false,
                "required": ["request_id", "surface_id", "action_id", "state"],
                "properties": {
                    "request_id": { "$ref": "#/$defs/UiActionRequestId" },
                    "surface_id": { "$ref": "#/$defs/UiSurfaceId" },
                    "action_id": { "$ref": "#/$defs/UiActionId" },
                    "node_id": { "$ref": "#/$defs/UiNodeId" },
                    "state": { "$ref": "#/$defs/UiActionResultState" },
                    "field_errors": {
                        "type": "object",
                        "additionalProperties": { "type": "array", "items": { "type": "string" } }
                    },
                    "form_errors": { "type": "array", "items": { "type": "string" } },
                    "warnings": { "type": "array", "items": { "type": "string" } },
                    "normalized_values": { "type": "object" },
                    "presentation": {
                        "type": "array",
                        "items": { "$ref": "#/$defs/UiPresentationOperation" }
                    },
                    "replacement": { "$ref": "#/$defs/UiNode" },
                    "payload": { "$ref": "#/$defs/JsonValue" },
                    "error": { "type": "string" }
                },
                "allOf": [{
                    "if": {
                        "properties": { "state": { "not": { "const": "accepted" } } },
                        "required": ["state"]
                    },
                    "then": {
                        "not": {
                            "anyOf": [
                                { "required": ["presentation"] },
                                { "required": ["replacement"] }
                            ]
                        }
                    }
                }]
            }
        }
    })
}

/// Generate renderer-neutral fixtures from the Rust-owned wire vocabulary.
#[must_use]
pub fn conformance_fixtures_json() -> Value {
    json!({
        "contract_version": "0.2.0",
        "fixtures": {
            "package_presentation": {
                "surfaces": [{
                    "id": "tickets",
                    "kind": "app",
                    "title": "Tickets",
                    "supports": ["render", "action"]
                }],
                "navigation": [{
                    "id": "tickets",
                    "label": "Tickets",
                    "target": { "kind": "surface", "surface_id": "tickets" }
                }]
            },
            "dialog_presence": {
                "$kind": "presentation_if",
                "predicate": { "kind": "present", "key": "create-ticket-dialog" },
                "node": {
                    "type": "dialog",
                    "id": "create-ticket-dialog",
                    "props": { "title": "Create ticket", "presentation": "auto" },
                    "slots": {
                        "body": [{ "type": "text", "props": { "text": "Dialog body" } }]
                    }
                }
            },
            "selected_workspace_equality": {
                "$kind": "presentation_if",
                "predicate": {
                    "kind": "equals",
                    "key": "selected-workspace",
                    "value": "workspace-alpha"
                },
                "node": {
                    "type": "text",
                    "props": { "text": "Selected workspace" }
                }
            },
            "form": {
                "type": "form",
                "id": "ticket-form",
                "props": {
                    "action": {
                        "id": "ticket.create",
                        "payload": { "source": "toolbar" }
                    },
                    "submit_label": "Create ticket"
                },
                "children": [{
                    "type": "text_input",
                    "id": "ticket-title",
                    "props": {
                        "name": "title",
                        "label": "Title",
                        "placeholder": "Ticket title"
                    }
                }]
            },
            "bound_row_identity": {
                "type": "panel",
                "id": "session-list",
                "children": [{
                    "$kind": "bind_list",
                    "source": "/session",
                    "where": { "lifecycle_class": "current" },
                    "item_template": {
                        "type": "button",
                        "id": { "$bind": "@/session_uuid" },
                        "props": {
                            "label": "Select session",
                            "action": {
                                "id": "contract.action",
                                "payload": {
                                    "operation": "select_session",
                                    "session_uuid": { "$bind": "@/session_uuid" }
                                }
                            }
                        }
                    }
                }]
            },
            "request": {
                "request_id": "request-1",
                "surface_id": "tickets.create",
                "action_id": "ticket.create",
                "node_id": "ticket-form",
                "kind": "submit",
                "values": { "title": "Ship contract" },
                "payload": { "source": "toolbar" }
            },
            "accepted": {
                "request_id": "request-1",
                "surface_id": "tickets.create",
                "action_id": "ticket.create",
                "node_id": "ticket-form",
                "state": "accepted",
                "presentation": [
                    { "kind": "set", "key": "notice", "value": "created" },
                    { "kind": "toggle", "key": "details" },
                    { "kind": "clear", "key": "create-ticket-dialog" }
                ],
                "replacement": {
                    "type": "text",
                    "id": "ticket-created",
                    "props": { "text": "Ticket created" }
                }
            },
            "rejected": {
                "request_id": "request-1",
                "surface_id": "tickets.create",
                "action_id": "ticket.create",
                "node_id": "ticket-form",
                "state": "rejected",
                "field_errors": { "ticket-title": ["Title is required"] },
                "form_errors": ["Fix the highlighted fields"],
                "normalized_values": { "title": "" }
            }
        }
    })
}

const TYPESCRIPT: &str = r#"
// Generated from botster-ui-contract Rust serde DTOs.
// Regenerate/check with: cargo run -p botster-ui-contract --example generate_assets

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type UiNodeId = string;
export type UiAuthoredNodeId = UiNodeId | UiBind;
export type UiActionId = string;
export type UiSurfaceId = string;
export type UiActionRequestId = string;
export type UiPresentationKey = string;
export type PackageSurfaceKind = __PackageSurfaceKind__;
export type PackageSurfaceOperation = __PackageSurfaceOperation__;
export interface PackageSurfaceDescriptor { id: string; kind: PackageSurfaceKind; title: string; description?: string; icon?: string; order?: number; category?: string; supports?: PackageSurfaceOperation[]; }
export type PackageNavigationTarget = { kind: "surface"; surface_id: string };
export interface PackageNavigationEntry { id: string; label: string; icon?: string; description?: string; target: PackageNavigationTarget; }
export type UiNodeKind = __UiNodeKind__;
export type UiWidthClass = __UiWidthClass__;
export type UiHeightClass = __UiHeightClass__;
export type UiPointer = __UiPointer__;
export type UiOrientation = __UiOrientation__;
export interface UiViewport { widthClass: UiWidthClass; heightClass: UiHeightClass; pointer: UiPointer; orientation?: UiOrientation; keyboardOccluded?: boolean; }
export interface UiKeyboardCapability { textEntry?: boolean; shortcuts?: boolean; focusTraversal?: boolean; }
export type UiDialogPresentation = __UiDialogPresentation__;
export type UiCapabilityFallback = __UiCapabilityFallback__;
export interface UiCapabilitySet { widthClasses?: UiWidthClass[]; heightClasses?: UiHeightClass[]; pointer: UiPointer; keyboard: UiKeyboardCapability; hover?: boolean; clipboard?: boolean; contextMenu?: boolean; dialogPresentations?: UiDialogPresentation[]; table?: boolean; terminalSelection?: boolean; qrCode?: boolean; iframe?: boolean; richColor?: boolean; fallbacks?: UiCapabilityFallback[]; }
export type UiSpaceToken = __UiSpaceToken__;
export type UiColorToken = __UiColorToken__;
export interface UiBind { $bind: string; }
export type UiPresentationOperation = { kind: "set"; key: UiPresentationKey; value: JsonValue } | { kind: "clear"; key: UiPresentationKey } | { kind: "toggle"; key: UiPresentationKey };
export type UiPresentationPredicate = { kind: "present"; key: UiPresentationKey } | { kind: "truthy"; key: UiPresentationKey } | { kind: "equals"; key: UiPresentationKey; value: JsonValue };
export interface UiResponsiveWidth { compact?: JsonValue; regular?: JsonValue; expanded?: JsonValue; }
export interface UiResponsiveHeight { short?: JsonValue; regular?: JsonValue; tall?: JsonValue; }
export type UiResponsiveValue = { $kind: "responsive"; width?: UiResponsiveWidth; height?: UiResponsiveHeight };
export interface UiCondition { width?: UiWidthClass; height?: UiHeightClass; pointer?: UiPointer; orientation?: UiOrientation; keyboardOccluded?: boolean; }
export type UiConditional = { $kind: "when"; condition: UiCondition; node: UiNode } | { $kind: "hidden"; condition: UiCondition; node: UiNode };
export type UiBindList = { $kind: "bind_list"; source: string; where?: Record<string, JsonValue>; item_template: UiNode; empty_template?: UiNode };
export type UiBindIf = { $kind: "bind_if"; path: string; node: UiNode } | { $kind: "presentation_if"; predicate: UiPresentationPredicate; node: UiNode };
export type UiChild = UiConditional | UiNode | UiBindList | UiBindIf;
export type UiFormProps = JsonObject & { action: UiAction; submit_label: string };
export type UiDialogProps = JsonObject & { title: string; presentation?: UiDialogPresentation; open?: never };
export type UiButtonProps = JsonObject & { label: string; action: UiAction };
export interface UiNodeBase { id?: UiAuthoredNodeId; children?: UiChild[]; slots?: Record<string, UiChild[]>; }
export type UiNode =
  | (UiNodeBase & { type: "form"; props: UiFormProps })
  | (UiNodeBase & { type: "dialog"; props: UiDialogProps })
  | (UiNodeBase & { type: "button"; props: UiButtonProps })
  | (UiNodeBase & { type: Exclude<UiNodeKind, "form" | "dialog" | "button">; props?: JsonObject });
export type UiFieldKind = __UiFieldKind__;
export interface UiFieldOption { value: JsonValue; label: string; disabled?: boolean; }
export interface UiFieldValidationHints { minLength?: number; maxLength?: number; pattern?: string; min?: number; max?: number; oneOf?: JsonValue[]; }
export interface UiFieldSchema { kind: UiFieldKind; name: string; label: string; description?: string; placeholder?: string; required?: boolean; default?: JsonValue; validation?: UiFieldValidationHints; options?: UiFieldOption[]; }
export type UiIframeSandboxToken = __UiIframeSandboxToken__;
export type UiIframePermission = __UiIframePermission__;
export interface UiIframeBridge { actions?: UiActionId[]; messages?: string[]; }
export type UiAction = { id: UiActionId; payload?: JsonValue; disabled?: boolean };
export type UiDensity = __UiDensity__;
export type UiVariant = __UiVariant__;
export type UiToolbarOverflow = __UiToolbarOverflow__;
export type UiMetricTrendDirection = __UiMetricTrendDirection__;
export interface UiMetricTrend { direction: UiMetricTrendDirection; value?: JsonValue; label?: string; }
export type UiSelectionMode = __UiSelectionMode__;
export interface UiSelection { mode: UiSelectionMode; selected?: string[]; }
export type UiTableColumnAlign = __UiTableColumnAlign__;
export interface UiTableColumnDescriptor { id: string; label?: string; align?: UiTableColumnAlign; }
export type UiTableColumn = string | UiTableColumnDescriptor;
export type UiTableCell = UiNode | JsonValue;
export interface UiTableRow { id: string; cells?: Record<string, UiTableCell>; action?: UiAction; }
export type UiActionKind = __UiActionKind__;
export type UiFormValues = JsonObject;
export interface UiActionRequest { request_id: UiActionRequestId; surface_id: UiSurfaceId; action_id: UiActionId; node_id?: UiNodeId; kind: UiActionKind; values?: UiFormValues; payload?: JsonValue; }
export type UiActionResultState = __UiActionResultState__;
export type UiFieldErrors = Record<string, string[]>;
export interface UiActionResult { request_id: UiActionRequestId; surface_id: UiSurfaceId; action_id: UiActionId; node_id?: UiNodeId; state: UiActionResultState; field_errors?: UiFieldErrors; form_errors?: string[]; warnings?: string[]; normalized_values?: UiFormValues; presentation?: UiPresentationOperation[]; replacement?: UiNode; payload?: JsonValue; error?: string; }
"#;
