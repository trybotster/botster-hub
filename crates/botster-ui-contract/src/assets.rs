use crate::{
    EntityOptionsFrame, EntityRecordItem, PackageSurfaceKind, PackageSurfaceOperation,
    UiActionKind, UiActionResultState, UiCapabilityFallback, UiColorToken, UiDensity,
    UiDialogPresentation, UiEntityOptionsExclude, UiEntityOptionsKind, UiEntityOptionsSource,
    UiFieldKind, UiHeightClass, UiIframePermission, UiIframeSandboxToken, UiMetricTrendDirection,
    UiNodeKind, UiOrientation, UiPointer, UiSelectionMode, UiSpaceToken, UiTableColumnAlign,
    UiToolbarOverflow, UiVariant, UiWidthClass, apply_entity_options_frame,
    project_entity_options_from_store,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

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
        "$id": "https://trybotster.dev/schemas/ui-contract-0.3.2.json",
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
            "UiBind": {
                "type": "object",
                "additionalProperties": false,
                "required": ["$bind"],
                "properties": {
                    "$bind": { "type": "string", "pattern": "^(/|@/)" }
                }
            },
            "UiBindableString": {
                "oneOf": [
                    { "type": "string", "pattern": "\\S" },
                    { "$ref": "#/$defs/UiBind" }
                ]
            },
            "UiAuthoredTextValue": {
                "oneOf": [
                    { "$ref": "#/$defs/UiBind" },
                    { "not": { "type": "object", "required": ["$bind"] } }
                ]
            },
            "UiNonBindableValue": {
                "not": { "type": "object", "required": ["$bind"] }
            },
            "UiNodeId": { "type": "string" },
            "UiAuthoredNodeId": {
                "oneOf": [
                    { "$ref": "#/$defs/UiNodeId" },
                    {
                        "description": "Schema validation is necessary but not sufficient: the Rust/Hub validator admits a bound id only on the direct UiBindList.item_template root, where row context exists.",
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["$bind"],
                        "properties": {
                            "$bind": { "type": "string", "pattern": "^@/.+" }
                        }
                    },
                    {
                        "description": "Valid only on identity-bearing descendants below a UiBindList.item_template root whose id is an item-relative binding. Keys are nonblank and unique across the complete authored item template.",
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["$kind", "key"],
                        "properties": {
                            "$kind": { "const": "bind_list_descendant_id" },
                            "key": { "type": "string", "pattern": "\\S" }
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
                    required_non_bindable_props_schema("stack", &["direction"]),
                    required_non_bindable_props_schema("form_section", &["title"]),
                    required_non_bindable_props_schema("form_field", &["schema"]),
                    required_non_bindable_props_schema("metric", &["label", "value"]),
                    required_non_bindable_props_schema("status_badge", &["label"]),
                    required_non_bindable_props_schema("icon", &["icon"]),
                    required_non_bindable_props_schema("badge", &["label"]),
                    required_non_bindable_props_schema("status_dot", &["label"]),
                    required_non_bindable_props_schema("empty_state", &["title"]),
                    required_non_bindable_props_schema("table", &["columns"]),
                    required_non_bindable_props_schema("terminal_view", &["session_id"]),
                    required_non_bindable_props_schema("connection_code_view", &["code"]),
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
                                        "submit_label": { "$ref": "#/$defs/UiBindableString" }
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
                                        "title": { "$ref": "#/$defs/UiNonBindableValue" },
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
                                        "label": { "$ref": "#/$defs/UiBindableString" },
                                        "action": { "$ref": "#/$defs/UiAction" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "icon_button" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["label", "icon", "action"],
                                    "properties": {
                                        "label": { "$ref": "#/$defs/UiBindableString" },
                                        "icon": { "$ref": "#/$defs/UiNonBindableValue" },
                                        "action": { "$ref": "#/$defs/UiAction" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "menu_item" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["label", "action"],
                                    "properties": {
                                        "label": { "$ref": "#/$defs/UiBindableString" },
                                        "action": { "$ref": "#/$defs/UiAction" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "text" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["text"],
                                    "properties": {
                                        "text": { "$ref": "#/$defs/UiAuthoredTextValue" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "iframe" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["src", "title"],
                                    "properties": {
                                        "src": { "$ref": "#/$defs/UiBindableString" },
                                        "title": { "$ref": "#/$defs/UiBindableString" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": {
                                "type": { "enum": ["text_input", "textarea", "checkbox"] }
                            },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["name", "label"],
                                    "properties": {
                                        "name": { "$ref": "#/$defs/UiNonBindableValue" },
                                        "label": { "$ref": "#/$defs/UiNonBindableValue" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "select" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["name", "label"],
                                    "properties": {
                                        "name": { "$ref": "#/$defs/UiNonBindableValue" },
                                        "label": { "$ref": "#/$defs/UiNonBindableValue" },
                                        "options_source": { "$ref": "#/$defs/UiEntityOptionsSource" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "select_option" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["value", "label"],
                                    "properties": {
                                        "value": { "$ref": "#/$defs/UiNonBindableValue" },
                                        "label": { "$ref": "#/$defs/UiNonBindableValue" }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "if": {
                            "properties": { "type": { "const": "custom" } },
                            "required": ["type"]
                        },
                        "then": {
                            "required": ["props"],
                            "properties": {
                                "props": {
                                    "type": "object",
                                    "required": ["namespace", "component", "reason"],
                                    "properties": {
                                        "namespace": { "type": "string" },
                                        "component": { "type": "string" },
                                        "reason": { "type": "string" }
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
            "UiEntityOptionsExclude": {
                "type": "object",
                "additionalProperties": false,
                "required": ["source", "value_field"],
                "properties": {
                    "source": { "type": "string", "pattern": "^/[^/]+$" },
                    "value_field": { "type": "string", "pattern": "\\S" },
                    "where": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    }
                }
            },
            "UiEntityOptionsSource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["$kind", "source", "value_field", "display_fields", "order"],
                "properties": {
                    "$kind": { "const": "entity_options" },
                    "source": { "type": "string", "pattern": "^/[^/]+$" },
                    "value_field": { "type": "string", "pattern": "\\S" },
                    "display_fields": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "type": "string", "pattern": "\\S" }
                    },
                    "order": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "type": "string", "pattern": "\\S" }
                    },
                    "where": {
                        "type": "object",
                        "additionalProperties": { "type": "string" }
                    },
                    "exclude": { "$ref": "#/$defs/UiEntityOptionsExclude" }
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

fn entity_options_reactive_timeline_fixture() -> Value {
    let descriptor = UiEntityOptionsSource {
        kind: UiEntityOptionsKind::EntityOptions,
        source: "/session".to_string(),
        value_field: "session_uuid".to_string(),
        display_fields: vec![
            "label".to_string(),
            "lifecycle_class".to_string(),
            "session_type".to_string(),
            "spawn_point".to_string(),
        ],
        order: vec!["label".to_string(), "session_uuid".to_string()],
        r#where: BTreeMap::from([("lifecycle_class".to_string(), json!("current"))]),
        exclude: Some(UiEntityOptionsExclude {
            source: "/project-pipelines.run".to_string(),
            value_field: "session_uuid".to_string(),
            r#where: BTreeMap::from([("status".to_string(), json!("active"))]),
        }),
    };

    let selection = "sess-alpha";
    let mut store = crate::EntityFamilyStore::new();
    let mut timeline = Vec::new();

    let mut push_step =
        |name: &str, frames: Vec<EntityOptionsFrame>, store: &mut crate::EntityFamilyStore| {
            for frame in &frames {
                apply_entity_options_frame(store, frame);
            }
            let projection = project_entity_options_from_store(&descriptor, store, Some(selection));
            timeline.push(json!({
                "name": name,
                "frames": frames,
                "expected_store": store,
                "expected_projection": projection,
            }));
        };

    push_step(
        "source_snapshot",
        vec![EntityOptionsFrame::Snapshot {
            entity_type: "session".to_string(),
            snapshot_seq: 1,
            resync_reason: None,
            items: vec![
                entity_item(
                    "sess-alpha",
                    json!({
                        "session_uuid": "sess-alpha",
                        "label": "Alpha",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
                entity_item(
                    "sess-bravo",
                    json!({
                        "session_uuid": "sess-bravo",
                        "label": "Bravo",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
                entity_item(
                    "sess-stale",
                    json!({
                        "session_uuid": "sess-stale",
                        "label": "Stale",
                        "lifecycle_class": "exited",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
            ],
        }],
        &mut store,
    );

    push_step(
        "exclude_snapshot",
        vec![EntityOptionsFrame::Snapshot {
            entity_type: "project-pipelines.run".to_string(),
            snapshot_seq: 1,
            resync_reason: None,
            items: vec![entity_item(
                "run-1",
                json!({
                    "id": "run-1",
                    "session_uuid": "sess-bravo",
                    "status": "active"
                }),
            )],
        }],
        &mut store,
    );

    push_step(
        "source_upsert",
        vec![EntityOptionsFrame::Upsert {
            entity_type: "session".to_string(),
            id: "sess-charlie".to_string(),
            seq: 2,
            fields: object_fields(json!({
                "session_uuid": "sess-charlie",
                "label": "Charlie",
                "lifecycle_class": "current",
                "session_type": "agent",
                "spawn_point": "remote"
            })),
        }],
        &mut store,
    );

    push_step(
        "source_patch",
        vec![EntityOptionsFrame::Patch {
            entity_type: "session".to_string(),
            id: "sess-alpha".to_string(),
            seq: 3,
            fields: object_fields(json!({ "label": "Alpha Z" })),
        }],
        &mut store,
    );

    push_step(
        "source_remove",
        vec![EntityOptionsFrame::Remove {
            entity_type: "session".to_string(),
            id: "sess-charlie".to_string(),
            seq: 4,
        }],
        &mut store,
    );

    push_step(
        "exclude_remove",
        vec![EntityOptionsFrame::Remove {
            entity_type: "project-pipelines.run".to_string(),
            id: "run-1".to_string(),
            seq: 2,
        }],
        &mut store,
    );

    push_step(
        "duplicate_values",
        vec![EntityOptionsFrame::Snapshot {
            entity_type: "session".to_string(),
            snapshot_seq: 5,
            resync_reason: None,
            items: vec![
                entity_item(
                    "row-a",
                    json!({
                        "session_uuid": "dup-value",
                        "label": "Zulu",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
                entity_item(
                    "row-b",
                    json!({
                        "session_uuid": "dup-value",
                        "label": "Alpha",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
            ],
        }],
        &mut store,
    );

    push_step(
        "unicode_labels",
        vec![EntityOptionsFrame::Snapshot {
            entity_type: "session".to_string(),
            snapshot_seq: 6,
            resync_reason: None,
            items: vec![
                entity_item(
                    "sess-cafe",
                    json!({
                        "session_uuid": "sess-café",
                        "label": "café",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
                entity_item(
                    "sess-jp",
                    json!({
                        "session_uuid": "sess-会話",
                        "label": "会話-😀",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
            ],
        }],
        &mut store,
    );

    push_step(
        "reconnect_snapshot",
        vec![
            EntityOptionsFrame::Snapshot {
                entity_type: "session".to_string(),
                snapshot_seq: 10,
                resync_reason: Some("reconnect".to_string()),
                items: vec![entity_item(
                    "sess-alpha",
                    json!({
                        "session_uuid": "sess-alpha",
                        "label": "Alpha",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                )],
            },
            EntityOptionsFrame::Snapshot {
                entity_type: "project-pipelines.run".to_string(),
                snapshot_seq: 10,
                resync_reason: Some("reconnect".to_string()),
                items: vec![],
            },
        ],
        &mut store,
    );

    push_step(
        "gap_recovery_snapshot",
        vec![EntityOptionsFrame::Snapshot {
            entity_type: "session".to_string(),
            snapshot_seq: 20,
            resync_reason: Some("gap".to_string()),
            items: vec![
                entity_item(
                    "sess-alpha",
                    json!({
                        "session_uuid": "sess-alpha",
                        "label": "Alpha",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
                entity_item(
                    "sess-delta",
                    json!({
                        "session_uuid": "sess-delta",
                        "label": "Delta",
                        "lifecycle_class": "current",
                        "session_type": "agent",
                        "spawn_point": "local"
                    }),
                ),
            ],
        }],
        &mut store,
    );

    push_step(
        "selection_invalid",
        vec![EntityOptionsFrame::Remove {
            entity_type: "session".to_string(),
            id: "sess-alpha".to_string(),
            seq: 21,
        }],
        &mut store,
    );

    let sample_node = json!({
        "type": "select",
        "id": "session-select",
        "props": {
            "name": "session",
            "label": "Session",
            "options_source": descriptor
        }
    });
    let sample_ui: crate::UiNode =
        serde_json::from_value(sample_node.clone()).expect("sample select node");
    let collector_from_sample_node = crate::collect_entity_option_families(&sample_ui);

    json!({
        "descriptor": descriptor,
        "selection": selection,
        "sample_node": sample_node,
        "collector_vectors": [
            { "authored_path": "/session", "subscription_id": "session" },
            { "authored_path": "/project-pipelines.run", "subscription_id": "project-pipelines.run" },
            {
                "authored_path": "/bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run",
                "subscription_id": "bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run"
            },
            { "authored_path": "/session/sess-1/label", "subscription_id": null },
            { "authored_path": "session", "subscription_id": null }
        ],
        "collector_from_sample_node": collector_from_sample_node,
        "timeline": timeline
    })
}

fn entity_item(id: &str, fields: Value) -> EntityRecordItem {
    EntityRecordItem {
        id: id.to_string(),
        fields: object_fields(fields),
    }
}

fn object_fields(value: Value) -> Map<String, Value> {
    value
        .as_object()
        .cloned()
        .expect("entity fields must be an object")
}

/// Generate renderer-neutral fixtures from the Rust-owned wire vocabulary.
#[must_use]
pub fn conformance_fixtures_json() -> Value {
    json!({
        "contract_version": "0.3.2",
        "bind_list_descendant_identity_vectors": [
            {
                "row": "session-1",
                "key": "remove",
                "realized_id": "botster-ui-descendant-v1:9:session-16:remove"
            },
            {
                "row": "1:ab",
                "key": "23:c",
                "realized_id": "botster-ui-descendant-v1:4:1:ab4:23:c"
            },
            {
                "row": "  row  ",
                "key": " spaced key ",
                "realized_id": "botster-ui-descendant-v1:7:  row  12: spaced key "
            },
            {
                "row": "café",
                "key": "重命名",
                "realized_id": "botster-ui-descendant-v1:5:café9:重命名"
            },
            {
                "row": "会話-😀",
                "key": "remove-🧹",
                "realized_id": "botster-ui-descendant-v1:11:会話-😀11:remove-🧹"
            },
            {
                "row": "botster-ui-descendant-v1:1:x",
                "key": "0:prefix:9",
                "realized_id": "botster-ui-descendant-v1:28:botster-ui-descendant-v1:1:x10:0:prefix:9"
            }
        ],
        "entity_options_reactive_timeline": entity_options_reactive_timeline_fixture(),
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
                        "type": "inline",
                        "id": { "$bind": "@/session_uuid" },
                        "children": [{
                            "type": "button",
                            "id": { "$kind": "bind_list_descendant_id", "key": "select" },
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
                        }]
                    }
                }]
            },
            "required_bindable_fields": {
                "authored": [
                    { "type": "button", "id": "bound-button", "props": { "label": { "$bind": "@/lifecycle_class" }, "action": { "id": "contract.action" } } },
                    { "type": "icon_button", "id": "bound-icon-button", "props": { "label": { "$bind": "@/lifecycle_class" }, "icon": "play", "action": { "id": "contract.action" } } },
                    { "type": "menu_item", "id": "bound-menu-item", "props": { "label": { "$bind": "@/lifecycle_class" }, "action": { "id": "contract.action" } } },
                    { "type": "form", "id": "bound-form", "props": { "action": { "id": "contract.submit" }, "submit_label": { "$bind": "@/lifecycle_class" } } },
                    { "type": "iframe", "id": "bound-iframe-src", "props": { "src": { "$bind": "@/url" }, "title": "Session" } },
                    { "type": "iframe", "id": "bound-iframe-title", "props": { "src": "/session.html", "title": { "$bind": "@/lifecycle_class" } } },
                    { "type": "text", "id": "bound-text", "props": { "text": { "$bind": "@/lifecycle_class" } } }
                ],
                "realized": [
                    { "type": "button", "id": "bound-button", "props": { "label": "current", "action": { "id": "contract.action" } } },
                    { "type": "icon_button", "id": "bound-icon-button", "props": { "label": "current", "icon": "play", "action": { "id": "contract.action" } } },
                    { "type": "menu_item", "id": "bound-menu-item", "props": { "label": "current", "action": { "id": "contract.action" } } },
                    { "type": "form", "id": "bound-form", "props": { "action": { "id": "contract.submit" }, "submit_label": "current" } },
                    { "type": "iframe", "id": "bound-iframe-src", "props": { "src": "/session.html", "title": "Session" } },
                    { "type": "iframe", "id": "bound-iframe-title", "props": { "src": "/session.html", "title": "current" } },
                    { "type": "text", "id": "bound-text", "props": { "text": "current" } }
                ]
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

fn required_non_bindable_props_schema(kind: &str, required_props: &[&str]) -> Value {
    let properties = required_props
        .iter()
        .map(|prop| {
            (
                (*prop).to_string(),
                json!({ "$ref": "#/$defs/UiNonBindableValue" }),
            )
        })
        .collect::<serde_json::Map<String, Value>>();

    json!({
        "if": {
            "properties": { "type": { "const": kind } },
            "required": ["type"]
        },
        "then": {
            "required": ["props"],
            "properties": {
                "props": {
                    "type": "object",
                    "required": required_props,
                    "properties": properties
                }
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
export declare const packageVersion: string;
export declare const schema: JsonObject;
export declare const conformanceFixtures: JsonObject;
export declare function realizeBindListDescendantId(rowId: string, key: string): UiNodeId;
export declare function projectEntityOptions(
  descriptor: UiEntityOptionsSource,
  sourceRecords: Record<string, JsonObject>,
  excludeRecords: Record<string, JsonObject>,
  selection?: string | null,
): EntityOptionsProjection;
export declare function collectEntityOptionFamilies(node: JsonObject): string[];
export declare function entityFamilySubscriptionId(authoredPath: string): string | null;
export type UiBindListDescendantId = { $kind: "bind_list_descendant_id"; key: string };
export type UiEntityOptionsKind = "entity_options";
export interface UiEntityOptionsExclude { source: string; value_field: string; where?: Record<string, string>; }
export interface UiEntityOptionsSource { $kind: UiEntityOptionsKind; source: string; value_field: string; display_fields: string[]; order: string[]; where?: Record<string, string>; exclude?: UiEntityOptionsExclude; }
export interface EntityOption { value: string; label: string; metadata?: Record<string, string>; }
export interface EntityOptionsProjection { options: EntityOption[]; selection_valid: boolean; }
export type UiAuthoredNodeId = UiNodeId | UiBind | UiBindListDescendantId;
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
export type UiBindableString = string | UiBind;
export type UiAuthoredTextValue = JsonValue | UiBind;
export type UiNonBindableValue = null | boolean | number | string | JsonValue[] | ({ [key: string]: JsonValue } & { $bind?: never });
export type UiRequiredNonBindableProps<Fields extends string> = JsonObject & Record<Fields, UiNonBindableValue>;
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
export type UiFormProps = JsonObject & { action: UiAction; submit_label: UiBindableString };
export type UiDialogProps = JsonObject & { title: UiNonBindableValue; presentation?: UiDialogPresentation; open?: never };
export type UiButtonProps = JsonObject & { label: UiBindableString; action: UiAction };
export type UiIconButtonProps = JsonObject & { label: UiBindableString; icon: UiNonBindableValue; action: UiAction };
export type UiMenuItemProps = JsonObject & { label: UiBindableString; action: UiAction };
export type UiTextProps = JsonObject & { text: UiAuthoredTextValue };
export type UiIframeProps = JsonObject & { src: UiBindableString; title: UiBindableString };
export type UiFieldControlProps = JsonObject & { name: UiNonBindableValue; label: UiNonBindableValue };
export type UiSelectProps = JsonObject & { name: UiNonBindableValue; label: UiNonBindableValue; options_source?: UiEntityOptionsSource };
export type UiSelectOptionProps = JsonObject & { value: UiNonBindableValue; label: UiNonBindableValue };
export type UiCustomProps = JsonObject & { namespace: string; component: string; reason: string };
export interface UiNodeBase { id?: UiAuthoredNodeId; children?: UiChild[]; slots?: Record<string, UiChild[]>; }
export type UiNode =
  | (UiNodeBase & { type: "stack"; props: UiRequiredNonBindableProps<"direction"> })
  | (UiNodeBase & { type: "form"; props: UiFormProps })
  | (UiNodeBase & { type: "form_section"; props: UiRequiredNonBindableProps<"title"> })
  | (UiNodeBase & { type: "form_field"; props: UiRequiredNonBindableProps<"schema"> })
  | (UiNodeBase & { type: "metric"; props: UiRequiredNonBindableProps<"label" | "value"> })
  | (UiNodeBase & { type: "status_badge"; props: UiRequiredNonBindableProps<"label"> })
  | (UiNodeBase & { type: "icon"; props: UiRequiredNonBindableProps<"icon"> })
  | (UiNodeBase & { type: "badge"; props: UiRequiredNonBindableProps<"label"> })
  | (UiNodeBase & { type: "status_dot"; props: UiRequiredNonBindableProps<"label"> })
  | (UiNodeBase & { type: "empty_state"; props: UiRequiredNonBindableProps<"title"> })
  | (UiNodeBase & { type: "table"; props: UiRequiredNonBindableProps<"columns"> })
  | (UiNodeBase & { type: "dialog"; props: UiDialogProps })
  | (UiNodeBase & { type: "button"; props: UiButtonProps })
  | (UiNodeBase & { type: "icon_button"; props: UiIconButtonProps })
  | (UiNodeBase & { type: "menu_item"; props: UiMenuItemProps })
  | (UiNodeBase & { type: "text"; props: UiTextProps })
  | (UiNodeBase & { type: "iframe"; props: UiIframeProps })
  | (UiNodeBase & { type: "text_input" | "textarea" | "checkbox"; props: UiFieldControlProps })
  | (UiNodeBase & { type: "select"; props: UiSelectProps })
  | (UiNodeBase & { type: "select_option"; props: UiSelectOptionProps })
  | (UiNodeBase & { type: "terminal_view"; props: UiRequiredNonBindableProps<"session_id"> })
  | (UiNodeBase & { type: "connection_code_view"; props: UiRequiredNonBindableProps<"code"> })
  | (UiNodeBase & { type: "custom"; props: UiCustomProps })
  | (UiNodeBase & { type: Exclude<UiNodeKind, "stack" | "form" | "form_section" | "form_field" | "metric" | "status_badge" | "icon" | "badge" | "status_dot" | "empty_state" | "table" | "dialog" | "button" | "icon_button" | "menu_item" | "text" | "iframe" | "text_input" | "textarea" | "checkbox" | "select" | "select_option" | "terminal_view" | "connection_code_view" | "custom">; props?: JsonObject });
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
