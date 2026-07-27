use serde_json::{Value, json};

/// Generate serde-shaped TypeScript declarations for the public UI contract.
#[must_use]
pub fn typescript_declarations() -> String {
    TYPESCRIPT.trim_start().to_string()
}

/// Generate the machine-readable schema shipped by `@trybotster/ui-contract`.
#[must_use]
pub fn json_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://trybotster.dev/schemas/ui-contract-0.1.0.json",
        "title": "Botster UI Contract",
        "oneOf": [
            { "$ref": "#/$defs/UiNode" },
            { "$ref": "#/$defs/UiActionRequest" },
            { "$ref": "#/$defs/UiActionResult" }
        ],
        "$defs": {
            "JsonValue": {},
            "UiNodeId": { "type": "string" },
            "UiActionId": { "type": "string" },
            "UiSurfaceId": { "type": "string" },
            "UiActionRequestId": { "type": "string" },
            "UiPresentationKey": { "type": "string", "minLength": 1 },
            "UiNodeKind": {
                "enum": NODE_KINDS
            },
            "UiActionKind": {
                "enum": ["submit", "reset", "validate", "cancel"]
            },
            "UiActionResultState": {
                "enum": ["accepted", "rejected", "deferred", "error"]
            },
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
                    "id": { "$ref": "#/$defs/UiNodeId" },
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
                                            "enum": ["auto", "inline", "overlay", "sheet", "fullscreen"]
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
        "contract_version": "0.1.0",
        "fixtures": {
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

const NODE_KINDS: &[&str] = &[
    "stack",
    "inline",
    "form",
    "form_section",
    "form_field",
    "panel",
    "metric",
    "metric_grid",
    "toolbar",
    "status_badge",
    "section",
    "scroll_area",
    "text",
    "icon",
    "badge",
    "status_dot",
    "empty_state",
    "list",
    "list_item",
    "tree",
    "tree_item",
    "table",
    "button",
    "icon_button",
    "menu",
    "menu_item",
    "dialog",
    "text_input",
    "textarea",
    "checkbox",
    "select",
    "select_option",
    "terminal_view",
    "connection_code_view",
    "iframe",
    "custom",
];

const TYPESCRIPT: &str = r#"
// Generated from botster-ui-contract Rust serde DTOs.
// Regenerate/check with: cargo run -p botster-ui-contract --example generate_assets

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type UiNodeId = string;
export type UiActionId = string;
export type UiSurfaceId = string;
export type UiActionRequestId = string;
export type UiPresentationKey = string;
export type UiNodeKind = "stack" | "inline" | "form" | "form_section" | "form_field" | "panel" | "metric" | "metric_grid" | "toolbar" | "status_badge" | "section" | "scroll_area" | "text" | "icon" | "badge" | "status_dot" | "empty_state" | "list" | "list_item" | "tree" | "tree_item" | "table" | "button" | "icon_button" | "menu" | "menu_item" | "dialog" | "text_input" | "textarea" | "checkbox" | "select" | "select_option" | "terminal_view" | "connection_code_view" | "iframe" | "custom";
export type UiWidthClass = "compact" | "regular" | "expanded";
export type UiHeightClass = "short" | "regular" | "tall";
export type UiPointer = "none" | "coarse" | "fine";
export type UiOrientation = "portrait" | "landscape";
export interface UiViewport { widthClass: UiWidthClass; heightClass: UiHeightClass; pointer: UiPointer; orientation?: UiOrientation; keyboardOccluded?: boolean; }
export interface UiKeyboardCapability { textEntry?: boolean; shortcuts?: boolean; focusTraversal?: boolean; }
export type UiDialogPresentation = "auto" | "inline" | "overlay" | "sheet" | "fullscreen";
export type UiCapabilityFallback = "table_as_list" | "dialog_inline" | "terminal_selection_disabled" | "connection_code_text" | "iframe_as_link" | "rich_color_muted" | "context_menu_as_menu" | "clipboard_manual" | "hover_persistent_hints";
export interface UiCapabilitySet { widthClasses?: UiWidthClass[]; heightClasses?: UiHeightClass[]; pointer: UiPointer; keyboard: UiKeyboardCapability; hover?: boolean; clipboard?: boolean; contextMenu?: boolean; dialogPresentations?: UiDialogPresentation[]; table?: boolean; terminalSelection?: boolean; qrCode?: boolean; iframe?: boolean; richColor?: boolean; fallbacks?: UiCapabilityFallback[]; }
export type UiSpaceToken = "none" | "xs" | "sm" | "md" | "lg" | "xl";
export type UiColorToken = "default" | "muted" | "accent" | "success" | "warning" | "danger";
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
export interface UiNodeBase { id?: UiNodeId; children?: UiChild[]; slots?: Record<string, UiChild[]>; }
export type UiNode =
  | (UiNodeBase & { type: "form"; props: UiFormProps })
  | (UiNodeBase & { type: "dialog"; props: UiDialogProps })
  | (UiNodeBase & { type: "button"; props: UiButtonProps })
  | (UiNodeBase & { type: Exclude<UiNodeKind, "form" | "dialog" | "button">; props?: JsonObject });
export type UiFieldKind = "text" | "textarea" | "checkbox" | "select";
export interface UiFieldOption { value: JsonValue; label: string; disabled?: boolean; }
export interface UiFieldValidationHints { minLength?: number; maxLength?: number; pattern?: string; min?: number; max?: number; oneOf?: JsonValue[]; }
export interface UiFieldSchema { kind: UiFieldKind; name: string; label: string; description?: string; placeholder?: string; required?: boolean; default?: JsonValue; validation?: UiFieldValidationHints; options?: UiFieldOption[]; }
export type UiIframeSandboxToken = "allow_downloads" | "allow_forms" | "allow_modals" | "allow_orientation_lock" | "allow_pointer_lock" | "allow_popups" | "allow_popups_to_escape_sandbox" | "allow_presentation" | "allow_same_origin" | "allow_scripts" | "allow_top_navigation_by_user_activation";
export type UiIframePermission = "fullscreen" | "clipboard_write" | "camera" | "microphone" | "geolocation" | "payment";
export interface UiIframeBridge { actions?: UiActionId[]; messages?: string[]; }
export type UiAction = { id: UiActionId; payload?: JsonValue; disabled?: boolean };
export type UiDensity = "compact" | "regular" | "spacious";
export type UiVariant = "plain" | "subtle" | "emphasized";
export type UiToolbarOverflow = "auto" | "never" | "always";
export type UiMetricTrendDirection = "up" | "down" | "flat";
export interface UiMetricTrend { direction: UiMetricTrendDirection; value?: JsonValue; label?: string; }
export type UiSelectionMode = "none" | "single" | "multiple";
export interface UiSelection { mode: UiSelectionMode; selected?: string[]; }
export type UiTableColumnAlign = "start" | "center" | "end";
export interface UiTableColumnDescriptor { id: string; label?: string; align?: UiTableColumnAlign; }
export type UiTableColumn = string | UiTableColumnDescriptor;
export type UiTableCell = UiNode | JsonValue;
export interface UiTableRow { id: string; cells?: Record<string, UiTableCell>; action?: UiAction; }
export type UiActionKind = "submit" | "reset" | "validate" | "cancel";
export type UiFormValues = JsonObject;
export interface UiActionRequest { request_id: UiActionRequestId; surface_id: UiSurfaceId; action_id: UiActionId; node_id?: UiNodeId; kind: UiActionKind; values?: UiFormValues; payload?: JsonValue; }
export type UiActionResultState = "accepted" | "rejected" | "deferred" | "error";
export type UiFieldErrors = Record<string, string[]>;
export interface UiActionResult { request_id: UiActionRequestId; surface_id: UiSurfaceId; action_id: UiActionId; node_id?: UiNodeId; state: UiActionResultState; field_errors?: UiFieldErrors; form_errors?: string[]; warnings?: string[]; normalized_values?: UiFormValues; presentation?: UiPresentationOperation[]; replacement?: UiNode; payload?: JsonValue; error?: string; }
"#;
