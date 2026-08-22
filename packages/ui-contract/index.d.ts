// Generated from botster-ui-contract Rust serde DTOs.
// Regenerate/check with: cargo run -p botster-ui-contract --example generate_assets

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type UiNodeId = string;
export declare const packageVersion: string;
export declare const NOTICE_TEXT_MAX_BYTES: number;
export declare const schema: JsonObject;
export declare const conformanceFixtures: JsonObject;
export declare function realizeBindListDescendantId(rowId: string, key: string): UiNodeId;
export declare function resolveNoticeText(payload: JsonValue, pointer: string): string;
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
export type PackageSurfaceKind = "app" | "settings" | "dashboard_widget" | "diagnostics";
export type PackageSurfaceOperation = "render" | "action";
export interface PackageSurfaceDescriptor { id: string; kind: PackageSurfaceKind; title: string; description?: string; icon?: string; order?: number; category?: string; supports?: PackageSurfaceOperation[]; }
export type PackageNavigationTarget = { kind: "surface"; surface_id: string };
export interface PackageNavigationEntry { id: string; label: string; icon?: string; description?: string; target: PackageNavigationTarget; }
export type PackageNoticeSubjectScope = "session";
export type PackageNoticeSeverity = "info" | "warning" | "error";
export interface PackageNoticeReactionDeclaration { owner?: string; name: string; subject_scope: PackageNoticeSubjectScope; text_pointer: string; ttl_ms: number; severity: PackageNoticeSeverity; }
export interface PackageNoticeReactionDescriptor { owner: string; name: string; subject_scope: PackageNoticeSubjectScope; text_pointer: string; ttl_ms: number; severity: PackageNoticeSeverity; }
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
export type UiFieldKind = "text" | "textarea" | "checkbox" | "select";
export interface UiFieldOption { value: JsonValue; label: string; disabled?: boolean; }
export interface UiFieldValidationHints { minLength?: number; maxLength?: number; pattern?: string; min?: number; max?: number; oneOf?: JsonValue[]; }
export interface UiFieldSchema { kind: UiFieldKind; name: string; label: string; description?: string; placeholder?: string; required?: boolean; default?: JsonValue; validation?: UiFieldValidationHints; options?: UiFieldOption[]; }
export type UiIframeSandboxToken = "allow_forms" | "allow_modals" | "allow_popups" | "allow_same_origin" | "allow_scripts" | "allow_downloads";
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
