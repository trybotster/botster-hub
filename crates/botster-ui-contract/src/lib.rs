#![recursion_limit = "256"]
//! Renderer-neutral UI node, binding, viewport, and action contracts.
//!
//! The UI contract is the standalone authority shared by Hub and clients:
//!
//! - **Kernel primitives** are renderer-portable nodes suitable for shared
//!   fallback paths: layout, text/content, actions, forms, collections, tables,
//!   dialogs, and shared field controls.
//! - **Application vocabulary** captures useful product-shaped semantics such
//!   as metrics, toolbars, panels, sections, and status badges. These remain
//!   shared vocabulary, but they are not the mandatory substrate for unknown
//!   custom components.
//! - **Host surface placeholders** such as terminal, connection-code, and
//!   iframe nodes identify host/client-owned rendering surfaces. `iframe` is
//!   the sanctioned full custom-app escape when sandboxed web content is the
//!   correct shape.
//! - **Custom** is a declarative, owner-classified escape hatch. It never loads
//!   renderer code or invokes plugin behavior; plugin-owned execution remains
//!   behind plugin worker/runtime boundaries. Unknown custom components degrade
//!   through their required kernel-or-iframe fallback slot. Recognizing
//!   renderers may consume component-specific custom props; non-recognizing
//!   clients must ignore those props and render the fallback. Core validates
//!   top-level `$bind` payload sentinels and treats nested custom payload data
//!   as package-owned.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use thiserror::Error;

mod assets;
mod entity_options;
mod notices;

pub use assets::{conformance_fixtures_json, json_schema, typescript_declarations};
pub use entity_options::{
    EntityFamilyStore, EntityOption, EntityOptionsFrame, EntityOptionsProjection, EntityRecordItem,
    UiEntityOptionsExclude, UiEntityOptionsKind, UiEntityOptionsSource, apply_entity_options_frame,
    apply_entity_options_frames, collect_entity_option_families, entity_family_subscription_id,
    project_entity_options, project_entity_options_from_store, validate_entity_options_source,
};
pub use notices::{
    NOTICE_TEXT_MAX_BYTES, NOTICE_TTL_MAX_MS, NOTICE_TTL_MIN_MS, NoticePointerError,
    NoticeTextError, PackageNoticeReactionDeclaration, PackageNoticeReactionDescriptor,
    PackageNoticeReactionValidationError, PackageNoticeSeverity, PackageNoticeSubjectScope,
    decode_notice_text_pointer, resolve_notice_text, validate_package_notice_reactions,
};

/// Transport-neutral UI surface metadata carried by a package manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageSurfaceDescriptor {
    /// Stable surface identifier within the package.
    pub id: String,
    /// Semantic surface kind.
    pub kind: PackageSurfaceKind,
    /// Human-readable surface title.
    pub title: String,
    /// Optional surface help text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional renderer-neutral icon or token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Legacy non-authoritative ordering hint kept for manifest compatibility.
    /// Hosts, users, and clients own actual navigation ordering policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<i64>,
    /// Optional host-readable category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Supported surface operations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supports: Vec<PackageSurfaceOperation>,
}

/// Semantic UI surface kinds a package can declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageSurfaceKind {
    /// Main application surface.
    App,
    /// Settings or preferences surface.
    Settings,
    /// Dashboard widget surface.
    DashboardWidget,
    /// Diagnostic or troubleshooting surface.
    Diagnostics,
}

/// Operations a client can perform for a declared package surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageSurfaceOperation {
    /// Client can render the surface.
    Render,
    /// Client can invoke actions for the surface.
    Action,
}

/// Package-authored navigation intent inspected by hosts without running
/// plugin code.
///
/// Navigation declares discoverability only. Hosts retain placement, ordering,
/// pinning, hiding, and admission policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageNavigationEntry {
    /// Stable navigation item identifier within the package.
    pub id: String,
    /// User-facing label for the navigation item.
    pub label: String,
    /// Optional renderer-neutral icon token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Optional descriptive help text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Host-resolved target for the navigation item.
    pub target: PackageNavigationTarget,
}

/// Host-resolved target for a package navigation entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PackageNavigationTarget {
    /// Target one package surface by stable surface id.
    Surface {
        /// Stable surface identifier within the same package.
        surface_id: String,
    },
}

/// Invalid package surface or navigation declaration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PackagePresentationValidationError {
    /// A stable identifier is empty or whitespace-only.
    #[error("{field} identifier must be non-empty")]
    EmptyId {
        /// Descriptor field containing the invalid identifier.
        field: &'static str,
    },
    /// More than one surface uses the same package-local identifier.
    #[error("duplicate package surface identifier `{id}`")]
    DuplicateSurfaceId {
        /// Duplicate package-local identifier.
        id: String,
    },
    /// More than one navigation entry uses the same package-local identifier.
    #[error("duplicate package navigation identifier `{id}`")]
    DuplicateNavigationId {
        /// Duplicate package-local identifier.
        id: String,
    },
    /// A navigation entry targets a surface the package did not declare.
    #[error("package navigation `{navigation_id}` targets undeclared surface `{surface_id}`")]
    UnknownNavigationSurface {
        /// Navigation entry containing the unresolved target.
        navigation_id: String,
        /// Missing package-local surface identifier.
        surface_id: String,
    },
    /// A surface repeats one operation declaration.
    #[error("package surface `{surface_id}` declares duplicate `{operation:?}` support")]
    DuplicateSurfaceOperation {
        /// Package-local surface identifier.
        surface_id: String,
        /// Repeated operation.
        operation: PackageSurfaceOperation,
    },
}

/// Validate one package's renderer-neutral surface and discoverability contract.
pub fn validate_package_presentation(
    surfaces: &[PackageSurfaceDescriptor],
    navigation: &[PackageNavigationEntry],
) -> Result<(), PackagePresentationValidationError> {
    let mut surface_ids = BTreeSet::new();
    for surface in surfaces {
        if surface.id.trim().is_empty() {
            return Err(PackagePresentationValidationError::EmptyId { field: "surface" });
        }
        if !surface_ids.insert(surface.id.as_str()) {
            return Err(PackagePresentationValidationError::DuplicateSurfaceId {
                id: surface.id.clone(),
            });
        }

        let mut operations = BTreeSet::new();
        for operation in &surface.supports {
            if !operations.insert(*operation) {
                return Err(
                    PackagePresentationValidationError::DuplicateSurfaceOperation {
                        surface_id: surface.id.clone(),
                        operation: *operation,
                    },
                );
            }
        }
    }

    let mut navigation_ids = BTreeSet::new();
    for entry in navigation {
        if entry.id.trim().is_empty() {
            return Err(PackagePresentationValidationError::EmptyId {
                field: "navigation",
            });
        }
        if !navigation_ids.insert(entry.id.as_str()) {
            return Err(PackagePresentationValidationError::DuplicateNavigationId {
                id: entry.id.clone(),
            });
        }
        match &entry.target {
            PackageNavigationTarget::Surface { surface_id } => {
                if surface_id.trim().is_empty() {
                    return Err(PackagePresentationValidationError::EmptyId {
                        field: "navigation surface target",
                    });
                }
                if !surface_ids.contains(surface_id.as_str()) {
                    return Err(
                        PackagePresentationValidationError::UnknownNavigationSurface {
                            navigation_id: entry.id.clone(),
                            surface_id: surface_id.clone(),
                        },
                    );
                }
            }
        }
    }

    Ok(())
}

/// Stable UI node identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UiNodeId(pub String);

/// Producer-authored key for identity-bearing descendants of a bound list row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "$kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UiBindListDescendantId {
    /// Compose the realized row identity with this stable control key.
    BindListDescendantId {
        /// Producer-owned key, unique within the complete item template.
        key: String,
    },
}

impl UiBindListDescendantId {
    /// Return the producer-authored control key.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::BindListDescendantId { key } => key,
        }
    }
}

/// Error returned when a bound-list descendant identity cannot be realized.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum UiBindListDescendantIdError {
    /// The realized row identity is blank.
    #[error("bound list row identity cannot be blank")]
    BlankRowId,
    /// The producer-authored descendant key is blank.
    #[error("bound list descendant identity key cannot be blank")]
    BlankKey,
}

/// Realize a descendant identity from the canonical row id and authored key.
pub fn realize_bind_list_descendant_id(
    row_id: &str,
    key: &str,
) -> Result<UiNodeId, UiBindListDescendantIdError> {
    if row_id.trim().is_empty() {
        return Err(UiBindListDescendantIdError::BlankRowId);
    }
    if key.trim().is_empty() {
        return Err(UiBindListDescendantIdError::BlankKey);
    }

    Ok(UiNodeId(format!(
        "botster-ui-descendant-v1:{}:{row_id}{}:{key}",
        row_id.len(),
        key.len()
    )))
}

/// Node identity as authored before any BindList row template is expanded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiAuthoredNodeId {
    /// Already-realized stable node identity.
    Literal(UiNodeId),
    /// Item-relative identity resolved from the current BindList row.
    Bind(UiBind),
    /// Identity composed from the nearest realized BindList row and a stable key.
    BindListDescendant(UiBindListDescendantId),
}

impl From<UiNodeId> for UiAuthoredNodeId {
    fn from(id: UiNodeId) -> Self {
        Self::Literal(id)
    }
}

impl UiAuthoredNodeId {
    /// Return the realized literal identity, if this value is not a binding.
    #[must_use]
    pub fn as_literal(&self) -> Option<&UiNodeId> {
        match self {
            Self::Literal(id) => Some(id),
            Self::Bind(_) | Self::BindListDescendant(_) => None,
        }
    }
}

/// Stable UI action identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UiActionId(pub String);

/// Stable UI surface identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UiSurfaceId(pub String);

/// Stable UI action request correlation identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UiActionRequestId(pub String);

/// Shared semantic UI node kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiNodeKind {
    /// Vertical or horizontal stack layout.
    Stack,
    /// Inline layout.
    Inline,
    /// Form container.
    Form,
    /// Form section grouping.
    FormSection,
    /// Schema-driven form field.
    FormField,
    /// Panel region.
    Panel,
    /// Application metric.
    Metric,
    /// Responsive metric collection.
    MetricGrid,
    /// Semantic command/filter/search/action container.
    Toolbar,
    /// Compact status display with state semantics.
    StatusBadge,
    /// Lightweight content grouping.
    Section,
    /// Scrollable region.
    ScrollArea,
    /// Text node.
    Text,
    /// Icon node.
    Icon,
    /// Badge node.
    Badge,
    /// Status dot node.
    StatusDot,
    /// Empty state node.
    EmptyState,
    /// List container.
    List,
    /// List item.
    ListItem,
    /// Tree container.
    Tree,
    /// Tree item.
    TreeItem,
    /// Table container.
    Table,
    /// Button/action node.
    Button,
    /// Icon-only button/action node.
    IconButton,
    /// Menu container.
    Menu,
    /// Menu item.
    MenuItem,
    /// Dialog node.
    Dialog,
    /// Text input node.
    TextInput,
    /// Textarea node.
    Textarea,
    /// Checkbox node.
    Checkbox,
    /// Select node.
    Select,
    /// Select option node.
    SelectOption,
    /// Terminal view placeholder.
    TerminalView,
    /// Connection-code view placeholder.
    ConnectionCodeView,
    /// Sandboxed iframe or webview placeholder for generated HTML surfaces.
    Iframe,
    /// Owner-namespaced custom component with a validated renderer fallback.
    Custom,
}

/// Semantic width class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UiWidthClass {
    /// Single-column or narrow content area.
    Compact,
    /// Standard content area.
    Regular,
    /// Wide split-pane content area.
    Expanded,
}

/// Semantic height class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UiHeightClass {
    /// Short cross-axis space.
    Short,
    /// Standard cross-axis space.
    Regular,
    /// Tall cross-axis space.
    Tall,
}

/// Semantic pointer precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UiPointer {
    /// No pointer input.
    None,
    /// Coarse pointer input.
    Coarse,
    /// Fine pointer input.
    Fine,
}

/// Semantic screen orientation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UiOrientation {
    /// Portrait orientation.
    Portrait,
    /// Landscape orientation.
    Landscape,
}

/// Renderer-neutral viewport context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiViewport {
    /// Content-area width class.
    pub width_class: UiWidthClass,
    /// Content-area height class.
    pub height_class: UiHeightClass,
    /// Pointer precision.
    pub pointer: UiPointer,
    /// Optional orientation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orientation: Option<UiOrientation>,
    /// Whether the software keyboard occludes the viewport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyboard_occluded: Option<bool>,
}

/// Renderer-supported keyboard behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiKeyboardCapability {
    /// Renderer can accept ordinary text entry.
    #[serde(default, skip_serializing_if = "is_false")]
    pub text_entry: bool,
    /// Renderer can expose keyboard shortcuts for semantic actions.
    #[serde(default, skip_serializing_if = "is_false")]
    pub shortcuts: bool,
    /// Renderer can move focus through interactive primitives.
    #[serde(default, skip_serializing_if = "is_false")]
    pub focus_traversal: bool,
}

/// Supported dialog presentation modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiDialogPresentation {
    /// Renderer chooses the best available presentation.
    Auto,
    /// Present inline in normal layout flow.
    Inline,
    /// Present as an overlay.
    Overlay,
    /// Present as a sheet.
    Sheet,
    /// Present as a fullscreen panel.
    Fullscreen,
}

/// Renderer-declared fallback when a primitive capability is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiCapabilityFallback {
    /// Render table content as a list-like structure.
    TableAsList,
    /// Present dialogs inline when richer presentation is unavailable.
    DialogInline,
    /// Render terminal views without selection affordances.
    TerminalSelectionDisabled,
    /// Render connection codes as text when QR rendering is unavailable.
    ConnectionCodeText,
    /// Render iframe/webview sources as links or text when embedded browsing is unavailable.
    IframeAsLink,
    /// Collapse rich color into semantic monochrome or muted styling.
    RichColorMuted,
    /// Expose context-menu actions through ordinary menu/action controls.
    ContextMenuAsMenu,
    /// Expose clipboard actions through manual copy/paste affordances.
    ClipboardManual,
    /// Render hover-only metadata persistently or behind explicit actions.
    HoverPersistentHints,
}

/// Renderer-neutral UI capability declaration.
///
/// This describes what a client renderer can support. Core validates contract
/// shape and declared downgrade handling; it does not choose visual fallbacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiCapabilitySet {
    /// Supported semantic width classes.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub width_classes: BTreeSet<UiWidthClass>,
    /// Supported semantic height classes.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub height_classes: BTreeSet<UiHeightClass>,
    /// Supported pointer precision.
    pub pointer: UiPointer,
    /// Keyboard behavior the renderer can provide.
    pub keyboard: UiKeyboardCapability,
    /// Renderer can expose hover-only affordances.
    #[serde(default, skip_serializing_if = "is_false")]
    pub hover: bool,
    /// Renderer can perform clipboard actions.
    #[serde(default, skip_serializing_if = "is_false")]
    pub clipboard: bool,
    /// Renderer can expose contextual action menus.
    #[serde(default, skip_serializing_if = "is_false")]
    pub context_menu: bool,
    /// Supported dialog presentation modes.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub dialog_presentations: BTreeSet<UiDialogPresentation>,
    /// Renderer can present table structure directly.
    #[serde(default, skip_serializing_if = "is_false")]
    pub table: bool,
    /// Renderer can support terminal text selection.
    #[serde(default, skip_serializing_if = "is_false")]
    pub terminal_selection: bool,
    /// Renderer can display QR or connection-code graphics.
    #[serde(default, skip_serializing_if = "is_false")]
    pub qr_code: bool,
    /// Renderer can embed sandboxed iframe/webview content.
    #[serde(default, skip_serializing_if = "is_false")]
    pub iframe: bool,
    /// Renderer can apply semantic color tokens beyond monochrome defaults.
    #[serde(default, skip_serializing_if = "is_false")]
    pub rich_color: bool,
    /// Declared deterministic fallback behavior for unsupported capabilities.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub fallbacks: BTreeSet<UiCapabilityFallback>,
}

impl UiCapabilitySet {
    /// Validate that a node can be rendered or downgraded by this capability set.
    pub fn validate_node(&self, node: &UiNode) -> Result<(), UiValidationError> {
        validate_ui_node_with_capabilities(node, self)
    }

    /// Validate that a realized node can be rendered or downgraded by this capability set.
    pub fn validate_realized_node(&self, node: &UiNode) -> Result<(), UiValidationError> {
        validate_ui_node_realized_with_capabilities(node, self)
    }

    fn supports_fallback(&self, fallback: UiCapabilityFallback) -> bool {
        self.fallbacks.contains(&fallback)
    }
}

/// Semantic spacing token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiSpaceToken {
    /// No spacing.
    None,
    /// Extra-small spacing.
    Xs,
    /// Small spacing.
    Sm,
    /// Medium spacing.
    Md,
    /// Large spacing.
    Lg,
    /// Extra-large spacing.
    Xl,
}

/// Semantic color token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiColorToken {
    /// Default foreground/background color.
    Default,
    /// Muted content color.
    Muted,
    /// Accent color.
    Accent,
    /// Success color.
    Success,
    /// Warning color.
    Warning,
    /// Danger color.
    Danger,
}

/// Binding path sentinel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiBind {
    /// Absolute entity path or item-relative path.
    pub path: String,
}

/// Client-local presentation-state key scoped by the active Hub/package surface.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UiPresentationKey(pub String);

/// Renderer-neutral mutation of scoped client-local presentation state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UiPresentationOperation {
    /// Set a local presentation value.
    Set {
        /// Surface-local key.
        key: UiPresentationKey,
        /// JSON value owned by the client-local presentation store.
        value: Value,
    },
    /// Remove a local presentation value.
    Clear {
        /// Surface-local key.
        key: UiPresentationKey,
    },
    /// Toggle the truthiness of a local presentation value.
    Toggle {
        /// Surface-local key.
        key: UiPresentationKey,
    },
}

/// Conditional predicate over scoped client-local presentation state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UiPresentationPredicate {
    /// Match when the key is present, regardless of its JSON value.
    Present {
        /// Surface-local key.
        key: UiPresentationKey,
    },
    /// Match when the key contains a truthy JSON value.
    Truthy {
        /// Surface-local key.
        key: UiPresentationKey,
    },
    /// Match when the key equals the authored JSON value.
    Equals {
        /// Surface-local key.
        key: UiPresentationKey,
        /// Authored comparison value.
        value: Value,
    },
}

impl Serialize for UiBind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = BTreeMap::new();
        map.insert("$bind", self.path.as_str());
        map.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UiBind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map = BTreeMap::<String, String>::deserialize(deserializer)?;
        match map.get("$bind") {
            Some(path) if map.len() == 1 => Ok(Self { path: path.clone() }),
            _ => Err(serde::de::Error::custom(
                "expected exactly one $bind string field",
            )),
        }
    }
}

/// Responsive value keyed by semantic width and height classes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$kind", rename_all = "lowercase")]
pub enum UiResponsiveValue {
    /// Viewport-dependent values.
    Responsive {
        /// Width values by semantic width class.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<UiResponsiveWidth>,
        /// Height values by semantic height class.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<UiResponsiveHeight>,
    },
}

/// Width responsive map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiResponsiveWidth {
    /// Compact width value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact: Option<Value>,
    /// Regular width value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regular: Option<Value>,
    /// Expanded width value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expanded: Option<Value>,
}

/// Height responsive map.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UiResponsiveHeight {
    /// Short height value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short: Option<Value>,
    /// Regular height value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regular: Option<Value>,
    /// Tall height value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tall: Option<Value>,
}

/// Viewport predicate used by conditional wrappers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiCondition {
    /// Width-class predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<UiWidthClass>,
    /// Height-class predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<UiHeightClass>,
    /// Pointer predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pointer: Option<UiPointer>,
    /// Orientation predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orientation: Option<UiOrientation>,
    /// Keyboard occlusion predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keyboard_occluded: Option<bool>,
}

/// Conditional child wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$kind", rename_all = "lowercase")]
pub enum UiConditional {
    /// Render the node only when the condition matches.
    When {
        /// Viewport predicate.
        condition: UiCondition,
        /// Wrapped node.
        node: Box<UiNode>,
    },
    /// Render the node only when the condition does not match.
    Hidden {
        /// Viewport predicate.
        condition: UiCondition,
        /// Wrapped node.
        node: Box<UiNode>,
    },
}

/// Entity-backed list binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$kind", rename_all = "snake_case")]
pub enum UiBindList {
    /// Render a node template once per matching entity.
    BindList {
        /// Entity family path.
        source: String,
        /// Exact top-level field filters.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[serde(rename = "where")]
        r#where: BTreeMap<String, Value>,
        /// Template for each entity row.
        item_template: Box<UiNode>,
        /// Template for an empty result.
        #[serde(skip_serializing_if = "Option::is_none")]
        empty_template: Option<Box<UiNode>>,
    },
}

/// Conditional node binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$kind", rename_all = "snake_case")]
pub enum UiBindIf {
    /// Render a node when the binding path is truthy.
    BindIf {
        /// Absolute entity path or item-relative path.
        path: String,
        /// Node to render.
        node: Box<UiNode>,
    },
    /// Render a node when scoped client-local presentation state matches.
    PresentationIf {
        /// Renderer-neutral predicate evaluated by the client.
        predicate: UiPresentationPredicate,
        /// Node whose presence is controlled by the predicate.
        node: Box<UiNode>,
    },
}

/// Child node entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiChild {
    /// Conditional wrapper child.
    Conditional(UiConditional),
    /// Static node child.
    Node(Box<UiNode>),
    /// Entity-backed list child.
    BindList(UiBindList),
    /// Conditional node child.
    BindIf(UiBindIf),
}

/// Shared UI node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiNode {
    /// Semantic primitive type.
    #[serde(rename = "type")]
    pub kind: UiNodeKind,
    /// Optional authored node identity.
    ///
    /// A row-relative binding is valid only on a BindList item template and
    /// must be resolved to a literal [`UiNodeId`] before renderer state or
    /// action dispatch uses the node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<UiAuthoredNodeId>,
    /// Semantic properties.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub props: Map<String, Value>,
    /// Positional child entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<UiChild>,
    /// Named slots for compound primitives.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: BTreeMap<String, Vec<UiChild>>,
}

impl UiNode {
    /// Validate an authored semantic UI tree recursively.
    pub fn validate(&self) -> Result<(), UiValidationError> {
        self.validate_authored()
    }

    /// Validate an authored semantic UI tree recursively before binding materialization.
    pub fn validate_authored(&self) -> Result<(), UiValidationError> {
        validate_ui_node_authored(self)
    }

    /// Validate a realized semantic UI tree recursively after binding materialization.
    pub fn validate_realized(&self) -> Result<(), UiValidationError> {
        validate_ui_node_realized(self)
    }

    /// Return the declared fallback for a validated custom component node.
    ///
    /// This is an accessor over the core contract shape only. Renderers still
    /// own presentation; core only exposes the validated static fallback node
    /// an unknown custom component should degrade to. Callers should validate
    /// the node, or its containing tree, before resolving the fallback. On a
    /// validated tree `None` means this is not a custom node; on an unvalidated
    /// tree `None` can also mean a malformed fallback slot.
    pub fn custom_fallback(&self) -> Option<&UiNode> {
        if self.kind != UiNodeKind::Custom {
            return None;
        }

        match self.slots.get("fallback")?.as_slice() {
            [UiChild::Node(node)] => Some(node),
            _ => None,
        }
    }
}

/// Validate one authored semantic UI node recursively.
pub fn validate_ui_node(node: &UiNode) -> Result<(), UiValidationError> {
    validate_ui_node_authored(node)
}

/// Validate one authored semantic UI node recursively before binding materialization.
pub fn validate_ui_node_authored(node: &UiNode) -> Result<(), UiValidationError> {
    validate_ui_node_in_context(
        node,
        UiValidationPhase::Authored,
        UiValidationContext::Static,
        &mut BTreeSet::new(),
    )
}

/// Validate one realized semantic UI node recursively after binding materialization.
pub fn validate_ui_node_realized(node: &UiNode) -> Result<(), UiValidationError> {
    validate_ui_node_in_context(
        node,
        UiValidationPhase::Realized,
        UiValidationContext::Static,
        &mut BTreeSet::new(),
    )
}

/// Validate one semantic UI node against renderer capabilities.
pub fn validate_ui_node_with_capabilities(
    node: &UiNode,
    capabilities: &UiCapabilitySet,
) -> Result<(), UiValidationError> {
    validate_ui_node_with_capabilities_in_phase(node, capabilities, UiValidationPhase::Authored)
}

/// Validate one realized semantic UI node against renderer capabilities.
pub fn validate_ui_node_realized_with_capabilities(
    node: &UiNode,
    capabilities: &UiCapabilitySet,
) -> Result<(), UiValidationError> {
    validate_ui_node_with_capabilities_in_phase(node, capabilities, UiValidationPhase::Realized)
}

fn validate_ui_node_with_capabilities_in_phase(
    node: &UiNode,
    capabilities: &UiCapabilitySet,
    phase: UiValidationPhase,
) -> Result<(), UiValidationError> {
    validate_node(
        node,
        phase,
        UiValidationContext::Static,
        &mut BTreeSet::new(),
    )
    .and_then(|()| validate_node_capabilities(node, capabilities))
    .map_err(|error| UiValidationError::Node {
        id: node.id.clone(),
        kind: node.kind,
        source: Box::new(error),
    })
}

/// Narrow v1 field kinds shared by form fields and input primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiFieldKind {
    /// Single-line text input.
    Text,
    /// Multi-line text input.
    Textarea,
    /// Boolean checkbox input.
    Checkbox,
    /// Select input backed by renderer-neutral options.
    Select,
}

/// Renderer-neutral field schema for schema-driven form fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiFieldSchema {
    /// Field primitive kind.
    pub kind: UiFieldKind,
    /// Submission/state name.
    pub name: String,
    /// User-facing label.
    pub label: String,
    /// Optional help or description text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional placeholder for text-like fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    /// Whether renderers should present the field as required.
    #[serde(default, skip_serializing_if = "is_false")]
    pub required: bool,
    /// Default value used to initialize renderer-local state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Validation hints for renderers and plugin authors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<UiFieldValidationHints>,
    /// Options for select fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<UiFieldOption>,
}

/// Renderer-neutral select option metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiFieldOption {
    /// Submitted option value.
    pub value: Value,
    /// User-facing option label.
    pub label: String,
    /// Whether renderers should present the option as disabled.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

/// Field validation metadata. Core validates the shape only; renderers and
/// plugins decide how to present or enforce these hints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiFieldValidationHints {
    /// Minimum string length hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_length: Option<u64>,
    /// Maximum string length hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u64>,
    /// Pattern hint string. Core does not compile or execute it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    /// Minimum numeric value hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Maximum numeric value hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// Renderer-neutral allowed-value hints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub one_of: Vec<Value>,
}

/// Sandboxed iframe/webview sandbox token.
///
/// Omitted or empty sandbox tokens mean the host should apply the most
/// restrictive sandbox posture. Core records intent only; hosts and clients
/// decide the runtime browser/webview policy they are willing to admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiIframeSandboxToken {
    /// Permit form submission inside the embedded document.
    AllowForms,
    /// Permit modal dialogs inside the embedded document.
    AllowModals,
    /// Permit popups from the embedded document.
    AllowPopups,
    /// Permit treating the embedded document as same-origin.
    AllowSameOrigin,
    /// Permit scripts inside the embedded document.
    AllowScripts,
    /// Permit downloads initiated by the embedded document.
    AllowDownloads,
}

/// Passive iframe/webview permission metadata.
///
/// These entries describe browser/webview feature policy intent only. Host
/// mediated Botster actions or message channels belong in [`UiIframeBridge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiIframePermission {
    /// Permit fullscreen presentation.
    Fullscreen,
    /// Permit clipboard writes.
    ClipboardWrite,
    /// Permit camera access if the host admits it.
    Camera,
    /// Permit microphone access if the host admits it.
    Microphone,
    /// Permit geolocation access if the host admits it.
    Geolocation,
    /// Permit payment APIs if the host admits them.
    Payment,
}

/// Host-mediated bridge metadata for iframe/webview content.
///
/// Omitted or empty bridge metadata means no Botster action or message bridge is
/// declared. Hosts still own admission and runtime wiring.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiIframeBridge {
    /// Botster action ids that iframe content may request through the host.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<UiActionId>,
    /// Host-defined message channel names the iframe may request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
}

/// Semantic UI action descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiAction {
    /// Semantic action id.
    pub id: UiActionId,
    /// Optional action payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Whether clients should present the action as disabled.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

/// Shared density intent for renderer-neutral application primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiDensity {
    /// Compact presentation.
    Compact,
    /// Standard presentation.
    Regular,
    /// Roomier presentation.
    Spacious,
}

/// Shared variant intent for renderer-neutral application primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiVariant {
    /// Plain grouping with no framing requirement.
    Plain,
    /// Subtle grouping or low-emphasis treatment.
    Subtle,
    /// Emphasized grouping or primary treatment.
    Emphasized,
}

/// Portable overflow intent for an action in a toolbar's `actions` slot.
///
/// Action declaration order is the priority order. Renderers move [`Self::Auto`]
/// actions into overflow from the end first, keep [`Self::Never`] actions in the
/// primary toolbar when possible, and render [`Self::Always`] actions only in
/// overflow. At constrained widths, every action must remain reachable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiToolbarOverflow {
    /// Overflow from the end of the declared action list as space tightens.
    #[default]
    Auto,
    /// Prefer placement in the primary toolbar.
    Never,
    /// Render only in the toolbar's overflow affordance.
    Always,
}

/// Directional trend metadata for metric primitives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiMetricTrend {
    /// Trend direction.
    pub direction: UiMetricTrendDirection,
    /// Optional renderer-neutral trend value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// Optional accessible label for the trend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Directional metric trend token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiMetricTrendDirection {
    /// Positive or increasing trend.
    Up,
    /// Negative or decreasing trend.
    Down,
    /// Flat or unchanged trend.
    Flat,
}

/// Selection behavior shared by list and table primitives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiSelection {
    /// Selection mode.
    pub mode: UiSelectionMode,
    /// Owner-controlled selected item or row ids.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selected: Vec<String>,
}

/// Selection mode shared by list and table primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiSelectionMode {
    /// Selection is not supported.
    None,
    /// One row or item may be selected.
    Single,
    /// Multiple rows or items may be selected.
    Multiple,
}

/// Table column descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiTableColumn {
    /// Existing simple column id shape.
    Id(String),
    /// Typed column descriptor.
    Descriptor(UiTableColumnDescriptor),
}

/// Typed table column descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiTableColumnDescriptor {
    /// Stable column id.
    pub id: String,
    /// Optional display label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Optional semantic alignment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<UiTableColumnAlign>,
}

/// Semantic table column alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiTableColumnAlign {
    /// Start-aligned content.
    Start,
    /// Center-aligned content.
    Center,
    /// End-aligned content.
    End,
}

/// Table row descriptor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiTableRow {
    /// Stable row id.
    pub id: String,
    /// Cells keyed by column id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cells: BTreeMap<String, UiTableCell>,
    /// Optional row-specific action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<UiAction>,
}

/// Table cell content.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UiTableCell {
    /// Nested UI node cell.
    Node(UiNode),
    /// Primitive JSON cell value.
    Value(Value),
}

/// Semantic UI action request kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiActionKind {
    /// Submit form values or commit an action.
    Submit,
    /// Reset local or owner-managed form state.
    Reset,
    /// Ask the owner to validate current values without committing them.
    Validate,
    /// Cancel a pending interaction.
    Cancel,
}

/// Transport-neutral form values keyed by field id.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UiFormValues(pub Map<String, Value>);

/// Transport-neutral action request emitted by a UI client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiActionRequest {
    /// Request correlation id.
    pub request_id: UiActionRequestId,
    /// Surface that owns or routed the action.
    pub surface_id: UiSurfaceId,
    /// Semantic action id.
    pub action_id: UiActionId,
    /// Optional node that emitted the action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<UiNodeId>,
    /// Semantic action request kind.
    pub kind: UiActionKind,
    /// Optional form values sent with submit or validate requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<UiFormValues>,
    /// Optional non-form action metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// UI action result state authored by the action owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiActionResultState {
    /// The owner accepted and applied the action.
    Accepted,
    /// The owner rejected the action, commonly with validation details.
    Rejected,
    /// The owner deferred completion and will resolve it asynchronously.
    Deferred,
    /// The owner failed to process the action.
    Error,
}

/// Field-level validation messages keyed by field id.
pub type UiFieldErrors = BTreeMap<String, Vec<String>>;

/// Action result identity, outcome, and owner-authored validation details.
///
/// Validation results are authoritative only when returned by the action owner,
/// host, or plugin. Clients may use hints for preflight presentation, but must
/// not treat normalized values or validation messages as client-side authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiActionResult {
    /// Request correlation id.
    pub request_id: UiActionRequestId,
    /// Surface that owns or routed the action.
    pub surface_id: UiSurfaceId,
    /// Semantic action id.
    pub action_id: UiActionId,
    /// Optional node that emitted the action.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<UiNodeId>,
    /// Owner-authored action result state.
    pub state: UiActionResultState,
    /// Owner-authored field validation errors keyed by field id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_errors: UiFieldErrors,
    /// Owner-authored form-level validation errors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub form_errors: Vec<String>,
    /// Owner-authored warnings that do not reject the action.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Owner-authored normalized values returned after validation or submit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub normalized_values: Option<UiFormValues>,
    /// Scoped client-local presentation operations applied after acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<Vec<UiPresentationOperation>>,
    /// Inline owner-authored replacement tree applied after acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement: Option<Box<UiNode>>,
    /// Optional successful or deferred action payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// Optional owner-authored error detail for rejected or failed actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl UiActionResult {
    /// Validate state-dependent effects and any inline replacement tree.
    pub fn validate(&self) -> Result<(), UiActionResultValidationError> {
        let has_presentation = self
            .presentation
            .as_ref()
            .is_some_and(|operations| !operations.is_empty());
        let has_replacement = self.replacement.is_some();

        if self.state != UiActionResultState::Accepted && (has_presentation || has_replacement) {
            return Err(UiActionResultValidationError::EffectsRequireAcceptance);
        }

        if let Some(operations) = &self.presentation {
            for operation in operations {
                let key = match operation {
                    UiPresentationOperation::Set { key, .. }
                    | UiPresentationOperation::Clear { key }
                    | UiPresentationOperation::Toggle { key } => key,
                };
                if key.0.trim().is_empty() {
                    return Err(UiActionResultValidationError::EmptyPresentationKey);
                }
            }
        }

        if let Some(replacement) = &self.replacement {
            replacement
                .validate_authored()
                .map_err(UiActionResultValidationError::InvalidReplacement)?;
        }

        Ok(())
    }
}

/// Validation error for owner-authored action results.
#[derive(Debug, Error, PartialEq)]
pub enum UiActionResultValidationError {
    /// Presentation or replacement effects are accepted-only.
    #[error("presentation and replacement effects require an accepted action result")]
    EffectsRequireAcceptance,
    /// Presentation state keys are always nonblank.
    #[error("presentation state key cannot be empty")]
    EmptyPresentationKey,
    /// The inline replacement tree is invalid.
    #[error("invalid replacement UI tree: {0}")]
    InvalidReplacement(UiValidationError),
}

/// UI contract validation error.
#[derive(Debug, Error, PartialEq)]
pub enum UiValidationError {
    /// Unknown primitive kind.
    #[error("unknown UI node kind `{kind}`")]
    UnknownKind {
        /// Unknown kind name.
        kind: String,
    },
    /// Required prop is missing.
    #[error("{kind:?} missing required prop `{prop}`")]
    MissingProp {
        /// Node kind.
        kind: UiNodeKind,
        /// Prop name.
        prop: &'static str,
    },
    /// Unknown prop is present.
    #[error("{kind:?} has unknown prop `{prop}`")]
    UnknownProp {
        /// Node kind.
        kind: UiNodeKind,
        /// Prop name.
        prop: String,
    },
    /// Prop value is invalid.
    #[error("{kind:?} has invalid prop `{prop}`: {reason}")]
    InvalidProp {
        /// Node kind.
        kind: UiNodeKind,
        /// Prop name.
        prop: String,
        /// Validation reason.
        reason: String,
    },
    /// Required slot is missing.
    #[error("{kind:?} missing required slot `{slot}`")]
    MissingSlot {
        /// Node kind.
        kind: UiNodeKind,
        /// Slot name.
        slot: &'static str,
    },
    /// Unknown slot is present.
    #[error("{kind:?} has unknown slot `{slot}`")]
    UnknownSlot {
        /// Node kind.
        kind: UiNodeKind,
        /// Slot name.
        slot: String,
    },
    /// Required action is missing.
    #[error("{kind:?} missing required action")]
    MissingAction {
        /// Node kind.
        kind: UiNodeKind,
    },
    /// Required accessible label is missing.
    #[error("{kind:?} missing required label")]
    MissingLabel {
        /// Node kind.
        kind: UiNodeKind,
    },
    /// Stable node id is required.
    #[error("{kind:?} missing required stable node id: {reason}")]
    MissingId {
        /// Node kind.
        kind: UiNodeKind,
        /// Requirement reason.
        reason: &'static str,
    },
    /// Binding path is invalid.
    #[error("invalid bind path `{path}`: {reason}")]
    InvalidBindPath {
        /// Invalid path.
        path: String,
        /// Validation reason.
        reason: String,
    },
    /// A bound-list descendant identity is misplaced or ambiguous.
    #[error("invalid bind_list descendant identity key `{key}`: {reason}")]
    InvalidBindListDescendantId {
        /// Producer-authored descendant key.
        key: String,
        /// Validation reason.
        reason: String,
    },
    /// Renderer capability is unsupported and no fallback was declared.
    #[error("{kind:?} requires unsupported renderer capability `{capability}`: {reason}")]
    UnsupportedCapability {
        /// Node kind.
        kind: UiNodeKind,
        /// Capability name.
        capability: &'static str,
        /// Validation reason.
        reason: String,
    },
    /// Recursive node context.
    #[error("invalid node {id:?} ({kind:?}): {source}")]
    Node {
        /// Node id.
        id: Option<UiAuthoredNodeId>,
        /// Node kind.
        kind: UiNodeKind,
        /// Nested error.
        source: Box<UiValidationError>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiValidationContext {
    Static,
    BindListItemRoot,
    BoundBindListItemDescendant,
    UnboundBindListItemDescendant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UiValidationPhase {
    Authored,
    Realized,
}

impl UiValidationContext {
    fn child(self, node: &UiNode) -> Self {
        match self {
            Self::Static => Self::Static,
            Self::BindListItemRoot => {
                if matches!(node.id, Some(UiAuthoredNodeId::Bind(_))) {
                    Self::BoundBindListItemDescendant
                } else {
                    Self::UnboundBindListItemDescendant
                }
            }
            Self::BoundBindListItemDescendant => Self::BoundBindListItemDescendant,
            Self::UnboundBindListItemDescendant => Self::UnboundBindListItemDescendant,
        }
    }
}

fn validate_ui_node_in_context(
    node: &UiNode,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    validate_node(node, phase, context, descendant_keys).map_err(|error| UiValidationError::Node {
        id: node.id.clone(),
        kind: node.kind,
        source: Box::new(error),
    })
}

fn validate_node(
    node: &UiNode,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    let schema = schema_for(node.kind);

    for &required in &schema.required_props {
        if !node.props.contains_key(required) {
            return Err(UiValidationError::MissingProp {
                kind: node.kind,
                prop: required,
            });
        }
    }

    for (prop, value) in &node.props {
        if !schema.allowed_props.contains(prop.as_str()) && node.kind != UiNodeKind::Custom {
            return Err(UiValidationError::UnknownProp {
                kind: node.kind,
                prop: prop.clone(),
            });
        }
        if schema.allowed_props.contains(prop.as_str()) {
            validate_prop_value(
                node.kind,
                prop,
                value,
                phase,
                schema.required_props.contains(&prop.as_str()),
            )?;
        } else {
            validate_custom_payload_prop(node.kind, prop, value, phase)?;
        }
    }

    validate_prop_combinations(node)?;
    validate_custom_node(node)?;
    validate_stable_id(node, phase, context, descendant_keys)?;
    validate_required_action(node)?;
    validate_required_label(node)?;

    let child_context = context.child(node);
    for required in schema.required_slots {
        if !node.slots.contains_key(required) {
            return Err(UiValidationError::MissingSlot {
                kind: node.kind,
                slot: required,
            });
        }
    }

    for (slot, children) in &node.slots {
        if !schema.allowed_slots.contains(slot.as_str()) {
            return Err(UiValidationError::UnknownSlot {
                kind: node.kind,
                slot: slot.clone(),
            });
        }
        for child in children {
            validate_child(child, phase, child_context, descendant_keys)?;
        }
    }

    for child in &node.children {
        validate_child(child, phase, child_context, descendant_keys)?;
    }

    Ok(())
}

fn validate_child(
    child: &UiChild,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    match child {
        UiChild::Conditional(conditional) => {
            validate_conditional(conditional, phase, context, descendant_keys)
        }
        UiChild::Node(node) => validate_ui_node_in_context(node, phase, context, descendant_keys),
        UiChild::BindList(bind_list) => validate_bind_list(bind_list, phase),
        UiChild::BindIf(bind_if) => validate_bind_if(bind_if, phase, context, descendant_keys),
    }
}

fn validate_conditional(
    conditional: &UiConditional,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    match conditional {
        UiConditional::When { condition: _, node }
        | UiConditional::Hidden { condition: _, node } => {
            validate_ui_node_in_context(node, phase, context, descendant_keys)
        }
    }
}

fn validate_bind_list(
    bind_list: &UiBindList,
    phase: UiValidationPhase,
) -> Result<(), UiValidationError> {
    match bind_list {
        UiBindList::BindList {
            source,
            r#where,
            item_template,
            empty_template,
        } => {
            validate_bind_path(source)?;
            if phase == UiValidationPhase::Realized {
                return Err(UiValidationError::InvalidBindPath {
                    path: source.clone(),
                    reason: "bind_list must be materialized before realized validation".to_string(),
                });
            }
            if !source.starts_with('/') {
                return Err(UiValidationError::InvalidBindPath {
                    path: source.clone(),
                    reason: "bind_list source must be an absolute entity family path".to_string(),
                });
            }
            validate_bind_list_where(r#where)?;
            validate_ui_node_in_context(
                item_template,
                phase,
                UiValidationContext::BindListItemRoot,
                &mut BTreeSet::new(),
            )?;
            if let Some(template) = empty_template {
                validate_ui_node_in_context(
                    template,
                    phase,
                    UiValidationContext::Static,
                    &mut BTreeSet::new(),
                )?;
            }
            Ok(())
        }
    }
}

fn validate_bind_if(
    bind_if: &UiBindIf,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    match bind_if {
        UiBindIf::BindIf { path, node } => {
            validate_bind_path(path)?;
            if phase == UiValidationPhase::Realized {
                return Err(UiValidationError::InvalidBindPath {
                    path: path.clone(),
                    reason: "bind_if must be materialized before realized validation".to_string(),
                });
            }
            validate_ui_node_in_context(node, phase, context, descendant_keys)
        }
        UiBindIf::PresentationIf { predicate, node } => {
            validate_presentation_predicate(predicate)?;
            validate_ui_node_in_context(node, phase, context, descendant_keys)
        }
    }
}

fn validate_presentation_predicate(
    predicate: &UiPresentationPredicate,
) -> Result<(), UiValidationError> {
    let key = match predicate {
        UiPresentationPredicate::Present { key }
        | UiPresentationPredicate::Truthy { key }
        | UiPresentationPredicate::Equals { key, .. } => key,
    };
    if key.0.trim().is_empty() {
        return Err(UiValidationError::InvalidBindPath {
            path: key.0.clone(),
            reason: "presentation key cannot be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_prop_value(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
    phase: UiValidationPhase,
    required: bool,
) -> Result<(), UiValidationError> {
    if let Some(path) = value.get("$bind").and_then(Value::as_str) {
        validate_bind_path(path)?;
    }

    if value.get("$bind").is_some() {
        let object = value
            .as_object()
            .ok_or_else(|| UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bind value must be an object".to_string(),
            })?;
        if object.len() != 1 {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bind value may only contain $bind".to_string(),
            });
        }
        if !object.get("$bind").is_some_and(Value::is_string) {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "$bind value must be a string".to_string(),
            });
        }
    }

    if phase == UiValidationPhase::Realized {
        reject_unresolved_bind(kind, prop, value)?;
    }

    if let Some(dynamic_kind) = value.get("$kind").and_then(Value::as_str) {
        match dynamic_kind {
            "responsive" => {
                serde_json::from_value::<UiResponsiveValue>(value.clone()).map_err(|error| {
                    UiValidationError::InvalidProp {
                        kind,
                        prop: prop.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                validate_token_value(kind, prop, value)?;
            }
            "entity_options" if kind == UiNodeKind::Select && prop == "options_source" => {
                let descriptor = deserialize_prop::<UiEntityOptionsSource>(kind, prop, value)?;
                validate_entity_options_source(kind, prop, &descriptor)?;
            }
            other => {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: format!("unknown dynamic value kind `{other}`"),
                });
            }
        }
    } else {
        validate_token_value(kind, prop, value)?;
    }

    match (kind, prop) {
        (UiNodeKind::FormField, "schema") => {
            let schema = deserialize_prop::<UiFieldSchema>(kind, prop, value)?;
            validate_field_schema(kind, prop, &schema)?;
        }
        (UiNodeKind::Select, "options_source") => {
            // Validated in the `$kind: entity_options` branch above. Reject
            // non-entity_options shapes that slipped past without a $kind arm.
            if value.get("$kind").and_then(Value::as_str) != Some("entity_options") {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: "options_source must use $kind entity_options".to_string(),
                });
            }
        }
        (
            UiNodeKind::TextInput
            | UiNodeKind::Textarea
            | UiNodeKind::Checkbox
            | UiNodeKind::Select,
            "validation",
        ) => {
            deserialize_prop::<UiFieldValidationHints>(kind, prop, value)?;
        }
        (_, "disabled" | "loading") => {
            if !value.is_boolean() && value.get("$bind").is_none() {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: "value must be a boolean".to_string(),
                });
            }
        }
        (_, "error") => validate_error_prop(kind, prop, value)?,
        (UiNodeKind::Dialog, "presentation") => {
            deserialize_prop::<UiDialogPresentation>(kind, prop, value)?;
        }
        (UiNodeKind::Form, "submit_label") => {
            validate_nonblank_string_or_bind_prop(kind, prop, value)?;
        }
        (_, "shortcut" | "hover_label" | "copy_value") => {
            validate_string_or_bind_prop(kind, prop, value)?;
        }
        (_, "context_menu") => {
            deserialize_prop::<Vec<UiAction>>(kind, prop, value)?;
        }
        (_, "action" | "primary_action" | "secondary_action" | "row_action" | "activation") => {
            let action = deserialize_prop::<UiAction>(kind, prop, value)?;
            validate_action(kind, prop, &action)?;
        }
        (_, "density") => {
            deserialize_prop::<UiDensity>(kind, prop, value)?;
        }
        (
            UiNodeKind::Panel | UiNodeKind::MetricGrid | UiNodeKind::Toolbar | UiNodeKind::Section,
            "variant",
        ) => {
            deserialize_prop::<UiVariant>(kind, prop, value)?;
        }
        (
            UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem,
            "toolbar_overflow",
        ) => {
            deserialize_prop::<UiToolbarOverflow>(kind, prop, value)?;
        }
        (_, "selection") => {
            let selection = deserialize_prop::<UiSelection>(kind, prop, value)?;
            validate_selection(kind, prop, &selection)?;
        }
        (UiNodeKind::Metric, "trend") => {
            let trend = deserialize_prop::<UiMetricTrend>(kind, prop, value)?;
            validate_metric_trend(kind, prop, &trend)?;
        }
        (UiNodeKind::MetricGrid, "compact") => {
            if !value.is_boolean() && value.get("$bind").is_none() {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: "value must be a boolean".to_string(),
                });
            }
        }
        (UiNodeKind::Table, "columns") => {
            let columns = deserialize_prop::<Vec<UiTableColumn>>(kind, prop, value)?;
            validate_table_columns(kind, prop, &columns)?;
        }
        (UiNodeKind::Table, "rows") => {
            let rows = deserialize_prop::<Vec<UiTableRow>>(kind, prop, value)?;
            validate_table_rows(kind, prop, &rows)?;
        }
        (UiNodeKind::Table, "empty_state") => {
            let empty_state = deserialize_prop::<UiNode>(kind, prop, value)?;
            validate_ui_node_in_context(
                &empty_state,
                phase,
                UiValidationContext::Static,
                &mut BTreeSet::new(),
            )?;
        }
        (UiNodeKind::Iframe, "src" | "title") => {
            validate_nonblank_string_or_bind_prop(kind, prop, value)?;
        }
        (UiNodeKind::Iframe, "sandbox") => {
            deserialize_prop::<Vec<UiIframeSandboxToken>>(kind, prop, value)?;
        }
        (UiNodeKind::Iframe, "allow") => {
            deserialize_prop::<Vec<UiIframePermission>>(kind, prop, value)?;
        }
        (UiNodeKind::Iframe, "bridge") => {
            let bridge = deserialize_prop::<UiIframeBridge>(kind, prop, value)?;
            validate_iframe_bridge(kind, prop, &bridge)?;
        }
        (UiNodeKind::Custom, "namespace" | "component") => {
            validate_custom_identifier_prop(kind, prop, value)?;
        }
        (UiNodeKind::Custom, "reason") => {
            validate_custom_reason_prop(kind, prop, value)?;
        }
        _ => {}
    }

    if phase == UiValidationPhase::Authored
        && required
        && value.get("$bind").is_some()
        && !is_required_bindable_prop(kind, prop)
    {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "required field does not accept a binding sentinel".to_string(),
        });
    }

    Ok(())
}

fn is_required_bindable_prop(kind: UiNodeKind, prop: &str) -> bool {
    matches!(
        (kind, prop),
        (
            UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem,
            "label"
        ) | (UiNodeKind::Form, "submit_label")
            | (UiNodeKind::Iframe, "src" | "title")
            | (UiNodeKind::Text, "text")
    )
}

fn reject_unresolved_bind(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    match value {
        Value::Array(values) => {
            for value in values {
                reject_unresolved_bind(kind, prop, value)?;
            }
        }
        Value::Object(object) => {
            if object.contains_key("$bind") {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: "unresolved binding sentinel is not valid after materialization"
                        .to_string(),
                });
            }
            for value in object.values() {
                reject_unresolved_bind(kind, prop, value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_action(
    kind: UiNodeKind,
    prop: &str,
    action: &UiAction,
) -> Result<(), UiValidationError> {
    if action.id.0.trim().is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "action id cannot be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_selection(
    kind: UiNodeKind,
    prop: &str,
    selection: &UiSelection,
) -> Result<(), UiValidationError> {
    if selection.mode == UiSelectionMode::None && !selection.selected.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "selection mode none cannot include selected ids".to_string(),
        });
    }
    if selection.mode == UiSelectionMode::Single && selection.selected.len() > 1 {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "single selection cannot include multiple selected ids".to_string(),
        });
    }
    if selection.selected.iter().any(|id| id.trim().is_empty()) {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "selected ids cannot be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_metric_trend(
    kind: UiNodeKind,
    prop: &str,
    trend: &UiMetricTrend,
) -> Result<(), UiValidationError> {
    if trend
        .label
        .as_deref()
        .is_some_and(|label| label.trim().is_empty())
    {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "trend label cannot be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_table_columns(
    kind: UiNodeKind,
    prop: &str,
    columns: &[UiTableColumn],
) -> Result<(), UiValidationError> {
    if columns.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "columns cannot be empty".to_string(),
        });
    }

    let mut ids = BTreeSet::new();
    for column in columns {
        let id = match column {
            UiTableColumn::Id(id) => id,
            UiTableColumn::Descriptor(descriptor) => &descriptor.id,
        };
        if id.trim().is_empty() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "column ids cannot be empty".to_string(),
            });
        }
        if !ids.insert(id.clone()) {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: format!("duplicate column id `{id}`"),
            });
        }
    }

    Ok(())
}

fn validate_table_rows(
    kind: UiNodeKind,
    prop: &str,
    rows: &[UiTableRow],
) -> Result<(), UiValidationError> {
    let mut ids = BTreeSet::new();
    for row in rows {
        if row.id.trim().is_empty() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "row ids cannot be empty".to_string(),
            });
        }
        if !ids.insert(row.id.clone()) {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: format!("duplicate row id `{}`", row.id),
            });
        }
        if let Some(action) = &row.action {
            validate_action(kind, prop, action)?;
        }
        for (column_id, cell) in &row.cells {
            if column_id.trim().is_empty() {
                return Err(UiValidationError::InvalidProp {
                    kind,
                    prop: prop.to_string(),
                    reason: "cell column ids cannot be empty".to_string(),
                });
            }
            if let UiTableCell::Node(node) = cell {
                node.validate()?;
            }
        }
    }

    Ok(())
}

fn deserialize_prop<T>(kind: UiNodeKind, prop: &str, value: &Value) -> Result<T, UiValidationError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value::<T>(value.clone()).map_err(|error| UiValidationError::InvalidProp {
        kind,
        prop: prop.to_string(),
        reason: error.to_string(),
    })
}

fn validate_field_schema(
    kind: UiNodeKind,
    prop: &str,
    schema: &UiFieldSchema,
) -> Result<(), UiValidationError> {
    if schema.name.trim().is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "schema name cannot be empty".to_string(),
        });
    }

    if schema.label.trim().is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "schema label cannot be empty".to_string(),
        });
    }

    if schema.kind == UiFieldKind::Select && schema.options.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "select schema requires options".to_string(),
        });
    }

    if schema.kind != UiFieldKind::Select && !schema.options.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "only select schema may define options".to_string(),
        });
    }

    Ok(())
}

fn validate_error_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    if value.get("$bind").is_some() || value.is_string() || value.is_null() {
        return Ok(());
    }

    if value
        .as_object()
        .and_then(|object| object.get("message"))
        .is_some_and(Value::is_string)
    {
        return Ok(());
    }

    Err(UiValidationError::InvalidProp {
        kind,
        prop: prop.to_string(),
        reason: "error must be a string or object with a string message".to_string(),
    })
}

fn validate_string_or_bind_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    if value.is_string() || value.get("$bind").is_some() {
        return Ok(());
    }

    Err(UiValidationError::InvalidProp {
        kind,
        prop: prop.to_string(),
        reason: "value must be a string or bind".to_string(),
    })
}

fn validate_nonblank_string_or_bind_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    match value.as_str() {
        Some(value) if !value.trim().is_empty() => Ok(()),
        Some(_) => Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value cannot be empty".to_string(),
        }),
        None if value.get("$bind").is_some() => Ok(()),
        None => Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value must be a non-empty string or bind".to_string(),
        }),
    }
}

fn validate_iframe_bridge(
    kind: UiNodeKind,
    prop: &str,
    bridge: &UiIframeBridge,
) -> Result<(), UiValidationError> {
    for action in &bridge.actions {
        if action.0.trim().is_empty() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bridge action ids cannot be empty".to_string(),
            });
        }
    }

    for message in &bridge.messages {
        if message.trim().is_empty() {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bridge messages cannot be empty".to_string(),
            });
        }
    }

    Ok(())
}

fn validate_custom_identifier_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    let value = value
        .as_str()
        .ok_or_else(|| UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value must be a string".to_string(),
        })?;

    if value.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value cannot be empty".to_string(),
        });
    }

    if value.starts_with(['.', '-', '_']) || value.ends_with(['.', '-', '_']) {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value cannot start or end with a separator".to_string(),
        });
    }

    let mut previous_was_separator = false;
    for character in value.chars() {
        let is_separator = matches!(character, '.' | '-' | '_');
        if !(character.is_ascii_lowercase() || character.is_ascii_digit() || is_separator) {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "value must use lowercase ASCII letters, digits, '.', '-', or '_'"
                    .to_string(),
            });
        }
        if is_separator && previous_was_separator {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "value cannot contain adjacent separators".to_string(),
            });
        }
        previous_was_separator = is_separator;
    }

    Ok(())
}

fn validate_custom_reason_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    match value.as_str() {
        Some(value) if !value.trim().is_empty() => Ok(()),
        Some(_) => Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value cannot be empty".to_string(),
        }),
        None => Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "value must be a string".to_string(),
        }),
    }
}

fn validate_custom_payload_prop(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
    phase: UiValidationPhase,
) -> Result<(), UiValidationError> {
    if prop == "fallback" {
        return Err(UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: "fallback must be declared in the `fallback` slot".to_string(),
        });
    }

    if value.get("$bind").is_some() {
        let object = value
            .as_object()
            .ok_or_else(|| UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bind value must be an object".to_string(),
            })?;
        if object.len() != 1 {
            return Err(UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "bind value may only contain $bind".to_string(),
            });
        }
        let path = object.get("$bind").and_then(Value::as_str).ok_or_else(|| {
            UiValidationError::InvalidProp {
                kind,
                prop: prop.to_string(),
                reason: "$bind value must be a string".to_string(),
            }
        })?;
        validate_bind_path(path)?;
    }

    if phase == UiValidationPhase::Realized {
        reject_unresolved_bind(kind, prop, value)?;
    }

    Ok(())
}

fn validate_prop_combinations(node: &UiNode) -> Result<(), UiValidationError> {
    let schema = schema_for(node.kind);

    if schema.allowed_props.contains("default") && node.props.contains_key("default") {
        for controlled_prop in ["value", "checked", "selected"] {
            if schema.allowed_props.contains(controlled_prop)
                && node.props.contains_key(controlled_prop)
            {
                return Err(UiValidationError::InvalidProp {
                    kind: node.kind,
                    prop: "default".to_string(),
                    reason: format!("default cannot be used with `{controlled_prop}`"),
                });
            }
        }
    }

    if node.kind == UiNodeKind::FormField {
        let schema = node
            .props
            .get("schema")
            .map(|value| deserialize_prop::<UiFieldSchema>(node.kind, "schema", value))
            .transpose()?;
        if let (Some(schema), Some(default)) = (schema, node.props.get("default")) {
            match &schema.default {
                Some(schema_default) if schema_default == default => {}
                Some(_) => {
                    return Err(UiValidationError::InvalidProp {
                        kind: node.kind,
                        prop: "default".to_string(),
                        reason: "default must match schema default".to_string(),
                    });
                }
                None => {
                    return Err(UiValidationError::InvalidProp {
                        kind: node.kind,
                        prop: "default".to_string(),
                        reason: "form_field default must be declared in schema".to_string(),
                    });
                }
            }
        }
    }

    if node.kind == UiNodeKind::Section
        && !node.props.contains_key("title")
        && !node.slots.contains_key("header")
    {
        return Err(UiValidationError::MissingProp {
            kind: node.kind,
            prop: "title",
        });
    }

    if node.kind == UiNodeKind::Select {
        let has_static_options = node
            .slots
            .get("options")
            .is_some_and(|children| !children.is_empty());
        let has_entity_options = node.props.contains_key("options_source");
        match (has_static_options, has_entity_options) {
            (true, false) | (false, true) => {}
            (true, true) => {
                return Err(UiValidationError::InvalidProp {
                    kind: node.kind,
                    prop: "options_source".to_string(),
                    reason: "select cannot combine static options with options_source".to_string(),
                });
            }
            (false, false) => {
                return Err(UiValidationError::MissingSlot {
                    kind: node.kind,
                    slot: "options",
                });
            }
        }
        if has_static_options {
            for child in node.slots.get("options").into_iter().flatten() {
                match child {
                    UiChild::Node(option) if option.kind == UiNodeKind::SelectOption => {}
                    _ => {
                        return Err(UiValidationError::InvalidProp {
                            kind: node.kind,
                            prop: "options".to_string(),
                            reason: "options slot may only contain select_option nodes".to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_custom_node(node: &UiNode) -> Result<(), UiValidationError> {
    if node.kind != UiNodeKind::Custom {
        return Ok(());
    }

    if !node.children.is_empty() {
        return Err(UiValidationError::InvalidProp {
            kind: node.kind,
            prop: "children".to_string(),
            reason: "custom nodes must put their fallback in the `fallback` slot".to_string(),
        });
    }

    let fallback = node
        .slots
        .get("fallback")
        .ok_or(UiValidationError::MissingSlot {
            kind: node.kind,
            slot: "fallback",
        })?;

    let [UiChild::Node(fallback)] = fallback.as_slice() else {
        return Err(UiValidationError::InvalidProp {
            kind: node.kind,
            prop: "fallback".to_string(),
            reason: "fallback slot must contain exactly one static node".to_string(),
        });
    };

    if !is_custom_fallback_kind(fallback.kind) {
        return Err(UiValidationError::InvalidProp {
            kind: node.kind,
            prop: "fallback".to_string(),
            reason: format!(
                "{:?} is not allowed as a custom fallback; use kernel primitives or iframe",
                fallback.kind
            ),
        });
    }

    Ok(())
}

fn is_custom_fallback_kind(kind: UiNodeKind) -> bool {
    matches!(
        kind,
        UiNodeKind::Stack
            | UiNodeKind::Inline
            | UiNodeKind::Form
            | UiNodeKind::ScrollArea
            | UiNodeKind::Text
            | UiNodeKind::Icon
            | UiNodeKind::Badge
            | UiNodeKind::StatusDot
            | UiNodeKind::EmptyState
            | UiNodeKind::List
            | UiNodeKind::Tree
            | UiNodeKind::Table
            | UiNodeKind::Button
            | UiNodeKind::IconButton
            | UiNodeKind::Menu
            | UiNodeKind::Dialog
            | UiNodeKind::TextInput
            | UiNodeKind::Textarea
            | UiNodeKind::Checkbox
            | UiNodeKind::Select
            | UiNodeKind::Iframe
    )
}

fn validate_stable_id(
    node: &UiNode,
    phase: UiValidationPhase,
    context: UiValidationContext,
    descendant_keys: &mut BTreeSet<String>,
) -> Result<(), UiValidationError> {
    if let Some(UiAuthoredNodeId::Bind(bind)) = &node.id {
        validate_bind_path(&bind.path)?;
        if phase == UiValidationPhase::Realized {
            return Err(UiValidationError::InvalidBindPath {
                path: bind.path.clone(),
                reason: "bound node id must be materialized before realized validation".to_string(),
            });
        }
        if matches!(
            context,
            UiValidationContext::BoundBindListItemDescendant
                | UiValidationContext::UnboundBindListItemDescendant
        ) {
            return Err(UiValidationError::InvalidBindPath {
                path: bind.path.clone(),
                reason: "a bound node id is valid only on the bind_list item_template root, not on its descendants"
                    .to_string(),
            });
        }
        if context != UiValidationContext::BindListItemRoot || !bind.path.starts_with("@/") {
            return Err(UiValidationError::InvalidBindPath {
                path: bind.path.clone(),
                reason: "bound node id requires an item-relative path on the bind_list item_template root"
                    .to_string(),
            });
        }
    }

    if let Some(UiAuthoredNodeId::BindListDescendant(descendant_id)) = &node.id {
        let key = descendant_id.key();
        if phase == UiValidationPhase::Realized {
            return Err(UiValidationError::InvalidBindListDescendantId {
                key: key.to_string(),
                reason: "descendant identity must be materialized before realized validation"
                    .to_string(),
            });
        }
        if key.trim().is_empty() {
            return Err(UiValidationError::InvalidBindListDescendantId {
                key: key.to_string(),
                reason: "key cannot be blank".to_string(),
            });
        }
        if context != UiValidationContext::BoundBindListItemDescendant {
            return Err(UiValidationError::InvalidBindListDescendantId {
                key: key.to_string(),
                reason: "the keyed form is valid only below a bind_list item_template root with an item-relative bound id"
                    .to_string(),
            });
        }
        // Authored descendant keys are intentionally template-global, even across
        // mutually exclusive branches; final realized-id collisions stay render-scoped.
        if !descendant_keys.insert(key.to_string()) {
            return Err(UiValidationError::InvalidBindListDescendantId {
                key: key.to_string(),
                reason: "key must be unique across the complete bind_list item template"
                    .to_string(),
            });
        }
    }

    let reason = if matches!(
        node.kind,
        UiNodeKind::Form | UiNodeKind::FormSection | UiNodeKind::FormField
    ) {
        Some("forms and form fields require stable identity")
    } else if matches!(
        node.kind,
        UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem
    ) {
        Some("action feedback requires stable identity")
    } else if matches!(
        node.kind,
        UiNodeKind::TextInput | UiNodeKind::Textarea | UiNodeKind::Checkbox | UiNodeKind::Select
    ) && node
        .props
        .keys()
        .any(|prop| matches!(prop.as_str(), "value" | "checked" | "selected" | "default"))
    {
        Some("field state requires stable identity")
    } else {
        None
    };

    let missing_id = node.id.as_ref().is_none_or(|id| match id {
        UiAuthoredNodeId::Literal(id) => id.0.trim().is_empty(),
        UiAuthoredNodeId::Bind(_) | UiAuthoredNodeId::BindListDescendant(_) => false,
    });
    if let Some(reason) = reason
        && missing_id
    {
        return Err(UiValidationError::MissingId {
            kind: node.kind,
            reason,
        });
    }

    Ok(())
}

fn validate_token_value(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError> {
    match prop {
        "gap" => validate_token_values::<UiSpaceToken>(kind, prop, value),
        "tone" => validate_token_values::<UiColorToken>(kind, prop, value),
        _ => Ok(()),
    }
}

fn validate_token_values<T>(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError>
where
    T: for<'de> Deserialize<'de>,
{
    if value.get("$bind").is_some() {
        return Ok(());
    }

    if value.get("$kind").and_then(Value::as_str) == Some("responsive") {
        if let Some(width) = value.get("width").and_then(Value::as_object) {
            for token in width.values() {
                validate_one_token::<T>(kind, prop, token)?;
            }
        }
        if let Some(height) = value.get("height").and_then(Value::as_object) {
            for token in height.values() {
                validate_one_token::<T>(kind, prop, token)?;
            }
        }
        return Ok(());
    }

    validate_one_token::<T>(kind, prop, value)
}

fn validate_one_token<T>(
    kind: UiNodeKind,
    prop: &str,
    value: &Value,
) -> Result<(), UiValidationError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value::<T>(value.clone())
        .map(|_| ())
        .map_err(|error| UiValidationError::InvalidProp {
            kind,
            prop: prop.to_string(),
            reason: error.to_string(),
        })
}

fn validate_required_action(node: &UiNode) -> Result<(), UiValidationError> {
    if matches!(
        node.kind,
        UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem
    ) && !node.props.contains_key("action")
    {
        return Err(UiValidationError::MissingAction { kind: node.kind });
    }
    Ok(())
}

fn validate_required_label(node: &UiNode) -> Result<(), UiValidationError> {
    if !matches!(
        node.kind,
        UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem
    ) {
        return Ok(());
    }

    let Some(label) = node.props.get("label") else {
        return Err(UiValidationError::MissingLabel { kind: node.kind });
    };
    if !is_required_bindable_prop(node.kind, "label") {
        return Err(UiValidationError::MissingLabel { kind: node.kind });
    }
    validate_nonblank_string_or_bind_prop(node.kind, "label", label)
        .map_err(|_| UiValidationError::MissingLabel { kind: node.kind })
}

fn validate_bind_path(path: &str) -> Result<(), UiValidationError> {
    if path.is_empty() {
        return Err(UiValidationError::InvalidBindPath {
            path: path.to_string(),
            reason: "path cannot be empty".to_string(),
        });
    }

    if path.starts_with('/') || path.starts_with("@/") {
        return Ok(());
    }

    Err(UiValidationError::InvalidBindPath {
        path: path.to_string(),
        reason: "path must start with `/` or `@/`".to_string(),
    })
}

fn validate_bind_list_where(r#where: &BTreeMap<String, Value>) -> Result<(), UiValidationError> {
    for key in r#where.keys() {
        if key.trim().is_empty() {
            return Err(UiValidationError::InvalidBindPath {
                path: key.clone(),
                reason: "bind_list where field cannot be empty".to_string(),
            });
        }
        if key.contains('/') || key.contains('.') {
            return Err(UiValidationError::InvalidBindPath {
                path: key.clone(),
                reason: "bind_list where filters exact top-level fields only".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_node_capabilities(
    node: &UiNode,
    capabilities: &UiCapabilitySet,
) -> Result<(), UiValidationError> {
    validate_node_capability_requirements(node, capabilities)?;

    for child in &node.children {
        validate_child_capabilities(child, capabilities)?;
    }
    for children in node.slots.values() {
        for child in children {
            validate_child_capabilities(child, capabilities)?;
        }
    }

    Ok(())
}

fn validate_child_capabilities(
    child: &UiChild,
    capabilities: &UiCapabilitySet,
) -> Result<(), UiValidationError> {
    match child {
        UiChild::Conditional(UiConditional::When { node, .. })
        | UiChild::Conditional(UiConditional::Hidden { node, .. })
        | UiChild::Node(node)
        | UiChild::BindIf(UiBindIf::BindIf { node, .. })
        | UiChild::BindIf(UiBindIf::PresentationIf { node, .. }) => {
            validate_node_capabilities(node, capabilities)
        }
        UiChild::BindList(UiBindList::BindList {
            item_template,
            empty_template,
            ..
        }) => {
            validate_node_capabilities(item_template, capabilities)?;
            if let Some(template) = empty_template {
                validate_node_capabilities(template, capabilities)?;
            }
            Ok(())
        }
    }
}

fn validate_node_capability_requirements(
    node: &UiNode,
    capabilities: &UiCapabilitySet,
) -> Result<(), UiValidationError> {
    let schema = schema_for(node.kind);

    if matches!(node.kind, UiNodeKind::TextInput | UiNodeKind::Textarea)
        && !capabilities.keyboard.text_entry
    {
        return unsupported(node.kind, "keyboard.textEntry", "text entry is required");
    }

    if matches!(
        node.kind,
        UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem
    ) && !capabilities.keyboard.focus_traversal
    {
        return unsupported(
            node.kind,
            "keyboard.focusTraversal",
            "interactive action nodes require focus traversal",
        );
    }

    if schema.allowed_props.contains("shortcut")
        && node.props.contains_key("shortcut")
        && !capabilities.keyboard.shortcuts
    {
        return unsupported(
            node.kind,
            "keyboard.shortcuts",
            "keyboard shortcut capability is required",
        );
    }

    if schema.allowed_props.contains("hover_label")
        && node.props.contains_key("hover_label")
        && !capabilities.hover
        && !capabilities.supports_fallback(UiCapabilityFallback::HoverPersistentHints)
    {
        return unsupported(node.kind, "hover", "hover fallback was not declared");
    }

    if schema.allowed_props.contains("copy_value")
        && node.props.contains_key("copy_value")
        && !capabilities.clipboard
        && !capabilities.supports_fallback(UiCapabilityFallback::ClipboardManual)
    {
        return unsupported(
            node.kind,
            "clipboard",
            "clipboard fallback was not declared",
        );
    }

    if schema.allowed_props.contains("context_menu")
        && node.props.contains_key("context_menu")
        && !capabilities.context_menu
        && !capabilities.supports_fallback(UiCapabilityFallback::ContextMenuAsMenu)
    {
        return unsupported(
            node.kind,
            "contextMenu",
            "context-menu fallback was not declared",
        );
    }

    if node.kind == UiNodeKind::Table
        && !capabilities.table
        && !capabilities.supports_fallback(UiCapabilityFallback::TableAsList)
    {
        return unsupported(node.kind, "table", "table fallback was not declared");
    }

    if node.kind == UiNodeKind::TerminalView
        && !capabilities.terminal_selection
        && !capabilities.supports_fallback(UiCapabilityFallback::TerminalSelectionDisabled)
    {
        return unsupported(
            node.kind,
            "terminalSelection",
            "terminal selection fallback was not declared",
        );
    }

    if node.kind == UiNodeKind::ConnectionCodeView
        && !capabilities.qr_code
        && !capabilities.supports_fallback(UiCapabilityFallback::ConnectionCodeText)
    {
        return unsupported(
            node.kind,
            "qrCode",
            "connection-code text fallback was not declared",
        );
    }

    if node.kind == UiNodeKind::Iframe
        && !capabilities.iframe
        && !capabilities.supports_fallback(UiCapabilityFallback::IframeAsLink)
    {
        return unsupported(node.kind, "iframe", "iframe link fallback was not declared");
    }

    if schema.allowed_props.contains("tone")
        && node.props.contains_key("tone")
        && !capabilities.rich_color
        && !capabilities.supports_fallback(UiCapabilityFallback::RichColorMuted)
    {
        return unsupported(
            node.kind,
            "richColor",
            "semantic color fallback was not declared",
        );
    }

    if node.kind == UiNodeKind::Dialog {
        let Some(presentation) = node.props.get("presentation") else {
            return Ok(());
        };
        let presentation =
            deserialize_prop::<UiDialogPresentation>(node.kind, "presentation", presentation)?;
        if presentation != UiDialogPresentation::Auto
            && !capabilities.dialog_presentations.contains(&presentation)
            && !capabilities.supports_fallback(UiCapabilityFallback::DialogInline)
        {
            return unsupported(
                node.kind,
                "dialogPresentations",
                "dialog presentation fallback was not declared",
            );
        }
    }

    Ok(())
}

fn unsupported<T>(
    kind: UiNodeKind,
    capability: &'static str,
    reason: &str,
) -> Result<T, UiValidationError> {
    Err(UiValidationError::UnsupportedCapability {
        kind,
        capability,
        reason: reason.to_string(),
    })
}

fn schema_for(kind: UiNodeKind) -> UiNodeSchema {
    match kind {
        UiNodeKind::Stack => schema(
            &["direction", "gap", "align", "justify"],
            &["direction"],
            &[],
            &[],
        ),
        UiNodeKind::Inline => schema(&["gap", "align", "justify"], &[], &[], &[]),
        UiNodeKind::Form => schema(
            &["action", "submit_label", "disabled", "loading", "error"],
            &["action", "submit_label"],
            &[],
            &[],
        ),
        UiNodeKind::FormSection => schema(
            &["title", "description", "disabled", "loading", "error"],
            &["title"],
            &[],
            &[],
        ),
        UiNodeKind::FormField => schema(
            &[
                "schema", "value", "checked", "selected", "default", "disabled", "loading", "error",
            ],
            &["schema"],
            &[],
            &[],
        ),
        UiNodeKind::Panel => schema(
            &["title", "tone", "density", "variant"],
            &[],
            &["header", "toolbar", "body", "footer", "empty", "actions"],
            &[],
        ),
        UiNodeKind::Metric => schema(
            &[
                "label", "value", "caption", "tone", "status", "trend", "delta", "action", "ref",
            ],
            &["label", "value"],
            &[],
            &[],
        ),
        UiNodeKind::MetricGrid => schema(&["density", "variant", "compact"], &[], &[], &[]),
        UiNodeKind::Toolbar => schema(
            &["label", "density", "variant"],
            &[],
            &["commands", "filters", "search", "actions"],
            &[],
        ),
        UiNodeKind::StatusBadge => schema(
            &["label", "status", "tone", "hover_label", "action"],
            &["label"],
            &[],
            &[],
        ),
        UiNodeKind::Section => schema(
            &["title", "description", "density", "variant"],
            &[],
            &["header", "toolbar", "body", "footer", "empty", "actions"],
            &[],
        ),
        UiNodeKind::ScrollArea => schema(&["height"], &[], &[], &[]),
        UiNodeKind::Text => schema(
            &["text", "tone", "variant", "hover_label", "copy_value"],
            &["text"],
            &[],
            &[],
        ),
        UiNodeKind::Icon => schema(
            &["icon", "label", "tone", "hover_label"],
            &["icon"],
            &[],
            &[],
        ),
        UiNodeKind::Badge => schema(&["label", "tone", "hover_label"], &["label"], &[], &[]),
        UiNodeKind::StatusDot => schema(&["label", "tone", "hover_label"], &["label"], &[], &[]),
        UiNodeKind::EmptyState => schema(
            &[
                "title",
                "description",
                "icon",
                "action",
                "primary_action",
                "secondary_action",
            ],
            &["title"],
            &[],
            &[],
        ),
        UiNodeKind::List => schema(&["aria_label", "selection"], &[], &[], &[]),
        UiNodeKind::ListItem => schema(
            &[
                "value",
                "selected",
                "action",
                "activation",
                "hover_label",
                "context_menu",
            ],
            &[],
            &["title", "subtitle", "meta", "actions"],
            &["title"],
        ),
        UiNodeKind::Tree => schema(&["aria_label"], &[], &[], &[]),
        UiNodeKind::TreeItem => schema(
            &[
                "value",
                "expanded",
                "selected",
                "hover_label",
                "context_menu",
            ],
            &[],
            &["title", "children", "actions"],
            &["title"],
        ),
        UiNodeKind::Table => schema(
            &[
                "columns",
                "rows",
                "empty_state",
                "selection",
                "row_action",
                "activation",
            ],
            &["columns"],
            &[],
            &[],
        ),
        UiNodeKind::Button => schema(
            &[
                "label",
                "action",
                "tone",
                "variant",
                "shortcut",
                "hover_label",
                "context_menu",
                "toolbar_overflow",
            ],
            &[],
            &[],
            &[],
        ),
        UiNodeKind::IconButton => schema(
            &[
                "label",
                "icon",
                "action",
                "tone",
                "variant",
                "shortcut",
                "hover_label",
                "context_menu",
                "toolbar_overflow",
            ],
            &["icon"],
            &[],
            &[],
        ),
        UiNodeKind::Menu => schema(&["label"], &[], &["items"], &["items"]),
        UiNodeKind::MenuItem => schema(
            &[
                "label",
                "action",
                "icon",
                "shortcut",
                "hover_label",
                "context_menu",
                "toolbar_overflow",
            ],
            &[],
            &[],
            &[],
        ),
        UiNodeKind::Dialog => schema(
            &["title", "presentation"],
            &["title"],
            &["body", "actions"],
            &["body"],
        ),
        UiNodeKind::TextInput => schema(
            &[
                "name",
                "label",
                "description",
                "value",
                "default",
                "placeholder",
                "required",
                "disabled",
                "loading",
                "error",
                "validation",
            ],
            &["name", "label"],
            &[],
            &[],
        ),
        UiNodeKind::Textarea => schema(
            &[
                "name",
                "label",
                "description",
                "value",
                "default",
                "placeholder",
                "required",
                "disabled",
                "loading",
                "error",
                "validation",
            ],
            &["name", "label"],
            &[],
            &[],
        ),
        UiNodeKind::Checkbox => schema(
            &[
                "name",
                "label",
                "description",
                "checked",
                "default",
                "required",
                "disabled",
                "loading",
                "error",
                "validation",
            ],
            &["name", "label"],
            &[],
            &[],
        ),
        UiNodeKind::Select => schema(
            &[
                "name",
                "label",
                "description",
                "value",
                "selected",
                "default",
                "required",
                "disabled",
                "loading",
                "error",
                "validation",
                "options_source",
            ],
            &["name", "label"],
            &["options"],
            // options xor options_source enforced in validate_prop_combinations
            &[],
        ),
        UiNodeKind::SelectOption => schema(
            &["value", "label", "disabled"],
            &["value", "label"],
            &[],
            &[],
        ),
        UiNodeKind::TerminalView => schema(&["session_id", "title"], &["session_id"], &[], &[]),
        UiNodeKind::ConnectionCodeView => {
            schema(&["code", "label", "copy_value"], &["code"], &[], &[])
        }
        UiNodeKind::Iframe => schema(
            &["src", "title", "sandbox", "allow", "bridge"],
            &["src", "title"],
            &[],
            &[],
        ),
        UiNodeKind::Custom => schema(
            &["namespace", "component", "reason"],
            &["namespace", "component", "reason"],
            &["fallback"],
            &["fallback"],
        ),
    }
}

fn schema(
    allowed_props: &[&'static str],
    required_props: &[&'static str],
    allowed_slots: &[&'static str],
    required_slots: &[&'static str],
) -> UiNodeSchema {
    UiNodeSchema {
        allowed_props: allowed_props.iter().copied().collect(),
        required_props: required_props.to_vec(),
        allowed_slots: allowed_slots.iter().copied().collect(),
        required_slots: required_slots.to_vec(),
    }
}

struct UiNodeSchema {
    allowed_props: BTreeSet<&'static str>,
    required_props: Vec<&'static str>,
    allowed_slots: BTreeSet<&'static str>,
    required_slots: Vec<&'static str>,
}

fn is_false(value: &bool) -> bool {
    !*value
}
