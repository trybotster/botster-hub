// Generated from crates/botster-hub-client Rust serde DTOs.
// Regenerate/check with: ./test.sh -p botster-hub-client
import type { PackageSurfaceDescriptor, UiActionRequest, UiActionResult, UiNode } from "@trybotster/ui-contract";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface AesGcmEnvelope {
  nonce: string;
  ciphertext: string;
  version: number;
}

export interface DaemonLocalWebrtcDeliveryChunk {
  version: number;
  delivery_kind: DaemonLocalWebrtcDeliveryKind;
  message_id: string;
  chunk_index: number;
  chunk_count: number;
  total_bytes: number;
  payload: string;
}

export type DaemonLocalWebrtcDeliveryKind =
  | "daemon_response"
  | "daemon_entity_frame";

export interface DaemonHello {
  protocol: string;
  compatibility: DaemonCompatibilityRequirement;
}

export interface DaemonHelloAck {
  protocol: string;
  compatibility: DaemonCompatibility;
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonCompatibility {
  protocol: string;
  protocol_version: number;
  features: string[];
  conformance_fixture_revision: number;
}

export interface DaemonCompatibilityRequirement {
  protocol: string;
  protocol_version: number;
  required_features: string[];
  minimum_conformance_fixture_revision: number;
  client_name: string;
}

export type DaemonRequest =
  | { type: "status" }
  | { type: "check_hub_update" }
  | { type: "start_hub_update"; scope: DaemonHubUpdateScope }
  | { type: "get_hub_update_execution" }
  | { type: "list_sessions" }
  | { type: "subscribe_entities"; entity_type: string; subscription_id: string }
  | { type: "unsubscribe_entities"; subscription_id: string }
  | { type: "remove_session"; session_id: string }
  | { type: "whoami"; caller_session_id: string | null }
  | { type: "post_message"; caller_session_id: string | null; target_session_id: string; envelope_id: string | null; body: string }
  | { type: "receive_messages"; caller_session_id: string; after: number | null; limit: number }
  | { type: "ack_message"; caller_session_id: string; envelope_id: string }
  | { type: "notify_session"; session_id: string; data: string }
  | { type: "spawn"; session_id: string; command: string }
  | { type: "attach"; session_id: string; subscription_id: string }
  | { type: "detach"; session_id: string; subscription_id: string }
  | { type: "send_input"; session_id: string; data: string }
  | { type: "mode_gated_input"; session_id: string; data: string; mode_generation: number; mode_revision: number }
  | { type: "resize"; session_id: string; rows: number; cols: number }
  | { type: "shutdown_session"; session_id: string }
  | { type: "drain"; session_id: string; subscription_id?: string }
  | { type: "read_screen"; session_id: string }
  | { type: "read_mode_flags"; session_id: string }
  | { type: "capture_snapshot"; session_id: string }
  | { type: "list_session_types" }
  | { type: "list_session_types_for_target"; target_id: string }
  | { type: "show_session_type"; session_type_id: string }
  | { type: "show_session_type_definition"; session_type_id: string }
  | { type: "create_session_type"; source: DaemonSessionTypeMutationSource; definition: DaemonSessionTypeDefinition }
  | { type: "update_session_type"; source: DaemonSessionTypeMutationSource; definition: DaemonSessionTypeDefinition }
  | { type: "delete_session_type"; source: DaemonSessionTypeMutationSource; session_type_id: string }
  | { type: "resolve_session_type"; session_type_id: string; request: DaemonSessionTypeRequest }
  | { type: "spawn_session_type"; session_type_id: string; session_id: string; request: DaemonSessionTypeRequest }
  | { type: "read_session_context"; session_id: string; context_id?: string | null; key?: string | null }
  | { type: "list_spawn_targets" }
  | { type: "show_spawn_target"; target_id: string }
  | { type: "create_spawn_target"; target_id?: string | null; label?: string | null; root: string; enabled?: boolean; kind?: string | null; base_ref?: string | null; metadata?: Record<string, string> }
  | { type: "update_spawn_target"; target_id: string; label?: string | null; root?: string | null; enabled?: boolean | null; kind?: string | null; base_ref?: string | null; metadata?: Record<string, string> | null }
  | { type: "delete_spawn_target"; target_id: string }
  | { type: "validate_spawn_target"; target_id: string }
  | { type: "list_worktrees" }
  | { type: "show_worktree"; worktree_id: string }
  | { type: "create_worktree"; worktree_id?: string | null; target_id: string; label?: string | null; path: string; metadata?: Record<string, string> }
  | { type: "delete_worktree"; worktree_id: string }
  | { type: "list_apps" }
  | { type: "resolve_app_launch"; package_name: string; entrypoint_id: string }
  | { type: "resolve_package_route"; package_name: string; route_id: string }
  | { type: "list_package_navigation" }
  | { type: "list_packages" }
  | { type: "list_available_packages"; registry_path: string }
  | { type: "inspect_available_package"; registry_path: string; entry_id: string }
  | { type: "preview_package_install"; registry_path: string; entry_id: string }
  | { type: "install_package_registry_entry"; registry_path: string; entry_id: string }
  | { type: "install_package_local_path"; path: string }
  | { type: "check_package_update"; package_name: string }
  | { type: "preview_package_update"; package_name: string; pin: DaemonPackagePin }
  | { type: "apply_package_update"; package_name: string; pin: DaemonPackagePin }
  | { type: "show_package"; package_name: string }
  | { type: "set_package_configuration"; package_name: string; values: Record<string, JsonValue> }
  | { type: "reload_package"; package_name: string }
  | { type: "refresh_local_packages" }
  | { type: "enable_package_local_path"; path: string }
  | { type: "enable_package"; package_name: string }
  | { type: "disable_package"; package_name: string }
  | { type: "remove_package"; package_name: string }
  | { type: "start_package_entrypoint"; package_name: string; entrypoint_id: string; environment_overrides?: Record<string, string> }
  | { type: "issue_local_webrtc_bootstrap"; package_name: string; entrypoint_id: string; origin: string }
  | { type: "stop_package_entrypoint"; package_name: string; entrypoint_id: string }
  | { type: "restart_package_entrypoint"; package_name: string; entrypoint_id: string }
  | { type: "package_entrypoint_status"; package_name: string; entrypoint_id: string }
  | { type: "plugin_lifecycle_status" }
  | { type: "plugin_mcp_list_tools" }
  | { type: "plugin_mcp_call_tool"; name: string; arguments: JsonValue }
  | { type: "plugin_surface_render"; package_name: string; surface_id: string; payload: JsonValue }
  | { type: "plugin_surface_action"; package_name: string; request: UiActionRequest }
  | { type: "local_webrtc_signal"; grant_id: string; grant_secret: string; origin: string; offer: JsonValue }
  | { type: "daemon_shutdown" };

export interface DaemonResponse {
  kind: DaemonResponseKind;
  status: DaemonStatus | null;
  sessions: DaemonSession[];
  session_types?: DaemonSessionType[];
  session_type_definition?: DaemonSessionTypeEditableDefinition | null;
  resolved_session_type?: DaemonResolvedSessionType | null;
  session_context?: DaemonSessionContext | null;
  read_screen?: DaemonReadScreen | null;
  mode_flags?: DaemonModeFlags | null;
  mode_gated_input?: DaemonModeGatedInputResult | null;
  capture_snapshot?: DaemonCaptureSnapshot | null;
  spawn_targets?: DaemonSpawnTarget[];
  spawn_target_validation?: DaemonSpawnTargetValidation | null;
  worktrees?: DaemonWorktree[];
  apps?: DaemonApp[];
  resolved_app_launch?: DaemonResolvedAppLaunch | null;
  resolved_package_route?: DaemonPackageRouteDescriptor | null;
  package_navigation?: DaemonPackageNavigationEntry[];
  packages: DaemonPackage[];
  available_packages?: DaemonAvailablePackage[];
  install_plan?: DaemonPackageInstallPlan | null;
  update_status?: DaemonPackageUpdateStatus | null;
  hub_update?: DaemonHubUpdate | null;
  hub_update_execution?: DaemonHubUpdateExecution | null;
  package_decision: DaemonPackageDecision | null;
  lifecycle: DaemonPluginLifecycle[];
  plugin_worker_counters?: DaemonPluginWorkerCounters | null;
  plugin_resource_counters?: DaemonPluginResourceCounters | null;
  plugin_tools: JsonValue[];
  plugin_tool_result: JsonValue;
  plugin_surface?: DaemonPluginSurface | null;
  plugin_action_result?: UiActionResult;
  local_webrtc_bootstrap?: DaemonLocalWebrtcBootstrap | null;
  local_webrtc_answer?: DaemonLocalWebrtcAnswer | null;
  events: DaemonEvent[];
  cleanup: DaemonSessionCleanup | null;
  coordination: DaemonCoordination | null;
  error: DaemonOperatorError | null;
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonReadScreen {
  session_id: string;
  text: string;
}

export interface DaemonModeFlags {
  session_id: string;
  kitty_enabled: boolean;
  cursor_visible: boolean;
  bracketed_paste: boolean;
  mouse_mode: number;
  alt_screen: boolean;
  focus_reporting: boolean;
  application_cursor: boolean;
  mode_generation: number;
  mode_revision: number;
}

export interface DaemonModeGatedInputResult {
  session_id: string;
  admitted: boolean;
  bytes_written: number;
  kitty_enabled: boolean;
  cursor_visible: boolean;
  bracketed_paste: boolean;
  mouse_mode: number;
  alt_screen: boolean;
  focus_reporting: boolean;
  application_cursor: boolean;
  mode_generation: number;
  mode_revision: number;
  error_kind?: string | null;
}

export interface DaemonCaptureSnapshot {
  session_id: string;
  rows: number;
  cols: number;
  payload_format?: string | null;
  payload_bytes: number;
}

export interface DaemonPluginSurface {
  package_name: string;
  surface_id: string;
  body: UiNode;
  ui_tree_snapshot?: DaemonUiTreeSnapshot | null;
}

export interface DaemonUiTreeSnapshot {
  package_name: string;
  surface_id: string;
  body: UiNode;
}

export interface DaemonWorktreeLifecycleEvent {
  event: string;
  worktree_id?: string | null;
  target_id?: string | null;
  status?: string | null;
  label?: string | null;
  display_path?: string | null;
  failure_kind?: string | null;
  message?: string | null;
}

export type DaemonResponseKind =
  | "status"
  | "hub_update"
  | "hub_update_execution"
  | "sessions"
  | "entity_subscribed"
  | "entity_unsubscribed"
  | "session_removed"
  | "spawned"
  | "events"
  | "session_types"
  | "session_type_definition"
  | "resolved_session_type"
  | "session_context"
  | "read_screen"
  | "read_mode_flags"
  | "mode_gated_input"
  | "capture_snapshot"
  | "spawn_targets"
  | "spawn_target_validation"
  | "worktrees"
  | "apps"
  | "resolved_app_launch"
  | "resolved_package_route"
  | "package_navigation"
  | "packages"
  | "available_packages"
  | "package_install_plan"
  | "package_update_status"
  | "package_decision"
  | "plugin_lifecycle"
  | "plugin_mcp_tools"
  | "plugin_mcp_tool_result"
  | "plugin_surface"
  | "plugin_action_result"
  | "local_webrtc_bootstrap"
  | "local_webrtc_answer"
  | "session_cleanup"
  | "identity"
  | "message_posted"
  | "messages"
  | "message_acked"
  | "session_notified"
  | "operator_error"
  | "shutdown";

export interface DaemonCoordination {
  identity: DaemonIdentity | null;
  publish: DaemonEnvelopePublish | null;
  messages: DaemonEnvelope[];
  next_cursor: number | null;
  ack: DaemonEnvelopeAck | null;
  notify: DaemonNotify | null;
}

export interface DaemonSpawnTarget {
  target_id: string;
  label: string;
  root: string;
  enabled: boolean;
  kind: string;
  base_ref?: string | null;
  metadata?: Record<string, string>;
}

export interface DaemonSpawnTargetValidation {
  target_id: string;
  ok: boolean;
  status: string;
}

export interface DaemonWorktree {
  worktree_id: string;
  target_id: string;
  label: string;
  path: string;
  status: string;
  management: string;
  git?: DaemonWorktreeGitMetadata | null;
  metadata?: Record<string, string>;
}

export interface DaemonWorktreeGitMetadata {
  repository_root: string;
  branch?: string | null;
  head?: string | null;
}

export interface DaemonIdentity {
  client_id: string;
  role: string;
  identity_source: string;
  caller_session_id: string | null;
  host_id: string;
  host_display_name: string;
}

export interface DaemonEnvelopePublish {
  deliveries: DaemonEnvelopeDelivery[];
}

export interface DaemonEnvelopeDelivery {
  envelope_id: string;
  target: string;
  cursor: number;
  status: string;
}

export interface DaemonEnvelope {
  envelope_id: string;
  source: string;
  content_type: string;
  body: string;
  created_at: number;
  cursor: number | null;
}

export interface DaemonEnvelopeAck {
  envelope_id: string | null;
  target: string | null;
  cursor: number | null;
  status: string;
}

export interface DaemonNotify {
  decision: string;
  state_count: number;
  states: string[];
}

export interface DaemonSessionTypeRequest {
  target_id?: string | null;
  cwd?: string | null;
  environment?: Record<string, string>;
  context: DaemonSessionTypeContextInput;
}

export interface DaemonSessionTypeContextInput {
  worktree_path?: string | null;
  repo_path?: string | null;
  branch_name?: string | null;
  prompt?: string | null;
  ticket_id?: string | null;
  workspace_id?: string | null;
  metadata?: Record<string, string>;
}

export type DaemonSessionTypeMutationSource =
  | { source: "device" }
  | { source: "repo"; target_id: string }
  | { source: "package"; package_name: string };

export type DaemonSessionTypeWorkingDirectory =
  | { policy: "package_root" }
  | { policy: "relative"; path: string };

export type DaemonSessionTypeExecution =
  | { mode: "relative_executable" }
  | { mode: "shell_command" };

export interface DaemonSessionTypeDefinition {
  id: string;
  label: string;
  description?: string | null;
  icon?: string | null;
  role: string;
  interaction: string;
  traits?: string[];
  lifecycle: string;
  execution?: DaemonSessionTypeExecution;
  command: string;
  args?: string[];
  working_directory?: DaemonSessionTypeWorkingDirectory;
  environment?: Record<string, string>;
  allowed_environment_overrides?: string[];
  context?: string[];
  target_id?: string | null;
}

export interface DaemonSessionTypeSource {
  kind: string;
  name: string;
}

export interface DaemonSessionTypeEditableDefinition {
  session_type_id: string;
  source: DaemonSessionTypeMutationSource;
  definition: DaemonSessionTypeDefinition;
}

export interface DaemonSessionType {
  session_type_id: string;
  source_name: string;
  id: string;
  source: string;
  editable: boolean;
  overridden_sources?: DaemonSessionTypeSource[];
  diagnostics?: string[];
  label: string;
  description?: string | null;
  icon?: string | null;
  role: string;
  interaction: string;
  traits?: string[];
  lifecycle: string;
  execution?: DaemonSessionTypeExecution;
  command: string;
  args?: string[];
  working_directory_policy: string;
  allowed_environment_overrides?: string[];
  context_keys?: string[];
  target_id: string;
  available: boolean;
}

export interface DaemonResolvedSessionType {
  session_type: DaemonSessionType;
  session_id: string;
  executable: string;
  arguments?: string[];
  working_directory: string;
  environment?: Record<string, string>;
  context_id: string;
  context_keys?: string[];
}

export interface DaemonSessionContext {
  context_id: string;
  session_id: string;
  values: Record<string, string>;
}

export interface DaemonApp {
  package_name: string;
  app_id: string;
  entrypoint_id: string;
  kind: string;
  launch_mode: string;
  lifecycle_state: string;
  diagnostics?: DaemonPackageDiagnostic[];
  actions?: DaemonPackageActionState[];
  blocked_reasons?: string[];
  launch_target: DaemonAppLaunchTarget;
  route?: DaemonPackageRouteDescriptor | null;
}

export interface DaemonAppLaunchTarget {
  kind: string;
  local_url?: string | null;
}

export interface DaemonResolvedAppLaunch {
  package_name: string;
  app_id: string;
  entrypoint_id: string;
  kind: string;
  launch_mode: string;
  command: string;
  args?: string[];
  working_directory: string;
  environment?: Record<string, string>;
}

export interface DaemonPackageRouteDescriptor {
  package_name: string;
  route_id: string;
  route_path: string;
  target: DaemonPackageRouteTarget;
  title: string;
  label: string;
  app_id?: string | null;
  surface_id?: string | null;
  icon?: string | null;
  category?: string | null;
  layout_mode: string;
  required_capabilities?: DaemonCapability[];
  enabled: boolean;
  blocked: boolean;
  diagnostics?: DaemonPackageDiagnostic[];
  supports_settings: boolean;
}

export interface DaemonPackageRouteTarget {
  kind: string;
  entrypoint_id?: string | null;
  surface_id?: string | null;
}

export interface DaemonPackageNavigationEntry {
  package_name: string;
  item_id: string;
  label: string;
  icon?: string | null;
  description?: string | null;
  route_id: string;
  route_path: string;
  target: DaemonPackageRouteTarget;
  source: DaemonPackageNavigationSource;
  enabled: boolean;
  blocked: boolean;
  diagnostics?: DaemonPackageDiagnostic[];
}

export interface DaemonPackageNavigationSource {
  kind: string;
  surface_id?: string | null;
  entrypoint_id?: string | null;
}

export interface DaemonLocalWebrtcBootstrap {
  grant_id: string;
  grant_secret: string;
  package_name: string;
  entrypoint_id: string;
  expected_origin: string;
  expires_at: number;
  signaling_transport: string;
  data_plane: string;
  ordered: boolean;
  max_retransmits?: number | null;
  max_packet_lifetime_ms?: number | null;
}

export interface DaemonLocalWebrtcAnswer {
  grant_id: string;
  answer: JsonValue;
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonPackage {
  package_name: string;
  version: string;
  classification: string;
  source_kind: string;
  state: string;
  requested_capabilities: DaemonCapability[];
  surfaces?: PackageSurfaceDescriptor[];
  routes?: DaemonPackageRouteDescriptor[];
  runnable_entrypoints: DaemonPackageRunnableEntrypoint[];
  configuration: DaemonPackageConfiguration;
  availability: DaemonPackageAvailability;
  dependency_availability?: DaemonPackageDependencyAvailability[];
  feature_availability?: DaemonPackageFeatureAvailability[];
  actions?: DaemonPackageActionState[];
  provider_profile_admitted: boolean;
}

export interface DaemonPackageAvailability {
  state: DaemonPackageAvailabilityState;
  reasons?: DaemonPackageAvailabilityReason[];
}

export type DaemonPackageAvailabilityState =
  | "available"
  | "blocked";

export interface DaemonPackageAvailabilityReason {
  reason: string;
  action: string;
  package_name?: string | null;
  capability?: DaemonCapability | null;
  requirement?: string | null;
}

export interface DaemonPackageDependencyAvailability {
  id: string;
  package_name: string;
  state: DaemonPackageAvailabilityState;
  reasons?: DaemonPackageAvailabilityReason[];
}

export interface DaemonPackageFeatureAvailability {
  id: string;
  state: DaemonPackageAvailabilityState;
  reasons?: DaemonPackageAvailabilityReason[];
}

export interface DaemonCapability {
  surface: string;
  scope: string | null;
}

export interface DaemonAvailablePackage {
  entry_id: string;
  package_name: string;
  version: string;
  classification: string;
  source_kind: string;
  source_label: string;
  first_party: boolean;
  state: string;
  requested_capabilities: DaemonCapability[];
  compatibility: DaemonPackageCompatibility;
  pin?: DaemonPackagePin | null;
  actions?: DaemonPackageActionState[];
}

export interface DaemonPackageActionState {
  action_id: string;
  status: DaemonPackageActionStatus;
  reason?: string | null;
  diagnostics?: DaemonPackageDiagnostic[];
  required_references?: DaemonPackageActionRequiredReference[];
  request?: DaemonPackageActionRequest | null;
}

export type DaemonPackageActionStatus =
  | "available"
  | "blocked"
  | "unavailable";

export interface DaemonPackageActionRequiredReference {
  kind: string;
  key: string;
}

export interface DaemonPackageActionRequest {
  request_type: string;
  pin?: DaemonPackagePin | null;
  package_name?: string | null;
  entry_id?: string | null;
  entrypoint_id?: string | null;
  registry_path?: string | null;
}

export interface DaemonPackageInstallPlan {
  entry: DaemonAvailablePackage;
  effects: DaemonPackageInstallEffect[];
  diagnostics: DaemonPackageDiagnostic[];
  mutates_registry: boolean;
  starts_entrypoints: boolean;
}

export interface DaemonPackageInstallEffect {
  kind: string;
  message: string;
}

export interface DaemonPackageUpdateStatus {
  package_name: string;
  update_available: boolean;
  reload_required: boolean;
  restart_required: boolean;
  pin?: DaemonPackagePin | null;
  diagnostics?: DaemonPackageDiagnostic[];
  actions?: DaemonPackageActionState[];
}

export interface DaemonPackageCompatibility {
  botster_requirement: string;
  result: string;
  diagnostics: string[];
}

export interface DaemonPackagePin {
  revision: string;
  branch?: string | null;
  tag?: string | null;
  rev?: string | null;
  checksum?: string | null;
  update_policy: string;
}

export interface DaemonPackageConfiguration {
  schema?: JsonValue | null;
  effective_values?: Record<string, JsonValue>;
  missing_required?: string[];
  diagnostics?: DaemonPackageDiagnostic[];
}

export interface DaemonPackageRunnableEntrypoint {
  id: string;
  kind: string;
  launch_mode: string;
  command: string;
  args: string[];
  working_directory: DaemonPackageWorkingDirectory;
  environment: DaemonPackageEnvironmentRequirement[];
  capabilities: DaemonCapability[];
  may_supervise: boolean;
  process: DaemonPackageProcess;
  actions?: DaemonPackageActionState[];
}

export interface DaemonPackageWorkingDirectory {
  policy: string;
  path: string | null;
}

export interface DaemonPackageEnvironmentRequirement {
  name: string;
  required: boolean;
  default: string | null;
  description: string | null;
}

export interface DaemonPackageProcess {
  state: string;
  pid?: number;
  started_at?: number;
  exited_at?: number;
  exit_status?: string;
  diagnostics: DaemonPackageDiagnostic[];
}

export interface DaemonPackageDiagnostic {
  kind: string;
  message: string;
}

export interface DaemonPackageDecision {
  package_name: string;
  action: string;
  state: string;
  classification: string;
}

export interface DaemonPluginLifecycle {
  package_name: string;
  state: string;
  loaded: boolean;
}

export interface DaemonPluginWorkerCounters {
  configured_queue_capacity: number;
  configured_executor_concurrency: number;
  live_plugin_executors: number;
  live_executor_workers: number;
  queued_jobs: number;
  in_flight_jobs: number;
}

export interface DaemonPluginResourceCounters {
  active_timer_resources: number;
}

export interface DaemonStatus {
  lifecycle_state: string;
  compatibility: DaemonCompatibility;
  software: DaemonSoftwareIdentity;
  installation: DaemonInstallationIdentity;
  host_id: string;
  host_display_name: string;
  schema_version: number;
  data_dir_configured: boolean;
  core_initialized: boolean;
  state_source: string;
  package_count: number;
  enabled_package_count: number;
  provider_count: number;
  enabled_provider_count: number;
  session_count: number;
  recovered_sessions: string[];
  stale_sessions: string[];
  lifecycle_counters?: DaemonLifecycleCounters;
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonSoftwareIdentity {
  product_id: string;
  product_name: string;
  version: string;
  build_revision?: string | null;
}

export type DaemonInstallationMode =
  | "development"
  | "unmanaged"
  | "managed";

export interface DaemonInstallationIdentity {
  mode: DaemonInstallationMode;
  provenance: string;
  release_channel?: string | null;
  provider?: string | null;
  diagnostics?: DaemonInstallationDiagnostic[];
}

export interface DaemonInstallationDiagnostic {
  kind: string;
  message: string;
}

export type DaemonHubUpdateState =
  | "current"
  | "available"
  | "unavailable";

export interface DaemonHubUpdate {
  state: DaemonHubUpdateState;
  current_version: string;
  available_version?: string | null;
  build_revision?: string | null;
  reason?: string | null;
  action?: string | null;
}

export type DaemonHubUpdateScope =
  | "core"
  | "all";

export type DaemonHubUpdateExecutionState =
  | "started"
  | "running"
  | "complete"
  | "failed";

export interface DaemonHubUpdateExecution {
  update_id: string;
  scope: DaemonHubUpdateScope;
  state: DaemonHubUpdateExecutionState;
  updater_pid: number;
  error?: string | null;
}

export interface DaemonLifecycleCounters {
  accepted_connections: number;
  rejected_connections: number;
  live_connections: number;
  high_water_live_connections: number;
  live_entity_subscriptions: number;
  high_water_entity_subscriptions: number;
  live_attach_subscriptions: number;
  high_water_attach_subscriptions: number;
  reconnect_registrations: number;
  cleanup_completed: number;
  cleanup_failed: number;
  cleanup_by_reason?: Record<string, number>;
  reconciliation_wakes: number;
  lifecycle_change_reads: number;
  lifecycle_baseline_reads: number;
  lifecycle_resync_reads: number;
  lifecycle_session_drains: number;
  entity_delivery_attempts: number;
  entity_delivery_successes: number;
  entity_delivery_overflows: number;
  entity_delivery_failures: number;
  stalled_writes: number;
}

export interface DaemonSession {
  session_id: string;
  lifecycle: string;
}

export interface DaemonSessionEntity {
  session_uuid: string;
  registry_state: string;
  lifecycle?: string | null;
  lifecycle_class: string;
  rows: number;
  cols: number;
  updated_at: number;
  exit_code?: number | null;
  failure_reason?: string | null;
  session_type_id?: string | null;
  session_type_source?: string | null;
  role?: string | null;
  traits?: string[];
  interaction?: string | null;
  session_type_lifecycle?: string | null;
}

export type DaemonEntityFrame =
  | { type: "entity_snapshot"; subscription_id: string; entity_type: string; snapshot_seq: number; items: JsonValue[]; resync_reason?: string | null }
  | { type: "entity_upsert"; subscription_id: string; entity_type: string; snapshot_seq: number; id: string; entity: JsonValue }
  | { type: "entity_patch"; subscription_id: string; entity_type: string; snapshot_seq: number; id: string; patch: JsonValue }
  | { type: "entity_remove"; subscription_id: string; entity_type: string; snapshot_seq: number; id: string }
  | { type: "entity_error"; subscription_id: string; entity_type: string; code: string; message: string };

export interface DaemonSessionCleanup {
  session_id: string;
  outcome: string;
}

export interface DaemonOperatorError {
  code: string;
  request_id: string;
  operation: string;
  message: string;
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonDiagnostic {
  kind: DaemonDiagnosticKind;
  operation: string | null;
  feature: string | null;
  message: string | null;
}

export type DaemonDiagnosticKind =
  | "connected"
  | "disconnected"
  | "compatibility_mismatch"
  | "unsupported_feature"
  | "terminal_stream_unavailable"
  | "worker_compatibility"
  | "action_failure"
  | "daemon_startup_failure"
  | "backpressure";

export type DaemonEvent =
  | { type: "session_lifecycle"; session_id: string; state: string }
  | { type: "terminal_output"; session_id: string; subscription_id: string; payload_base64: string; payload_encoding: "base64"; bytes: number }
  | { type: "snapshot"; session_id: string; subscription_id: string; payload_base64: string; payload_encoding: "base64"; bytes: number }
  | { type: "scrollback"; session_id: string; subscription_id: string; payload_base64: string; payload_encoding: "base64"; bytes: number }
  | { type: "process_exit"; session_id: string; subscription_id: string; code: number | null }
  | { type: "attach_state"; session_id: string; subscription_id: string; state: string }
  | { type: "runtime_observation"; kind: string }
  | { type: "worktree_lifecycle"; event: DaemonWorktreeLifecycleEvent };
