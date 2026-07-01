// Generated from crates/botster-hub-client Rust serde DTOs.
// Regenerate/check with: ./test.sh -p botster-hub-client

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface AesGcmEnvelope {
  nonce: string;
  ciphertext: string;
  version: number;
}

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
  minimum_protocol_version: number;
  required_features: string[];
  minimum_conformance_fixture_revision: number;
  client_name: string;
}

export type DaemonRequest =
  | { type: "status" }
  | { type: "list_sessions" }
  | { type: "whoami"; caller_session_id: string | null }
  | { type: "post_message"; caller_session_id: string | null; target_session_id: string; envelope_id: string | null; body: string }
  | { type: "receive_messages"; caller_session_id: string; after: number | null; limit: number }
  | { type: "ack_message"; caller_session_id: string; envelope_id: string }
  | { type: "notify_session"; session_id: string; data: string }
  | { type: "spawn"; session_id: string; command: string }
  | { type: "attach"; session_id: string; subscription_id: string }
  | { type: "detach"; session_id: string; subscription_id: string }
  | { type: "send_input"; session_id: string; data: string }
  | { type: "resize"; session_id: string; rows: number; cols: number }
  | { type: "shutdown_session"; session_id: string }
  | { type: "drain"; session_id: string }
  | { type: "list_session_templates" }
  | { type: "show_session_template"; template_id: string }
  | { type: "resolve_session_template"; template_id: string; request: DaemonSessionTemplateRequest }
  | { type: "spawn_session_template"; template_id: string; session_id: string; request: DaemonSessionTemplateRequest }
  | { type: "read_session_context"; session_id: string; context_id?: string | null; key?: string | null }
  | { type: "list_apps" }
  | { type: "resolve_app_launch"; package_name: string; entrypoint_id: string }
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
  | { type: "enable_package_local_path"; path: string }
  | { type: "enable_package"; package_name: string }
  | { type: "disable_package"; package_name: string }
  | { type: "remove_package"; package_name: string }
  | { type: "start_package_entrypoint"; package_name: string; entrypoint_id: string; environment_overrides?: Record<string, string> }
  | { type: "stop_package_entrypoint"; package_name: string; entrypoint_id: string }
  | { type: "restart_package_entrypoint"; package_name: string; entrypoint_id: string }
  | { type: "package_entrypoint_status"; package_name: string; entrypoint_id: string }
  | { type: "plugin_lifecycle_status" }
  | { type: "plugin_mcp_list_tools" }
  | { type: "plugin_mcp_call_tool"; name: string; arguments: JsonValue }
  | { type: "plugin_surface_render"; package_name: string; surface_id: string; payload: JsonValue }
  | { type: "plugin_surface_action"; package_name: string; surface_id: string; action_id: string; payload: JsonValue }
  | { type: "local_webrtc_signal"; grant_id: string; grant_secret: string; origin: string; offer: JsonValue }
  | { type: "daemon_shutdown" };

export interface DaemonResponse {
  kind: DaemonResponseKind;
  status: DaemonStatus | null;
  sessions: DaemonSession[];
  session_templates?: DaemonSessionTemplate[];
  resolved_session_template?: DaemonResolvedSessionTemplate | null;
  session_context?: DaemonSessionContext | null;
  apps?: DaemonApp[];
  resolved_app_launch?: DaemonResolvedAppLaunch | null;
  packages: DaemonPackage[];
  available_packages?: DaemonAvailablePackage[];
  install_plan?: DaemonPackageInstallPlan | null;
  update_status?: DaemonPackageUpdateStatus | null;
  package_decision: DaemonPackageDecision | null;
  lifecycle: DaemonPluginLifecycle[];
  plugin_tools: JsonValue[];
  plugin_tool_result: JsonValue;
  plugin_surface?: JsonValue;
  plugin_action_result?: JsonValue;
  local_webrtc_bootstrap?: DaemonLocalWebrtcBootstrap | null;
  local_webrtc_answer?: DaemonLocalWebrtcAnswer | null;
  events: DaemonEvent[];
  cleanup: DaemonSessionCleanup | null;
  coordination: DaemonCoordination | null;
  error: DaemonOperatorError | null;
  diagnostics?: DaemonDiagnostic[];
}

export type DaemonResponseKind =
  | "status"
  | "sessions"
  | "spawned"
  | "events"
  | "session_templates"
  | "resolved_session_template"
  | "session_context"
  | "apps"
  | "resolved_app_launch"
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

export interface DaemonSessionTemplateRequest {
  target_id?: string | null;
  cwd?: string | null;
  environment?: Record<string, string>;
  context: DaemonSessionTemplateContextInput;
}

export interface DaemonSessionTemplateContextInput {
  worktree_path?: string | null;
  repo_path?: string | null;
  branch_name?: string | null;
  prompt?: string | null;
  ticket_id?: string | null;
  workspace_id?: string | null;
  metadata?: Record<string, string>;
}

export interface DaemonSessionTemplate {
  template_id: string;
  package_name: string;
  id: string;
  source: string;
  command: string;
  args?: string[];
  working_directory_policy: string;
  allowed_environment_overrides?: string[];
  context_keys?: string[];
  target_id: string;
  available: boolean;
}

export interface DaemonResolvedSessionTemplate {
  template: DaemonSessionTemplate;
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
  surfaces?: DaemonPackageSurfaceDescriptor[];
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
  hub_version: string;
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

export interface DaemonPackageSurfaceDescriptor {
  id: string;
  kind: string;
  title: string;
  description?: string | null;
  icon?: string | null;
  order?: number | null;
  category?: string | null;
  supports?: string[];
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

export interface DaemonStatus {
  lifecycle_state: string;
  compatibility: DaemonCompatibility;
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
  diagnostics?: DaemonDiagnostic[];
}

export interface DaemonSession {
  session_id: string;
  lifecycle: string;
}

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
  | "action_failure"
  | "daemon_startup_failure"
  | "backpressure";

export type DaemonEvent =
  | { type: "session_lifecycle"; session_id: string; state: string }
  | { type: "terminal_output"; session_id: string; subscription_id: string; data: string }
  | { type: "snapshot"; session_id: string; subscription_id: string; data: string; bytes: number }
  | { type: "scrollback"; session_id: string; subscription_id: string; data: string; bytes: number }
  | { type: "process_exit"; session_id: string; subscription_id: string; code: number | null }
  | { type: "attach_state"; session_id: string; subscription_id: string; state: string }
  | { type: "runtime_observation"; kind: string };
