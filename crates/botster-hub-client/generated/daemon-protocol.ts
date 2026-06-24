// Generated from crates/botster-hub-client Rust serde DTOs.
// Regenerate/check with: ./test.sh -p botster-hub-client

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface DaemonHello {
  protocol: string;
  compatibility: DaemonCompatibilityRequirement;
}

export interface DaemonHelloAck {
  protocol: string;
  compatibility: DaemonCompatibility;
  diagnostics: DaemonDiagnostic[];
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
  | { type: "list_packages" }
  | { type: "install_package_local_path"; path: string }
  | { type: "show_package"; package_name: string }
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
  | { type: "daemon_shutdown" };

export interface DaemonResponse {
  kind: DaemonResponseKind;
  status: DaemonStatus | null;
  sessions: DaemonSession[];
  packages: DaemonPackage[];
  package_decision: DaemonPackageDecision | null;
  lifecycle: DaemonPluginLifecycle[];
  plugin_tools: JsonValue[];
  plugin_tool_result: JsonValue;
  plugin_surface?: JsonValue;
  plugin_action_result?: JsonValue;
  events: DaemonEvent[];
  cleanup: DaemonSessionCleanup | null;
  coordination: DaemonCoordination | null;
  error: DaemonOperatorError | null;
  diagnostics: DaemonDiagnostic[];
}

export type DaemonResponseKind =
  | "status"
  | "sessions"
  | "spawned"
  | "events"
  | "packages"
  | "package_decision"
  | "plugin_lifecycle"
  | "plugin_mcp_tools"
  | "plugin_mcp_tool_result"
  | "plugin_surface"
  | "plugin_action_result"
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

export interface DaemonPackage {
  package_name: string;
  version: string;
  classification: string;
  state: string;
  requested_capabilities: DaemonCapability[];
  runnable_entrypoints: DaemonPackageRunnableEntrypoint[];
  provider_profile_admitted: boolean;
}

export interface DaemonCapability {
  surface: string;
  scope: string | null;
}

export interface DaemonPackageRunnableEntrypoint {
  id: string;
  kind: string;
  command: string;
  args: string[];
  working_directory: DaemonPackageWorkingDirectory;
  environment: DaemonPackageEnvironmentRequirement[];
  mode: string;
  capabilities: DaemonCapability[];
  may_supervise: boolean;
  process: DaemonPackageProcess;
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
  diagnostics: DaemonDiagnostic[];
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
  diagnostics: DaemonDiagnostic[];
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
  | "daemon_startup_failure";

export type DaemonEvent =
  | { type: "session_lifecycle"; session_id: string; state: string }
  | { type: "terminal_output"; session_id: string; subscription_id: string; data: string }
  | { type: "snapshot"; session_id: string; subscription_id: string; data: string; bytes: number }
  | { type: "scrollback"; session_id: string; subscription_id: string; data: string; bytes: number }
  | { type: "process_exit"; session_id: string; subscription_id: string; code: number | null }
  | { type: "attach_state"; session_id: string; subscription_id: string; state: string }
  | { type: "runtime_observation"; kind: string };
