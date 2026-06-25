pub(crate) fn daemon_protocol_typescript() -> String {
    let mut output = String::new();
    line(
        &mut output,
        "// Generated from crates/botster-hub-client Rust serde DTOs.",
    );
    line(
        &mut output,
        "// Regenerate/check with: ./test.sh -p botster-hub-client",
    );
    line(&mut output, "");
    line(
        &mut output,
        "export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };",
    );
    line(
        &mut output,
        "export type JsonObject = { [key: string]: JsonValue };",
    );
    line(&mut output, "");

    emit_interface(
        &mut output,
        "DaemonHello",
        &[
            ("protocol", "string"),
            ("compatibility", "DaemonCompatibilityRequirement"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonHelloAck",
        &[
            ("protocol", "string"),
            ("compatibility", "DaemonCompatibility"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonCompatibility",
        &[
            ("protocol", "string"),
            ("protocol_version", "number"),
            ("features", "string[]"),
            ("conformance_fixture_revision", "number"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonCompatibilityRequirement",
        &[
            ("protocol", "string"),
            ("minimum_protocol_version", "number"),
            ("required_features", "string[]"),
            ("minimum_conformance_fixture_revision", "number"),
            ("client_name", "string"),
        ],
    );

    emit_union(
        &mut output,
        "DaemonRequest",
        &[
            ("status", &[]),
            ("list_sessions", &[]),
            ("whoami", &[("caller_session_id", "string | null")]),
            (
                "post_message",
                &[
                    ("caller_session_id", "string | null"),
                    ("target_session_id", "string"),
                    ("envelope_id", "string | null"),
                    ("body", "string"),
                ],
            ),
            (
                "receive_messages",
                &[
                    ("caller_session_id", "string"),
                    ("after", "number | null"),
                    ("limit", "number"),
                ],
            ),
            (
                "ack_message",
                &[("caller_session_id", "string"), ("envelope_id", "string")],
            ),
            (
                "notify_session",
                &[("session_id", "string"), ("data", "string")],
            ),
            ("spawn", &[("session_id", "string"), ("command", "string")]),
            (
                "attach",
                &[("session_id", "string"), ("subscription_id", "string")],
            ),
            (
                "detach",
                &[("session_id", "string"), ("subscription_id", "string")],
            ),
            (
                "send_input",
                &[("session_id", "string"), ("data", "string")],
            ),
            (
                "resize",
                &[
                    ("session_id", "string"),
                    ("rows", "number"),
                    ("cols", "number"),
                ],
            ),
            ("shutdown_session", &[("session_id", "string")]),
            ("drain", &[("session_id", "string")]),
            ("list_packages", &[]),
            ("list_available_packages", &[("registry_path", "string")]),
            (
                "inspect_available_package",
                &[("registry_path", "string"), ("entry_id", "string")],
            ),
            (
                "preview_package_install",
                &[("registry_path", "string"), ("entry_id", "string")],
            ),
            (
                "install_package_registry_entry",
                &[("registry_path", "string"), ("entry_id", "string")],
            ),
            ("install_package_local_path", &[("path", "string")]),
            ("show_package", &[("package_name", "string")]),
            (
                "set_package_configuration",
                &[
                    ("package_name", "string"),
                    ("values", "Record<string, JsonValue>"),
                ],
            ),
            ("enable_package_local_path", &[("path", "string")]),
            ("enable_package", &[("package_name", "string")]),
            ("disable_package", &[("package_name", "string")]),
            ("remove_package", &[("package_name", "string")]),
            (
                "start_package_entrypoint",
                &[
                    ("package_name", "string"),
                    ("entrypoint_id", "string"),
                    ("environment_overrides?", "Record<string, string>"),
                ],
            ),
            (
                "stop_package_entrypoint",
                &[("package_name", "string"), ("entrypoint_id", "string")],
            ),
            (
                "restart_package_entrypoint",
                &[("package_name", "string"), ("entrypoint_id", "string")],
            ),
            (
                "package_entrypoint_status",
                &[("package_name", "string"), ("entrypoint_id", "string")],
            ),
            ("plugin_lifecycle_status", &[]),
            ("plugin_mcp_list_tools", &[]),
            (
                "plugin_mcp_call_tool",
                &[("name", "string"), ("arguments", "JsonValue")],
            ),
            (
                "plugin_surface_render",
                &[
                    ("package_name", "string"),
                    ("surface_id", "string"),
                    ("payload", "JsonValue"),
                ],
            ),
            (
                "plugin_surface_action",
                &[
                    ("package_name", "string"),
                    ("surface_id", "string"),
                    ("action_id", "string"),
                    ("payload", "JsonValue"),
                ],
            ),
            ("daemon_shutdown", &[]),
        ],
    );

    emit_interface(
        &mut output,
        "DaemonResponse",
        &[
            ("kind", "DaemonResponseKind"),
            ("status", "DaemonStatus | null"),
            ("sessions", "DaemonSession[]"),
            ("packages", "DaemonPackage[]"),
            ("available_packages?", "DaemonAvailablePackage[]"),
            ("install_plan?", "DaemonPackageInstallPlan | null"),
            ("package_decision", "DaemonPackageDecision | null"),
            ("lifecycle", "DaemonPluginLifecycle[]"),
            ("plugin_tools", "JsonValue[]"),
            ("plugin_tool_result", "JsonValue"),
            ("plugin_surface?", "JsonValue"),
            ("plugin_action_result?", "JsonValue"),
            ("events", "DaemonEvent[]"),
            ("cleanup", "DaemonSessionCleanup | null"),
            ("coordination", "DaemonCoordination | null"),
            ("error", "DaemonOperatorError | null"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonResponseKind",
        &[
            "status",
            "sessions",
            "spawned",
            "events",
            "packages",
            "available_packages",
            "package_install_plan",
            "package_decision",
            "plugin_lifecycle",
            "plugin_mcp_tools",
            "plugin_mcp_tool_result",
            "plugin_surface",
            "plugin_action_result",
            "session_cleanup",
            "identity",
            "message_posted",
            "messages",
            "message_acked",
            "session_notified",
            "operator_error",
            "shutdown",
        ],
    );

    emit_interface(
        &mut output,
        "DaemonCoordination",
        &[
            ("identity", "DaemonIdentity | null"),
            ("publish", "DaemonEnvelopePublish | null"),
            ("messages", "DaemonEnvelope[]"),
            ("next_cursor", "number | null"),
            ("ack", "DaemonEnvelopeAck | null"),
            ("notify", "DaemonNotify | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonIdentity",
        &[
            ("client_id", "string"),
            ("role", "string"),
            ("identity_source", "string"),
            ("caller_session_id", "string | null"),
            ("host_id", "string"),
            ("host_display_name", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonEnvelopePublish",
        &[("deliveries", "DaemonEnvelopeDelivery[]")],
    );
    emit_interface(
        &mut output,
        "DaemonEnvelopeDelivery",
        &[
            ("envelope_id", "string"),
            ("target", "string"),
            ("cursor", "number"),
            ("status", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonEnvelope",
        &[
            ("envelope_id", "string"),
            ("source", "string"),
            ("content_type", "string"),
            ("body", "string"),
            ("created_at", "number"),
            ("cursor", "number | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonEnvelopeAck",
        &[
            ("envelope_id", "string | null"),
            ("target", "string | null"),
            ("cursor", "number | null"),
            ("status", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonNotify",
        &[
            ("decision", "string"),
            ("state_count", "number"),
            ("states", "string[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackage",
        &[
            ("package_name", "string"),
            ("version", "string"),
            ("classification", "string"),
            ("state", "string"),
            ("requested_capabilities", "DaemonCapability[]"),
            ("surfaces?", "DaemonPackageSurfaceDescriptor[]"),
            ("runnable_entrypoints", "DaemonPackageRunnableEntrypoint[]"),
            ("configuration", "DaemonPackageConfiguration"),
            ("availability", "DaemonPackageAvailability"),
            (
                "dependency_availability?",
                "DaemonPackageDependencyAvailability[]",
            ),
            (
                "feature_availability?",
                "DaemonPackageFeatureAvailability[]",
            ),
            ("provider_profile_admitted", "boolean"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageAvailability",
        &[
            ("state", "DaemonPackageAvailabilityState"),
            ("reasons?", "DaemonPackageAvailabilityReason[]"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonPackageAvailabilityState",
        &["available", "blocked"],
    );
    emit_interface(
        &mut output,
        "DaemonPackageAvailabilityReason",
        &[
            ("reason", "string"),
            ("action", "string"),
            ("package_name?", "string | null"),
            ("capability?", "DaemonCapability | null"),
            ("requirement?", "string | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageDependencyAvailability",
        &[
            ("id", "string"),
            ("package_name", "string"),
            ("state", "DaemonPackageAvailabilityState"),
            ("reasons?", "DaemonPackageAvailabilityReason[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageFeatureAvailability",
        &[
            ("id", "string"),
            ("state", "DaemonPackageAvailabilityState"),
            ("reasons?", "DaemonPackageAvailabilityReason[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonCapability",
        &[("surface", "string"), ("scope", "string | null")],
    );
    emit_interface(
        &mut output,
        "DaemonAvailablePackage",
        &[
            ("entry_id", "string"),
            ("package_name", "string"),
            ("version", "string"),
            ("classification", "string"),
            ("source_kind", "string"),
            ("source_label", "string"),
            ("first_party", "boolean"),
            ("state", "string"),
            ("requested_capabilities", "DaemonCapability[]"),
            ("compatibility", "DaemonPackageCompatibility"),
            ("pin?", "DaemonPackagePin | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageInstallPlan",
        &[
            ("entry", "DaemonAvailablePackage"),
            ("effects", "DaemonPackageInstallEffect[]"),
            ("diagnostics", "DaemonPackageDiagnostic[]"),
            ("mutates_registry", "boolean"),
            ("starts_entrypoints", "boolean"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageInstallEffect",
        &[("kind", "string"), ("message", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonPackageCompatibility",
        &[
            ("botster_requirement", "string"),
            ("hub_version", "string"),
            ("result", "string"),
            ("diagnostics", "string[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackagePin",
        &[
            ("revision", "string"),
            ("branch?", "string | null"),
            ("tag?", "string | null"),
            ("rev?", "string | null"),
            ("checksum?", "string | null"),
            ("update_policy", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageSurfaceDescriptor",
        &[
            ("id", "string"),
            ("kind", "string"),
            ("title", "string"),
            ("description?", "string | null"),
            ("icon?", "string | null"),
            ("order?", "number | null"),
            ("category?", "string | null"),
            ("supports?", "string[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageConfiguration",
        &[
            ("schema?", "JsonValue | null"),
            ("effective_values?", "Record<string, JsonValue>"),
            ("missing_required?", "string[]"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageRunnableEntrypoint",
        &[
            ("id", "string"),
            ("kind", "string"),
            ("command", "string"),
            ("args", "string[]"),
            ("working_directory", "DaemonPackageWorkingDirectory"),
            ("environment", "DaemonPackageEnvironmentRequirement[]"),
            ("mode", "string"),
            ("capabilities", "DaemonCapability[]"),
            ("may_supervise", "boolean"),
            ("process", "DaemonPackageProcess"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageWorkingDirectory",
        &[("policy", "string"), ("path", "string | null")],
    );
    emit_interface(
        &mut output,
        "DaemonPackageEnvironmentRequirement",
        &[
            ("name", "string"),
            ("required", "boolean"),
            ("default", "string | null"),
            ("description", "string | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageProcess",
        &[
            ("state", "string"),
            ("pid?", "number"),
            ("started_at?", "number"),
            ("exited_at?", "number"),
            ("exit_status?", "string"),
            ("diagnostics", "DaemonPackageDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageDiagnostic",
        &[("kind", "string"), ("message", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonPackageDecision",
        &[
            ("package_name", "string"),
            ("action", "string"),
            ("state", "string"),
            ("classification", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPluginLifecycle",
        &[
            ("package_name", "string"),
            ("state", "string"),
            ("loaded", "boolean"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonStatus",
        &[
            ("lifecycle_state", "string"),
            ("compatibility", "DaemonCompatibility"),
            ("host_id", "string"),
            ("host_display_name", "string"),
            ("schema_version", "number"),
            ("data_dir_configured", "boolean"),
            ("core_initialized", "boolean"),
            ("state_source", "string"),
            ("package_count", "number"),
            ("enabled_package_count", "number"),
            ("provider_count", "number"),
            ("enabled_provider_count", "number"),
            ("session_count", "number"),
            ("recovered_sessions", "string[]"),
            ("stale_sessions", "string[]"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSession",
        &[("session_id", "string"), ("lifecycle", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonSessionCleanup",
        &[("session_id", "string"), ("outcome", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonOperatorError",
        &[
            ("code", "string"),
            ("request_id", "string"),
            ("operation", "string"),
            ("message", "string"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonDiagnostic",
        &[
            ("kind", "DaemonDiagnosticKind"),
            ("operation", "string | null"),
            ("feature", "string | null"),
            ("message", "string | null"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonDiagnosticKind",
        &[
            "connected",
            "disconnected",
            "compatibility_mismatch",
            "unsupported_feature",
            "terminal_stream_unavailable",
            "action_failure",
            "daemon_startup_failure",
        ],
    );

    emit_union(
        &mut output,
        "DaemonEvent",
        &[
            (
                "session_lifecycle",
                &[("session_id", "string"), ("state", "string")],
            ),
            (
                "terminal_output",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("data", "string"),
                ],
            ),
            (
                "snapshot",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("data", "string"),
                    ("bytes", "number"),
                ],
            ),
            (
                "scrollback",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("data", "string"),
                    ("bytes", "number"),
                ],
            ),
            (
                "process_exit",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("code", "number | null"),
                ],
            ),
            (
                "attach_state",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("state", "string"),
                ],
            ),
            ("runtime_observation", &[("kind", "string")]),
        ],
    );

    if output.ends_with("\n\n") {
        output.pop();
    }
    output
}

fn line(output: &mut String, text: &str) {
    output.push_str(text);
    output.push('\n');
}

fn emit_interface(output: &mut String, name: &str, fields: &[(&str, &str)]) {
    line(output, &format!("export interface {name} {{"));
    for (field, ty) in fields {
        line(output, &format!("  {field}: {ty};"));
    }
    line(output, "}");
    line(output, "");
}

fn emit_string_union(output: &mut String, name: &str, values: &[&str]) {
    line(output, &format!("export type {name} ="));
    for (index, value) in values.iter().enumerate() {
        let suffix = if index + 1 == values.len() { ";" } else { "" };
        line(output, &format!("  | \"{value}\"{suffix}"));
    }
    line(output, "");
}

fn emit_union(output: &mut String, name: &str, variants: &[(&str, &[(&str, &str)])]) {
    line(output, &format!("export type {name} ="));
    for (index, (tag, fields)) in variants.iter().enumerate() {
        let suffix = if index + 1 == variants.len() { ";" } else { "" };
        let mut body = format!("{{ type: \"{tag}\"");
        for (field, ty) in *fields {
            body.push_str(&format!("; {field}: {ty}"));
        }
        body.push_str(" }");
        line(output, &format!("  | {body}{suffix}"));
    }
    line(output, "");
}
