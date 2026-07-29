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
    line(
        &mut output,
        "import type { UiActionRequest, UiActionResult, UiNode } from \"@trybotster/ui-contract\";",
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
        "AesGcmEnvelope",
        &[
            ("nonce", "string"),
            ("ciphertext", "string"),
            ("version", "number"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonLocalWebrtcDeliveryChunk",
        &[
            ("version", "number"),
            ("delivery_kind", "DaemonLocalWebrtcDeliveryKind"),
            ("message_id", "string"),
            ("chunk_index", "number"),
            ("chunk_count", "number"),
            ("total_bytes", "number"),
            ("payload", "string"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonLocalWebrtcDeliveryKind",
        &["daemon_response", "daemon_entity_frame"],
    );

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
            (
                "subscribe_entities",
                &[("entity_type", "string"), ("subscription_id", "string")],
            ),
            ("unsubscribe_entities", &[("subscription_id", "string")]),
            ("remove_session", &[("session_id", "string")]),
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
            ("read_screen", &[("session_id", "string")]),
            ("read_mode_flags", &[("session_id", "string")]),
            ("capture_snapshot", &[("session_id", "string")]),
            ("list_session_templates", &[]),
            ("show_session_template", &[("template_id", "string")]),
            (
                "resolve_session_template",
                &[
                    ("template_id", "string"),
                    ("request", "DaemonSessionTemplateRequest"),
                ],
            ),
            (
                "spawn_session_template",
                &[
                    ("template_id", "string"),
                    ("session_id", "string"),
                    ("request", "DaemonSessionTemplateRequest"),
                ],
            ),
            (
                "read_session_context",
                &[
                    ("session_id", "string"),
                    ("context_id?", "string | null"),
                    ("key?", "string | null"),
                ],
            ),
            ("list_spawn_targets", &[]),
            ("show_spawn_target", &[("target_id", "string")]),
            (
                "create_spawn_target",
                &[
                    ("target_id?", "string | null"),
                    ("label?", "string | null"),
                    ("root", "string"),
                    ("enabled?", "boolean"),
                    ("kind?", "string | null"),
                    ("base_ref?", "string | null"),
                    ("metadata?", "Record<string, string>"),
                ],
            ),
            (
                "update_spawn_target",
                &[
                    ("target_id", "string"),
                    ("label?", "string | null"),
                    ("root?", "string | null"),
                    ("enabled?", "boolean | null"),
                    ("kind?", "string | null"),
                    ("base_ref?", "string | null"),
                    ("metadata?", "Record<string, string> | null"),
                ],
            ),
            ("delete_spawn_target", &[("target_id", "string")]),
            ("validate_spawn_target", &[("target_id", "string")]),
            ("list_worktrees", &[]),
            ("show_worktree", &[("worktree_id", "string")]),
            (
                "create_worktree",
                &[
                    ("worktree_id?", "string | null"),
                    ("target_id", "string"),
                    ("label?", "string | null"),
                    ("path", "string"),
                    ("metadata?", "Record<string, string>"),
                ],
            ),
            ("delete_worktree", &[("worktree_id", "string")]),
            ("list_apps", &[]),
            (
                "resolve_app_launch",
                &[("package_name", "string"), ("entrypoint_id", "string")],
            ),
            (
                "resolve_package_route",
                &[("package_name", "string"), ("route_id", "string")],
            ),
            ("list_package_navigation", &[]),
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
            ("check_package_update", &[("package_name", "string")]),
            (
                "preview_package_update",
                &[("package_name", "string"), ("pin", "DaemonPackagePin")],
            ),
            (
                "apply_package_update",
                &[("package_name", "string"), ("pin", "DaemonPackagePin")],
            ),
            ("show_package", &[("package_name", "string")]),
            (
                "set_package_configuration",
                &[
                    ("package_name", "string"),
                    ("values", "Record<string, JsonValue>"),
                ],
            ),
            ("reload_package", &[("package_name", "string")]),
            ("refresh_local_packages", &[]),
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
                "issue_local_webrtc_bootstrap",
                &[
                    ("package_name", "string"),
                    ("entrypoint_id", "string"),
                    ("origin", "string"),
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
                &[("package_name", "string"), ("request", "UiActionRequest")],
            ),
            (
                "local_webrtc_signal",
                &[
                    ("grant_id", "string"),
                    ("grant_secret", "string"),
                    ("origin", "string"),
                    ("offer", "JsonValue"),
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
            ("session_templates?", "DaemonSessionTemplate[]"),
            (
                "resolved_session_template?",
                "DaemonResolvedSessionTemplate | null",
            ),
            ("session_context?", "DaemonSessionContext | null"),
            ("read_screen?", "DaemonReadScreen | null"),
            ("mode_flags?", "DaemonModeFlags | null"),
            ("capture_snapshot?", "DaemonCaptureSnapshot | null"),
            ("spawn_targets?", "DaemonSpawnTarget[]"),
            (
                "spawn_target_validation?",
                "DaemonSpawnTargetValidation | null",
            ),
            ("worktrees?", "DaemonWorktree[]"),
            ("apps?", "DaemonApp[]"),
            ("resolved_app_launch?", "DaemonResolvedAppLaunch | null"),
            (
                "resolved_package_route?",
                "DaemonPackageRouteDescriptor | null",
            ),
            ("package_navigation?", "DaemonPackageNavigationEntry[]"),
            ("packages", "DaemonPackage[]"),
            ("available_packages?", "DaemonAvailablePackage[]"),
            ("install_plan?", "DaemonPackageInstallPlan | null"),
            ("update_status?", "DaemonPackageUpdateStatus | null"),
            ("package_decision", "DaemonPackageDecision | null"),
            ("lifecycle", "DaemonPluginLifecycle[]"),
            (
                "plugin_worker_counters?",
                "DaemonPluginWorkerCounters | null",
            ),
            ("plugin_tools", "JsonValue[]"),
            ("plugin_tool_result", "JsonValue"),
            ("plugin_surface?", "DaemonPluginSurface | null"),
            ("plugin_action_result?", "UiActionResult"),
            (
                "local_webrtc_bootstrap?",
                "DaemonLocalWebrtcBootstrap | null",
            ),
            ("local_webrtc_answer?", "DaemonLocalWebrtcAnswer | null"),
            ("events", "DaemonEvent[]"),
            ("cleanup", "DaemonSessionCleanup | null"),
            ("coordination", "DaemonCoordination | null"),
            ("error", "DaemonOperatorError | null"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonReadScreen",
        &[("session_id", "string"), ("text", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonModeFlags",
        &[("session_id", "string"), ("mouse_mode", "number")],
    );
    emit_interface(
        &mut output,
        "DaemonCaptureSnapshot",
        &[
            ("session_id", "string"),
            ("rows", "number"),
            ("cols", "number"),
            ("payload_format?", "string | null"),
            ("payload_bytes", "number"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPluginSurface",
        &[
            ("package_name", "string"),
            ("surface_id", "string"),
            ("body", "UiNode"),
            ("ui_tree_snapshot?", "DaemonUiTreeSnapshot | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonUiTreeSnapshot",
        &[
            ("package_name", "string"),
            ("surface_id", "string"),
            ("body", "UiNode"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonWorktreeLifecycleEvent",
        &[
            ("event", "string"),
            ("worktree_id?", "string | null"),
            ("target_id?", "string | null"),
            ("status?", "string | null"),
            ("label?", "string | null"),
            ("display_path?", "string | null"),
            ("failure_kind?", "string | null"),
            ("message?", "string | null"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonResponseKind",
        &[
            "status",
            "sessions",
            "entity_subscribed",
            "entity_unsubscribed",
            "session_removed",
            "spawned",
            "events",
            "session_templates",
            "resolved_session_template",
            "session_context",
            "read_screen",
            "read_mode_flags",
            "capture_snapshot",
            "spawn_targets",
            "spawn_target_validation",
            "worktrees",
            "apps",
            "resolved_app_launch",
            "resolved_package_route",
            "package_navigation",
            "packages",
            "available_packages",
            "package_install_plan",
            "package_update_status",
            "package_decision",
            "plugin_lifecycle",
            "plugin_mcp_tools",
            "plugin_mcp_tool_result",
            "plugin_surface",
            "plugin_action_result",
            "local_webrtc_bootstrap",
            "local_webrtc_answer",
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
        "DaemonSpawnTarget",
        &[
            ("target_id", "string"),
            ("label", "string"),
            ("root", "string"),
            ("enabled", "boolean"),
            ("kind", "string"),
            ("base_ref?", "string | null"),
            ("metadata?", "Record<string, string>"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSpawnTargetValidation",
        &[
            ("target_id", "string"),
            ("ok", "boolean"),
            ("status", "string"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonWorktree",
        &[
            ("worktree_id", "string"),
            ("target_id", "string"),
            ("label", "string"),
            ("path", "string"),
            ("status", "string"),
            ("management", "string"),
            ("git?", "DaemonWorktreeGitMetadata | null"),
            ("metadata?", "Record<string, string>"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonWorktreeGitMetadata",
        &[
            ("repository_root", "string"),
            ("branch?", "string | null"),
            ("head?", "string | null"),
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
        "DaemonSessionTemplateRequest",
        &[
            ("target_id?", "string | null"),
            ("cwd?", "string | null"),
            ("environment?", "Record<string, string>"),
            ("context", "DaemonSessionTemplateContextInput"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSessionTemplateContextInput",
        &[
            ("worktree_path?", "string | null"),
            ("repo_path?", "string | null"),
            ("branch_name?", "string | null"),
            ("prompt?", "string | null"),
            ("ticket_id?", "string | null"),
            ("workspace_id?", "string | null"),
            ("metadata?", "Record<string, string>"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSessionTemplate",
        &[
            ("template_id", "string"),
            ("package_name", "string"),
            ("id", "string"),
            ("source", "string"),
            ("command", "string"),
            ("args?", "string[]"),
            ("working_directory_policy", "string"),
            ("allowed_environment_overrides?", "string[]"),
            ("context_keys?", "string[]"),
            ("target_id", "string"),
            ("available", "boolean"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonResolvedSessionTemplate",
        &[
            ("template", "DaemonSessionTemplate"),
            ("session_id", "string"),
            ("executable", "string"),
            ("arguments?", "string[]"),
            ("working_directory", "string"),
            ("environment?", "Record<string, string>"),
            ("context_id", "string"),
            ("context_keys?", "string[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSessionContext",
        &[
            ("context_id", "string"),
            ("session_id", "string"),
            ("values", "Record<string, string>"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonApp",
        &[
            ("package_name", "string"),
            ("app_id", "string"),
            ("entrypoint_id", "string"),
            ("kind", "string"),
            ("launch_mode", "string"),
            ("lifecycle_state", "string"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
            ("actions?", "DaemonPackageActionState[]"),
            ("blocked_reasons?", "string[]"),
            ("launch_target", "DaemonAppLaunchTarget"),
            ("route?", "DaemonPackageRouteDescriptor | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonAppLaunchTarget",
        &[("kind", "string"), ("local_url?", "string | null")],
    );
    emit_interface(
        &mut output,
        "DaemonResolvedAppLaunch",
        &[
            ("package_name", "string"),
            ("app_id", "string"),
            ("entrypoint_id", "string"),
            ("kind", "string"),
            ("launch_mode", "string"),
            ("command", "string"),
            ("args?", "string[]"),
            ("working_directory", "string"),
            ("environment?", "Record<string, string>"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageRouteDescriptor",
        &[
            ("package_name", "string"),
            ("route_id", "string"),
            ("route_path", "string"),
            ("target", "DaemonPackageRouteTarget"),
            ("title", "string"),
            ("label", "string"),
            ("app_id?", "string | null"),
            ("surface_id?", "string | null"),
            ("icon?", "string | null"),
            ("category?", "string | null"),
            ("layout_mode", "string"),
            ("required_capabilities?", "DaemonCapability[]"),
            ("enabled", "boolean"),
            ("blocked", "boolean"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
            ("supports_settings", "boolean"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageRouteTarget",
        &[
            ("kind", "string"),
            ("entrypoint_id?", "string | null"),
            ("surface_id?", "string | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageNavigationEntry",
        &[
            ("package_name", "string"),
            ("item_id", "string"),
            ("label", "string"),
            ("icon?", "string | null"),
            ("description?", "string | null"),
            ("route_id", "string"),
            ("route_path", "string"),
            ("target", "DaemonPackageRouteTarget"),
            ("source", "DaemonPackageNavigationSource"),
            ("enabled", "boolean"),
            ("blocked", "boolean"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageNavigationSource",
        &[
            ("kind", "string"),
            ("surface_id?", "string | null"),
            ("entrypoint_id?", "string | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonLocalWebrtcBootstrap",
        &[
            ("grant_id", "string"),
            ("grant_secret", "string"),
            ("package_name", "string"),
            ("entrypoint_id", "string"),
            ("expected_origin", "string"),
            ("expires_at", "number"),
            ("signaling_transport", "string"),
            ("data_plane", "string"),
            ("ordered", "boolean"),
            ("max_retransmits?", "number | null"),
            ("max_packet_lifetime_ms?", "number | null"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonLocalWebrtcAnswer",
        &[
            ("grant_id", "string"),
            ("answer", "JsonValue"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackage",
        &[
            ("package_name", "string"),
            ("version", "string"),
            ("classification", "string"),
            ("source_kind", "string"),
            ("state", "string"),
            ("requested_capabilities", "DaemonCapability[]"),
            ("surfaces?", "DaemonPackageSurfaceDescriptor[]"),
            ("routes?", "DaemonPackageRouteDescriptor[]"),
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
            ("actions?", "DaemonPackageActionState[]"),
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
            ("actions?", "DaemonPackageActionState[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonPackageActionState",
        &[
            ("action_id", "string"),
            ("status", "DaemonPackageActionStatus"),
            ("reason?", "string | null"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
            (
                "required_references?",
                "DaemonPackageActionRequiredReference[]",
            ),
            ("request?", "DaemonPackageActionRequest | null"),
        ],
    );
    emit_string_union(
        &mut output,
        "DaemonPackageActionStatus",
        &["available", "blocked", "unavailable"],
    );
    emit_interface(
        &mut output,
        "DaemonPackageActionRequiredReference",
        &[("kind", "string"), ("key", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonPackageActionRequest",
        &[
            ("request_type", "string"),
            ("pin?", "DaemonPackagePin | null"),
            ("package_name?", "string | null"),
            ("entry_id?", "string | null"),
            ("entrypoint_id?", "string | null"),
            ("registry_path?", "string | null"),
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
        "DaemonPackageUpdateStatus",
        &[
            ("package_name", "string"),
            ("update_available", "boolean"),
            ("reload_required", "boolean"),
            ("restart_required", "boolean"),
            ("pin?", "DaemonPackagePin | null"),
            ("diagnostics?", "DaemonPackageDiagnostic[]"),
            ("actions?", "DaemonPackageActionState[]"),
        ],
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
            ("launch_mode", "string"),
            ("command", "string"),
            ("args", "string[]"),
            ("working_directory", "DaemonPackageWorkingDirectory"),
            ("environment", "DaemonPackageEnvironmentRequirement[]"),
            ("capabilities", "DaemonCapability[]"),
            ("may_supervise", "boolean"),
            ("process", "DaemonPackageProcess"),
            ("actions?", "DaemonPackageActionState[]"),
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
        "DaemonPluginWorkerCounters",
        &[
            ("configured_queue_capacity", "number"),
            ("configured_executor_concurrency", "number"),
            ("live_plugin_executors", "number"),
            ("live_executor_workers", "number"),
            ("queued_jobs", "number"),
            ("in_flight_jobs", "number"),
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
            ("lifecycle_counters?", "DaemonLifecycleCounters"),
            ("diagnostics?", "DaemonDiagnostic[]"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonLifecycleCounters",
        &[
            ("accepted_connections", "number"),
            ("rejected_connections", "number"),
            ("live_connections", "number"),
            ("high_water_live_connections", "number"),
            ("live_entity_subscriptions", "number"),
            ("high_water_entity_subscriptions", "number"),
            ("live_attach_subscriptions", "number"),
            ("high_water_attach_subscriptions", "number"),
            ("reconnect_registrations", "number"),
            ("cleanup_completed", "number"),
            ("cleanup_failed", "number"),
            ("cleanup_by_reason?", "Record<string, number>"),
            ("reconciliation_wakes", "number"),
            ("lifecycle_change_reads", "number"),
            ("lifecycle_baseline_reads", "number"),
            ("lifecycle_resync_reads", "number"),
            ("lifecycle_session_drains", "number"),
            ("entity_delivery_attempts", "number"),
            ("entity_delivery_successes", "number"),
            ("entity_delivery_overflows", "number"),
            ("entity_delivery_failures", "number"),
            ("stalled_writes", "number"),
        ],
    );
    emit_interface(
        &mut output,
        "DaemonSession",
        &[("session_id", "string"), ("lifecycle", "string")],
    );
    emit_interface(
        &mut output,
        "DaemonSessionEntity",
        &[
            ("session_uuid", "string"),
            ("registry_state", "string"),
            ("lifecycle?", "string | null"),
            ("rows", "number"),
            ("cols", "number"),
            ("updated_at", "number"),
            ("exit_code?", "number | null"),
            ("failure_reason?", "string | null"),
        ],
    );
    emit_union(
        &mut output,
        "DaemonEntityFrame",
        &[
            (
                "entity_snapshot",
                &[
                    ("subscription_id", "string"),
                    ("entity_type", "string"),
                    ("snapshot_seq", "number"),
                    ("items", "DaemonSessionEntity[]"),
                    ("resync_reason?", "string | null"),
                ],
            ),
            (
                "entity_upsert",
                &[
                    ("subscription_id", "string"),
                    ("entity_type", "string"),
                    ("snapshot_seq", "number"),
                    ("id", "string"),
                    ("entity", "DaemonSessionEntity"),
                ],
            ),
            (
                "entity_patch",
                &[
                    ("subscription_id", "string"),
                    ("entity_type", "string"),
                    ("snapshot_seq", "number"),
                    ("id", "string"),
                    ("patch", "JsonValue"),
                ],
            ),
            (
                "entity_remove",
                &[
                    ("subscription_id", "string"),
                    ("entity_type", "string"),
                    ("snapshot_seq", "number"),
                    ("id", "string"),
                ],
            ),
        ],
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
            "backpressure",
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
                    ("payload_base64", "string"),
                    ("payload_encoding", "\"base64\""),
                    ("bytes", "number"),
                ],
            ),
            (
                "scrollback",
                &[
                    ("session_id", "string"),
                    ("subscription_id", "string"),
                    ("payload_base64", "string"),
                    ("payload_encoding", "\"base64\""),
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
            (
                "worktree_lifecycle",
                &[("event", "DaemonWorktreeLifecycleEvent")],
            ),
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
