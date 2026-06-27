#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};
use std::{fs, thread};

use botster_core::{
    Capability, CapabilitySurface, ExtensionEntrypoint, ExtensionKind, ExtensionRuntime, ModeFlags,
    PackageBlockedReason, PackageConfigurationField, PackageConfigurationFieldType,
    PackageConfigurationSchema, PackageConfigurationSecretValue, PackageConfigurationValue,
    PackageDependency, PackageDependencyKind, PackageFeatureGate, PackageRequirement, RequestId,
    SessionId, SessionLifecycleState, SubscriptionId,
};
use botster_core_daemon::{GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence};
use botster_hub::{
    DataDirectoryOption, HostIdentityOptions, HubClientAdmission, HubClientApi, HubClientError,
    HubClientEvent, HubClientIdentity, HubClientOperation, HubClientPackageClassification,
    HubClientPackageState, HubClientRequest, HubClientResponseBody, HubClientRole, HubRuntime,
    HubStartupOptions, PackageProvenance, PackageRegistry, RuntimeEnvironment, SessionDefaults,
    TransportBindings,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    ensure_session_worker_binary();
    let data_directory = format!("target/botster-hub-test-data/client-api-{name}");
    let _ = fs::remove_dir_all(&data_directory);
    let config = HubStartupOptions {
        host: HostIdentityOptions {
            id: "hub-client-api-test".to_string(),
            display_name: "Hub Client API Test".to_string(),
            fingerprint: None,
        },
        data_directory: DataDirectoryOption::Explicit(data_directory.into()),
        session_defaults: SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn session_id() -> SessionId {
    SessionId("hub-client-api-session".to_string())
}

fn subscription_id() -> SubscriptionId {
    SubscriptionId("hub-client-api-subscription".to_string())
}

fn empty_registry() -> PackageRegistry {
    PackageRegistry::new(Vec::<Capability>::new().into_iter().collect())
}

fn write_session_template_package(root: &std::path::Path) {
    fs::create_dir_all(root.join("bin")).expect("create session template package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'template:%s:%s\\n' \"$BOTSTER_SESSION_ID\" \"$BOTSTER_MODE\"\n",
    )
    .expect("write session template script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session template script");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "session-template.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "session_templates": [
    {
      "id": "init",
      "command": "bin/init.sh",
      "environment": { "BOTSTER_MODE": "default" },
      "allowed_environment_overrides": ["BOTSTER_MODE"],
      "context": ["prompt"]
    }
  ]
}
"#,
    )
    .expect("write session template package manifest");
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn plugin_manifest(name: &str, capabilities: Vec<Capability>) -> botster_core::PackageManifest {
    botster_core::PackageManifest {
        name: name.to_string(),
        version: "1.0.0".to_string(),
        kind: ExtensionKind::Plugin,
        botster: ">=0.1.0".to_string(),
        source: Some(botster_core::PackageSource::Git {
            repo: "https://example.invalid/botster/plugin.git".to_string(),
            reference: "v1.0.0".to_string(),
        }),
        capabilities,
        entrypoints: vec![ExtensionEntrypoint {
            runtime: ExtensionRuntime::Lua,
            path: "plugin.lua".to_string(),
            bootstrap: false,
        }],
        dependencies: Vec::new(),
        features: Vec::new(),
        configuration: None,
        host_profile: None,
        surfaces: Vec::new(),
        runnable_entrypoints: Vec::new(),
    }
}

fn provenance() -> PackageProvenance {
    PackageProvenance {
        source: "local-private-source".to_string(),
        checksum: Some("sha256:test".to_string()),
    }
}

fn configurable_plugin_manifest(
    name: &str,
    capabilities: Vec<Capability>,
) -> botster_core::PackageManifest {
    let mut manifest = plugin_manifest(name, capabilities);
    manifest.configuration = Some(PackageConfigurationSchema {
        groups: Vec::new(),
        fields: vec![
            PackageConfigurationField {
                key: "endpoint".to_string(),
                field_type: PackageConfigurationFieldType::Url,
                label: "Endpoint".to_string(),
                description: None,
                required: true,
                default: None,
                validation: None,
                group: None,
                order: None,
                options: Vec::new(),
            },
            PackageConfigurationField {
                key: "api_token".to_string(),
                field_type: PackageConfigurationFieldType::Secret,
                label: "API token".to_string(),
                description: None,
                required: true,
                default: Some(PackageConfigurationValue::Secret {
                    state: PackageConfigurationSecretValue::Unset,
                }),
                validation: None,
                group: None,
                order: None,
                options: Vec::new(),
            },
        ],
    });
    manifest
}

fn project_pipelines_manifest_with_github_feature() -> botster_core::PackageManifest {
    let mut manifest = plugin_manifest(
        "project-pipelines",
        vec![capability(CapabilitySurface::Surfaces, None)],
    );
    manifest.dependencies = vec![PackageDependency {
        id: "github-provider".to_string(),
        package: "github-provider".to_string(),
        kind: PackageDependencyKind::Optional,
        feature: Some("github_pr_lifecycle".to_string()),
        requirements: vec![PackageRequirement::Provider {
            provider: "github-provider".to_string(),
        }],
    }];
    manifest.features = vec![
        PackageFeatureGate {
            id: "local_pipelines".to_string(),
            label: "Local pipelines".to_string(),
            description: None,
            dependencies: Vec::new(),
            requirements: Vec::new(),
        },
        PackageFeatureGate {
            id: "github_pr_lifecycle".to_string(),
            label: "GitHub PR lifecycle".to_string(),
            description: None,
            dependencies: vec!["github-provider".to_string()],
            requirements: vec![
                PackageRequirement::Config {
                    key: "endpoint".to_string(),
                },
                PackageRequirement::Auth {
                    key: "api_token".to_string(),
                },
            ],
        },
    ];
    manifest
}

fn capability_gated_plugin_manifest() -> botster_core::PackageManifest {
    let mut manifest = plugin_manifest(
        "capability-gated.plugin",
        vec![capability(CapabilitySurface::Surfaces, None)],
    );
    manifest.features = vec![PackageFeatureGate {
        id: "localhost_preview".to_string(),
        label: "Localhost preview".to_string(),
        description: None,
        dependencies: Vec::new(),
        requirements: vec![PackageRequirement::Capability {
            capability: capability(CapabilitySurface::Network, Some("localhost")),
        }],
    }];
    manifest
}

#[test]
fn session_templates_resolve_spawn_context_and_reject_unadmitted_reads() {
    let package_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-template-package",
    );
    let _ = fs::remove_dir_all(&package_root);
    write_session_template_package(&package_root);
    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&package_root, "install session template package")
        .expect("install session template package");
    packages
        .enable("session-template.plugin", "enable session template package")
        .expect("enable session template package");
    let mut runtime = explicit_runtime("session-template");
    let api = HubClientApi::local_operator("session-template-client");

    let list = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTemplates {
                request_id: request_id("list-session-templates"),
            },
        )
        .expect("list templates");
    let HubClientResponseBody::SessionTemplates(templates) = list.body else {
        panic!("session templates response expected");
    };
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].id, "init");

    let rejected_env = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionTemplate {
                request_id: request_id("resolve-rejected-env"),
                template_id: "init".to_string(),
                template_request: botster_hub::SessionTemplateRequest {
                    environment: BTreeMap::from([(
                        "BOTSTER_UNDECLARED".to_string(),
                        "no".to_string(),
                    )]),
                    ..botster_hub::SessionTemplateRequest::default()
                },
            },
        )
        .expect_err("undeclared env override rejected");
    assert!(matches!(
        rejected_env,
        HubClientError::SessionTemplate {
            kind: "environment_not_admitted",
            ..
        }
    ));

    let resolved = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionTemplate {
                request_id: request_id("resolve-generic-template-id"),
                template_id: "init".to_string(),
                template_request: botster_hub::SessionTemplateRequest {
                    environment: BTreeMap::from([(
                        "BOTSTER_MODE".to_string(),
                        "override".to_string(),
                    )]),
                    context: botster_hub::SessionTemplateContextInput {
                        prompt: Some("hello from api".to_string()),
                        ..botster_hub::SessionTemplateContextInput::default()
                    },
                    ..botster_hub::SessionTemplateRequest::default()
                },
            },
        )
        .expect("resolve bare generic template id");
    let HubClientResponseBody::ResolvedSessionTemplate(resolved) = resolved.body else {
        panic!("resolved template response expected");
    };
    assert_eq!(resolved.template.id, "init");
    assert_eq!(
        resolved.environment.get("BOTSTER_MODE").map(String::as_str),
        Some("override")
    );
    assert!(resolved.environment.contains_key("BOTSTER_CONTEXT_ID"));

    let spawn = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::SpawnSessionTemplate {
                request_id: request_id("spawn-session-template"),
                template_id: "init".to_string(),
                template_request: botster_hub::SessionTemplateRequest {
                    session_id: Some(SessionId("session-template-api-session".to_string())),
                    context: botster_hub::SessionTemplateContextInput {
                        prompt: Some("hello from spawn".to_string()),
                        ..botster_hub::SessionTemplateContextInput::default()
                    },
                    ..botster_hub::SessionTemplateRequest::default()
                },
                now_seconds: 1,
            },
        )
        .expect("spawn session template");
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawned response expected");
    };
    assert_eq!(spawned.session.session_id.0, "session-template-api-session");

    let context = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadSessionContext {
                request_id: request_id("read-session-context"),
                session_id: SessionId("session-template-api-session".to_string()),
                context_id: None,
                key: Some("prompt".to_string()),
            },
        )
        .expect("read session context");
    let HubClientResponseBody::SessionContext(context) = context.body else {
        panic!("context response expected");
    };
    assert_eq!(
        context.values.get("prompt").map(String::as_str),
        Some("hello from spawn")
    );

    let unadmitted = HubClientApi::new(
        HubClientIdentity {
            client_id: botster_core::ClientId("unadmitted".to_string()),
            role: HubClientRole::Unadmitted,
        },
        HubClientAdmission::deny_all(),
    );
    let denied = unadmitted
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadSessionContext {
                request_id: request_id("unadmitted-context-read"),
                session_id: SessionId("session-template-api-session".to_string()),
                context_id: None,
                key: None,
            },
        )
        .expect_err("unadmitted context reads are denied");
    assert!(matches!(denied, HubClientError::AdmissionDenied { .. }));
}

#[test]
fn package_configuration_client_package_rows_are_sanitized() {
    let api = HubClientApi::local_operator("package-configuration-client");
    let capability = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![capability.clone()].into_iter().collect());
    packages
        .install(
            configurable_plugin_manifest("configuration.plugin", vec![capability]),
            provenance(),
            "install configurable package",
        )
        .expect("install package");
    packages
        .set_configuration(
            "configuration.plugin",
            BTreeMap::from([
                (
                    "endpoint".to_string(),
                    PackageConfigurationValue::Url {
                        value: "https://example.invalid/hook".to_string(),
                    },
                ),
                (
                    "api_token".to_string(),
                    PackageConfigurationValue::Secret {
                        state: PackageConfigurationSecretValue::WriteOnly,
                    },
                ),
            ]),
            "set configuration",
        )
        .expect("set configuration");
    let mut runtime = explicit_runtime("package-configuration-client");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("list-package-configuration"),
            },
        )
        .expect("list packages");
    let HubClientResponseBody::Packages(rows) = response.body else {
        panic!("packages response expected");
    };
    let row = rows
        .into_iter()
        .find(|row| row.package_name == "configuration.plugin")
        .expect("configuration package row");

    assert!(row.configuration.schema.is_some());
    assert!(row.configuration.missing_required.is_empty());
    assert!(row.configuration.diagnostics.is_empty());
    assert_eq!(
        row.configuration.effective_values["api_token"],
        serde_json::json!({"type":"secret","state":"redacted"})
    );
    let row_json = serde_json::to_string(&row.configuration.effective_values)
        .expect("serialize effective values");
    assert!(!row_json.contains("write_only"));
    assert!(!row_json.contains("super-secret-token"));
}

#[test]
fn package_availability_projects_core_resolution_matrix_to_client_rows() {
    let api = HubClientApi::local_operator("package-availability-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    packages
        .install(
            project_pipelines_manifest_with_github_feature(),
            provenance(),
            "install project pipelines",
        )
        .expect("install project pipelines");
    packages
        .enable("project-pipelines", "enable project pipelines")
        .expect("enable project pipelines");
    let mut runtime = explicit_runtime("package-availability-client");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("list-package-availability"),
            },
        )
        .expect("list packages");
    let HubClientResponseBody::Packages(rows) = response.body else {
        panic!("packages response expected");
    };
    let row = rows
        .into_iter()
        .find(|row| row.package_name == "project-pipelines")
        .expect("project pipelines package row");

    assert_eq!(
        row.availability.state,
        botster_hub::HubClientPackageAvailabilityState::Available
    );
    assert!(row.availability.reasons.is_empty());

    let local_feature = row
        .feature_availability
        .iter()
        .find(|feature| feature.id == "local_pipelines")
        .expect("local feature row");
    assert_eq!(
        local_feature.state,
        botster_hub::HubClientPackageAvailabilityState::Available
    );

    let github_feature = row
        .feature_availability
        .iter()
        .find(|feature| feature.id == "github_pr_lifecycle")
        .expect("github feature row");
    assert_eq!(
        github_feature.state,
        botster_hub::HubClientPackageAvailabilityState::Blocked
    );
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_package"
            && reason.action == "install_package"
            && reason.package_name.as_deref() == Some("github-provider")
    }));
    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "missing_auth"
            && reason.action == "authenticate"
            && reason.requirement.as_deref() == Some("api_token")
    }));
    assert!(format!("{:?}", github_feature.reasons).contains("github-provider"));
}

#[test]
fn package_availability_reports_installed_but_disabled_dependency() {
    let api = HubClientApi::local_operator("package-disabled-dependency-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    packages
        .install(
            project_pipelines_manifest_with_github_feature(),
            provenance(),
            "install project pipelines",
        )
        .expect("install project pipelines");
    packages
        .enable("project-pipelines", "enable project pipelines")
        .expect("enable project pipelines");
    packages
        .install(
            plugin_manifest("github-provider", vec![surfaces]),
            provenance(),
            "install disabled github provider",
        )
        .expect("install github provider disabled");
    let mut runtime = explicit_runtime("package-disabled-dependency-client");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("list-disabled-dependency"),
            },
        )
        .expect("list packages");
    let HubClientResponseBody::Packages(rows) = response.body else {
        panic!("packages response expected");
    };
    let row = rows
        .into_iter()
        .find(|row| row.package_name == "project-pipelines")
        .expect("project pipelines package row");
    let github_feature = row
        .feature_availability
        .iter()
        .find(|feature| feature.id == "github_pr_lifecycle")
        .expect("github feature row");

    assert!(github_feature.reasons.iter().any(|reason| {
        reason.reason == "disabled_package"
            && reason.action == "enable_package"
            && reason.package_name.as_deref() == Some("github-provider")
    }));
}

#[test]
fn package_availability_reports_capability_denial() {
    let api = HubClientApi::local_operator("package-capability-denial-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    packages
        .install(
            capability_gated_plugin_manifest(),
            provenance(),
            "install capability gated plugin",
        )
        .expect("install capability gated plugin");
    packages
        .enable("capability-gated.plugin", "enable capability gated plugin")
        .expect("enable capability gated plugin");
    let mut runtime = explicit_runtime("package-capability-denial-client");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("list-capability-denial"),
            },
        )
        .expect("list packages");
    let HubClientResponseBody::Packages(rows) = response.body else {
        panic!("packages response expected");
    };
    let row = rows
        .into_iter()
        .find(|row| row.package_name == "capability-gated.plugin")
        .expect("capability gated package row");
    let preview_feature = row
        .feature_availability
        .iter()
        .find(|feature| feature.id == "localhost_preview")
        .expect("localhost preview feature row");

    assert_eq!(
        preview_feature.state,
        botster_hub::HubClientPackageAvailabilityState::Blocked
    );
    let reason = preview_feature
        .reasons
        .iter()
        .find(|reason| reason.reason == "missing_capability")
        .expect("missing capability reason");
    assert_eq!(reason.action, "grant_capability");
    assert_eq!(
        reason.capability,
        Some(botster_hub::HubClientCapability {
            surface: "Network".to_string(),
            scope: Some("localhost".to_string()),
        })
    );
    assert!(reason.package_name.is_none());
    assert!(reason.requirement.is_none());
}

#[test]
fn package_availability_reason_vocabulary_is_stable_and_sanitized() {
    let blocked_reasons = [
        PackageBlockedReason::MissingPackage {
            package: "github-provider".to_string(),
        },
        PackageBlockedReason::DisabledPackage {
            package: "github-provider".to_string(),
        },
        PackageBlockedReason::MissingProvider {
            provider: "github-provider".to_string(),
        },
        PackageBlockedReason::MissingCapability {
            package: Some("capability-gated.plugin".to_string()),
            capability: capability(CapabilitySurface::Network, Some("localhost")),
        },
        PackageBlockedReason::MissingAuth {
            key: "github_token".to_string(),
        },
        PackageBlockedReason::MissingConfig {
            key: "github_owner".to_string(),
        },
    ];
    let rows = blocked_reasons
        .iter()
        .map(botster_hub::HubClientPackageAvailabilityReason::from)
        .map(|reason| {
            (
                reason.reason,
                reason.action,
                reason.package_name,
                reason.capability.map(|capability| {
                    (
                        capability.surface,
                        capability.scope.unwrap_or_else(|| "<none>".to_string()),
                    )
                }),
                reason.requirement,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (
                "missing_package".to_string(),
                "install_package".to_string(),
                Some("github-provider".to_string()),
                None,
                None,
            ),
            (
                "disabled_package".to_string(),
                "enable_package".to_string(),
                Some("github-provider".to_string()),
                None,
                None,
            ),
            (
                "missing_provider".to_string(),
                "install_provider".to_string(),
                Some("github-provider".to_string()),
                None,
                None,
            ),
            (
                "missing_capability".to_string(),
                "grant_capability".to_string(),
                Some("capability-gated.plugin".to_string()),
                Some(("Network".to_string(), "localhost".to_string())),
                None,
            ),
            (
                "missing_auth".to_string(),
                "authenticate".to_string(),
                None,
                None,
                Some("github_token".to_string()),
            ),
            (
                "missing_config".to_string(),
                "configure_package".to_string(),
                None,
                None,
                Some("github_owner".to_string()),
            ),
        ]
    );
}

fn drain_until(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        for event in events {
            if let HubClientEvent::TerminalOutput { data, .. } = event {
                observed.extend(data);
            }
        }

        if observed
            .windows(needle.len())
            .any(|window| window == needle)
        {
            return observed;
        }

        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} in {:?}",
        String::from_utf8_lossy(needle),
        String::from_utf8_lossy(&observed)
    );
}

fn drain_events_until(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    subscription_id: &SubscriptionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<HubClientEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain-events"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        if observed.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id: observed_subscription_id,
                    data,
                    ..
                } if observed_subscription_id == subscription_id
                    && data.windows(needle.len()).any(|window| window == needle)
            )
        }) {
            return observed;
        }

        thread::sleep(Duration::from_millis(20));
    }

    panic!(
        "timed out waiting for {:?} on {:?}",
        String::from_utf8_lossy(needle),
        subscription_id
    );
}

fn history_data(event: &HubClientEvent) -> Option<(&SubscriptionId, &[u8])> {
    match event {
        HubClientEvent::Snapshot {
            subscription_id,
            data,
            ..
        }
        | HubClientEvent::Scrollback {
            subscription_id,
            data,
            ..
        } => Some((subscription_id, data)),
        _ => None,
    }
}

fn drain_events_for(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    logical_clock: &mut u64,
    duration: Duration,
) -> Vec<HubClientEvent> {
    let deadline = Instant::now() + duration;
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::DrainRuntime {
                    request_id: request_id("drain-extra"),
                    session_id: session_id.clone(),
                    last_output_at: *logical_clock,
                },
            )
            .expect("extra drain through client api");
        *logical_clock += 1;

        let HubClientResponseBody::Events(events) = response.body else {
            panic!("drain should return events");
        };
        observed.extend(events);

        thread::sleep(Duration::from_millis(20));
    }

    observed
}

#[test]
fn late_attach_receives_prior_terminal_history_before_later_live_output() {
    let first_api = HubClientApi::local_operator("late-history-first-client");
    let late_api = HubClientApi::local_operator("late-history-late-client");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("late-history");
    let session_id = SessionId("late-history-session".to_string());
    let first_subscription = SubscriptionId("late-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("late-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("late-history-spawn"),
                session_id: session_id.clone(),
                command: "printf 'before-late\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn late-history session");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("late-history-first-attach"),
                session_id: session_id.clone(),
                subscription_id: first_subscription,
                now_seconds: logical_clock,
            },
        )
        .expect("attach first subscription");
    logical_clock += 1;
    drain_until(
        &first_api,
        &mut runtime,
        &packages,
        &session_id,
        b"before-late",
        &mut logical_clock,
    );

    late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("late-history-late-attach"),
                session_id: session_id.clone(),
                subscription_id: late_subscription.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach late subscription");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("late-history-input"),
                session_id: session_id.clone(),
                data: b"live-after-late\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("send live output after late attach");
    logical_clock += 1;

    let events = drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-after-late",
        &mut logical_clock,
    );
    let history_index = events
        .iter()
        .position(|event| {
            history_data(event).is_some_and(|(subscription_id, data)| {
                subscription_id == &late_subscription
                    && data
                        .windows(b"before-late".len())
                        .any(|window| window == b"before-late")
            })
        })
        .unwrap_or_else(|| {
            panic!("late subscription should receive prior output as history, got {events:?}")
        });
    let live_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id,
                    data,
                    ..
                } if subscription_id == &late_subscription
                    && data
                        .windows(b"after:live-after-late".len())
                        .any(|window| window == b"after:live-after-late")
            )
        })
        .expect("late subscription should receive later live output");

    assert!(
        history_index < live_index,
        "late history should precede later live output, got {events:?}"
    );
}

#[test]
fn late_attach_without_prior_output_does_not_fabricate_history() {
    let first_api = HubClientApi::local_operator("no-history-first-client");
    let late_api = HubClientApi::local_operator("no-history-late-client");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("no-history");
    let session_id = SessionId("no-history-session".to_string());
    let first_subscription = SubscriptionId("no-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("no-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("no-history-spawn"),
                session_id: session_id.clone(),
                command: "while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done"
                    .to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn no-history session");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("no-history-first-attach"),
                session_id: session_id.clone(),
                subscription_id: first_subscription,
                now_seconds: logical_clock,
            },
        )
        .expect("attach first no-history subscription");
    logical_clock += 1;

    late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("no-history-late-attach"),
                session_id: session_id.clone(),
                subscription_id: late_subscription.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach late no-history subscription");
    logical_clock += 1;

    first_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("no-history-input"),
                session_id: session_id.clone(),
                data: b"live-only\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("send live output after no-history late attach");
    logical_clock += 1;

    let events = drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-only",
        &mut logical_clock,
    );

    assert!(
        !events.iter().any(|event| {
            history_data(event).is_some_and(|(subscription_id, data)| {
                subscription_id == &late_subscription && !data.is_empty()
            })
        }),
        "late no-history subscription should not receive fabricated history, got {events:?}"
    );
}

#[test]
fn local_client_api_exercises_status_spawn_attach_input_resize_detach_shutdown_and_events() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let second_api = HubClientApi::local_operator("local-client-api-test-two");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("session-flow");
    let session_id = session_id();
    let subscription_id = subscription_id();
    let second_subscription_id = SubscriptionId("hub-client-api-subscription-two".to_string());
    let mut logical_clock = 100;

    let status = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Status {
                request_id: request_id("status"),
            },
        )
        .expect("status through client api");
    let HubClientResponseBody::Status(status) = status.body else {
        panic!("status response expected");
    };
    assert_eq!(status.profile_id, "botster-hub");
    assert_eq!(status.session_count, 0);

    let sessions = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessions {
                request_id: request_id("list-empty"),
            },
        )
        .expect("list through client api");
    assert!(
        matches!(sessions.body, HubClientResponseBody::Sessions(sessions) if sessions.is_empty())
    );

    let spawn = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id("spawn"),
                session_id: session_id.clone(),
                command: "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn through client api");
    logical_clock += 1;
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawned response expected");
    };
    assert_eq!(spawned.session.session_id, session_id);
    assert_eq!(spawned.session.lifecycle, SessionLifecycleState::Running);
    assert!(spawned.events.is_empty());

    let attach = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("attach"),
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach through client api");
    logical_clock += 1;
    assert!(matches!(attach.body, HubClientResponseBody::Events(_)));
    second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Attach {
                request_id: request_id("attach-two"),
                session_id: session_id.clone(),
                subscription_id: second_subscription_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("attach second client through client api");
    logical_clock += 1;

    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"ready",
        &mut logical_clock,
    );

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Resize {
            request_id: request_id("resize"),
            session_id: session_id.clone(),
            rows: 30,
            cols: 100,
            now_seconds: logical_clock,
        },
    )
    .expect("resize through client api");
    logical_clock += 1;

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Input {
            request_id: request_id("input"),
            session_id: session_id.clone(),
            data: b"ping-hub\n".to_vec(),
            now_seconds: logical_clock,
        },
    )
    .expect("input through client api");
    logical_clock += 1;

    let echo_events = drain_events_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        &subscription_id,
        b"echo:ping-hub",
        &mut logical_clock,
    );
    assert!(echo_events.iter().any(|event| {
        matches!(
            event,
            HubClientEvent::TerminalOutput {
                subscription_id: observed_subscription_id,
                data,
                ..
            } if observed_subscription_id == &subscription_id
                && data
                    .windows(b"echo:ping-hub".len())
                    .any(|window| window == b"echo:ping-hub")
        )
    }));
    assert!(
        echo_events.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id: observed_subscription_id,
                    data,
                    ..
                } if observed_subscription_id == &second_subscription_id
                    && data
                        .windows(b"echo:ping-hub".len())
                        .any(|window| window == b"echo:ping-hub")
            )
        }),
        "both attached subscriptions should receive shared session output"
    );

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Detach {
            request_id: request_id("detach"),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("detach through client api");
    logical_clock += 1;

    second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Input {
                request_id: request_id("input-after-detach"),
                session_id: session_id.clone(),
                data: b"after-detach\n".to_vec(),
                now_seconds: logical_clock,
            },
        )
        .expect("input from still-attached client through client api");
    logical_clock += 1;

    let after_detach_events = drain_events_until(
        &second_api,
        &mut runtime,
        &packages,
        &session_id,
        &second_subscription_id,
        b"echo:after-detach",
        &mut logical_clock,
    );
    let extra_after_detach_events = drain_events_for(
        &second_api,
        &mut runtime,
        &packages,
        &session_id,
        &mut logical_clock,
        Duration::from_millis(200),
    );
    assert!(
        after_detach_events
            .iter()
            .chain(extra_after_detach_events.iter())
            .all(|event| {
                !matches!(
                    event,
                    HubClientEvent::TerminalOutput {
                        subscription_id: observed_subscription_id,
                        data,
                        ..
                    } if observed_subscription_id == &subscription_id
                        && data
                            .windows(b"echo:after-detach".len())
                            .any(|window| window == b"echo:after-detach")
                )
            }),
        "detached subscription should not receive later output, including after an extra drain window"
    );

    let shutdown = second_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Shutdown {
                request_id: request_id("shutdown"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("shutdown through client api");
    let HubClientResponseBody::Events(events) = shutdown.body else {
        panic!("shutdown should return events");
    };
    assert!(events.is_empty());
}

#[test]
fn guarded_notification_write_is_hub_admitted_and_core_delivered() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let mut runtime = explicit_runtime("guarded-write");
    let session_actions = capability(
        CapabilitySurface::SessionActions,
        Some("guarded_session_notification_write"),
    );
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(
        vec![session_actions.clone(), surfaces.clone()]
            .into_iter()
            .collect(),
    );
    packages
        .install(
            plugin_manifest("workflow.plugin", vec![session_actions.clone()]),
            provenance(),
            "install package",
        )
        .expect("install allowed package");
    packages
        .enable("workflow.plugin", "enable package")
        .expect("enable allowed package");
    packages
        .install(
            plugin_manifest("blocked.plugin", vec![surfaces]),
            provenance(),
            "install blocked package",
        )
        .expect("install blocked package");
    packages
        .enable("blocked.plugin", "enable blocked package")
        .expect("enable blocked package");

    let session_id = SessionId("client-guarded".to_string());
    let subscription_id = SubscriptionId("client-guarded-subscription".to_string());
    let mut logical_clock = 200;
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Spawn {
            request_id: request_id("guarded-spawn"),
            session_id: session_id.clone(),
            command:
                "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
                    .to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn through client api");
    logical_clock += 1;
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Attach {
            request_id: request_id("guarded-attach"),
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            now_seconds: logical_clock,
        },
    )
    .expect("attach through client api");
    logical_clock += 1;

    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"ready",
        &mut logical_clock,
    );

    let mode_flags = ModeFlags {
        cursor_visible: true,
        ..ModeFlags::default()
    };
    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::GuardedNotificationWrite {
                request_id: request_id("guarded-write"),
                session_id: session_id.clone(),
                package_name: "workflow.plugin".to_string(),
                data: b"guarded-client\n".to_vec(),
                readiness: ReadinessEvidence::ready(mode_flags.clone()),
                now_seconds: logical_clock,
            },
        )
        .expect("allowed package should write through core daemon");
    logical_clock += 1;
    let HubClientResponseBody::GuardedWrite(result) = response.body else {
        panic!("guarded write response expected");
    };
    assert!(matches!(result.decision, GuardedWriteDecision::Write));
    assert_eq!(
        result.states,
        vec![
            GuardedWriteDeliveryState::Accepted,
            GuardedWriteDeliveryState::Written
        ],
        "core daemon owns guarded-write delivery states"
    );
    drain_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        b"echo:guarded-client",
        &mut logical_clock,
    );

    let denied = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::GuardedNotificationWrite {
                request_id: request_id("guarded-denied"),
                session_id,
                package_name: "blocked.plugin".to_string(),
                data: b"blocked\n".to_vec(),
                readiness: ReadinessEvidence::ready(mode_flags),
                now_seconds: logical_clock,
            },
        )
        .expect_err("ungranted package should be denied by hub policy");
    assert_eq!(
        denied,
        HubClientError::PackageCapabilityDenied {
            request_id: request_id("guarded-denied"),
            operation: HubClientOperation::GuardedNotificationWrite,
            package_name: "blocked.plugin".to_string(),
        }
    );
}

#[test]
fn read_screen_and_snapshot_return_typed_unsupported_until_daemon_api_exists() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("unsupported-daemon-ops");

    let read_screen = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadScreen {
                request_id: request_id("read-screen"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("daemon-backed read_screen should be typed unsupported");
    assert_eq!(
        read_screen,
        HubClientError::UnsupportedDaemonOperation {
            request_id: request_id("read-screen"),
            operation: HubClientOperation::ReadScreen,
            daemon_operation: "read_screen",
        }
    );

    let capture_snapshot = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CaptureSnapshot {
                request_id: request_id("capture-snapshot"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("daemon-backed capture_snapshot should be typed unsupported");
    assert_eq!(
        capture_snapshot,
        HubClientError::UnsupportedDaemonOperation {
            request_id: request_id("capture-snapshot"),
            operation: HubClientOperation::CaptureSnapshot,
            daemon_operation: "capture_snapshot",
        }
    );
}

#[test]
fn package_and_lifecycle_queries_are_sanitized_and_explicitly_pulled() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let mut runtime = explicit_runtime("packages");
    let surface = capability(CapabilitySurface::Surfaces, None);
    let network = capability(CapabilitySurface::Network, Some("localhost"));
    let package_root = "target/botster-hub-test-data/client-api-package-runnable";
    let _ = fs::remove_dir_all(package_root);
    fs::create_dir_all(format!("{package_root}/web")).expect("create package directories");
    fs::write(format!("{package_root}/plugin.lua"), "-- synthetic plugin").expect("write plugin");
    fs::write(format!("{package_root}/web/dev-server"), "#!/bin/sh\n")
        .expect("write runnable command");
    fs::write(
        format!("{package_root}/botster-package.json"),
        r#"{
  "name": "workflow.plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [{ "surface": "surfaces" }],
  "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }],
  "surfaces": [{
    "id": "workflow.home",
    "kind": "app",
    "title": "Workflow Home",
    "description": "Workflow dashboard",
    "icon": "workflow",
    "order": 10,
    "category": "workflows",
    "supports": ["render", "action"]
  }],
  "runnable_entrypoints": [{
    "id": "web",
    "kind": "web_app",
    "command": "web/dev-server",
    "args": ["--host", "127.0.0.1"],
    "working_directory": { "policy": "relative", "path": "web" },
    "environment": [{ "name": "BOTSTER_WEB_PORT", "required": false, "default": "5173" }],
    "launch_mode": "background",
    "capabilities": [{ "surface": "network", "scope": "localhost" }],
    "may_supervise": true
  }]
}
"#,
    )
    .expect("write package manifest");
    let mut packages = PackageRegistry::new(vec![surface.clone(), network].into_iter().collect());
    packages
        .install_local_path(package_root, "install package")
        .expect("install package");
    packages
        .enable("workflow.plugin", "enable package")
        .expect("enable package");

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Attach {
            request_id: request_id("attach-missing-session"),
            session_id: session_id(),
            subscription_id: subscription_id(),
            now_seconds: 1,
        },
    )
    .expect_err("attach is a transport handshake and should not hydrate packages");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackages {
                request_id: request_id("packages"),
            },
        )
        .expect("package query through client api");
    let HubClientResponseBody::Packages(records) = response.body else {
        panic!("packages response expected");
    };
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.package_name, "workflow.plugin");
    assert_eq!(
        record.classification,
        HubClientPackageClassification::Plugin
    );
    assert_eq!(record.state, HubClientPackageState::Enabled);
    assert_eq!(record.surfaces.len(), 1);
    let surface = &record.surfaces[0];
    assert_eq!(surface.id, "workflow.home");
    assert_eq!(surface.kind, "app");
    assert_eq!(surface.title, "Workflow Home");
    assert_eq!(surface.description.as_deref(), Some("Workflow dashboard"));
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(10));
    assert_eq!(surface.category.as_deref(), Some("workflows"));
    assert_eq!(surface.supports, ["render", "action"]);
    assert_eq!(record.runnable_entrypoints.len(), 1);
    let entrypoint = &record.runnable_entrypoints[0];
    assert_eq!(entrypoint.id, "web");
    assert_eq!(entrypoint.kind, "web_app");
    assert_eq!(entrypoint.command, "web/dev-server");
    assert_eq!(entrypoint.args, ["--host", "127.0.0.1"]);
    assert_eq!(entrypoint.working_directory.policy, "relative");
    assert_eq!(entrypoint.working_directory.path.as_deref(), Some("web"));
    assert_eq!(entrypoint.environment[0].name, "BOTSTER_WEB_PORT");
    assert_eq!(entrypoint.environment[0].default.as_deref(), Some("5173"));
    assert_eq!(entrypoint.launch_mode, "background");
    assert_eq!(entrypoint.capabilities[0].surface, "Network");
    assert!(entrypoint.may_supervise);
    assert_eq!(entrypoint.process.state, "not_started");
    assert!(
        !format!("{record:?}").contains("local-private-source"),
        "package client response must not expose provenance"
    );
    assert!(
        !format!("{record:?}").contains(package_root),
        "package client response must not expose local package root"
    );

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::PluginLifecycleStatus {
                request_id: request_id("plugin-lifecycle"),
            },
        )
        .expect("plugin lifecycle status through client api");
    let HubClientResponseBody::PluginLifecycle(records) = response.body else {
        panic!("plugin lifecycle response expected");
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].package_name, "workflow.plugin");
    assert_eq!(records[0].state, HubClientPackageState::Enabled);
    assert!(!records[0].loaded);
}

#[test]
fn denied_client_request_returns_typed_admission_error() {
    let api = HubClientApi::new(
        HubClientIdentity {
            client_id: botster_core::ClientId("denied-client".to_string()),
            role: HubClientRole::Unadmitted,
        },
        HubClientAdmission::deny_all(),
    );
    let mut runtime = explicit_runtime("denied");
    let packages = empty_registry();

    let error = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Status {
                request_id: request_id("denied-status"),
            },
        )
        .expect_err("denied client should fail");

    assert_eq!(
        error,
        HubClientError::AdmissionDenied {
            request_id: request_id("denied-status"),
            operation: HubClientOperation::Status,
            role: HubClientRole::Unadmitted,
        }
    );

    let error = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Shutdown {
                request_id: request_id("denied-shutdown"),
                session_id: session_id(),
                now_seconds: 1,
            },
        )
        .expect_err("denied client should not shut down sessions");

    assert_eq!(
        error,
        HubClientError::AdmissionDenied {
            request_id: request_id("denied-shutdown"),
            operation: HubClientOperation::Shutdown,
            role: HubClientRole::Unadmitted,
        }
    );
}
