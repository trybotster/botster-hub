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
    SessionId, SessionLifecycleState, SubscriptionId, TerminalAttachState,
};
use botster_core_daemon::{GuardedWriteDecision, GuardedWriteDeliveryState, ReadinessEvidence};
use botster_hub::{
    DataDirectoryOption, DeviceSessionTypeSource, FileHubStateStore, HostIdentityOptions,
    HubClientAdmission, HubClientApi, HubClientError, HubClientEvent, HubClientIdentity,
    HubClientOperation, HubClientPackageClassification, HubClientPackageState, HubClientRequest,
    HubClientResponseBody, HubClientRole, HubPackageManifest, HubRuntime, HubStartupOptions,
    HubStateStore, PackageProvenance, PackageRegistry, PackageSessionType,
    PackageSessionTypeExecution, PackageSessionTypeWorkingDirectory, RuntimeEnvironment,
    SessionDefaults, SessionTypeMutationSource, SpawnTarget, TransportBindings,
};
use botster_ui_contract::{
    PackageNavigationEntry, PackageNavigationTarget, PackageSurfaceDescriptor, PackageSurfaceKind,
    PackageSurfaceOperation,
};

mod support;
use support::ensure_session_worker_binary;

fn explicit_runtime(name: &str) -> HubRuntime {
    ensure_session_worker_binary();
    let data_directory = format!(
        "target/botster-hub-test-data/client-api-{}-{name}",
        std::process::id()
    );
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config)
}

#[test]
fn session_type_device_crud_is_authoritative_and_package_mutation_is_read_only() {
    let mut runtime = explicit_runtime("session-type-device-crud");
    let config = runtime.config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets.push(SpawnTarget {
                target_id: "repo:concurrent".to_string(),
                label: "Concurrently persisted target".to_string(),
                root: std::path::PathBuf::from("."),
                enabled: false,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            });
        })
        .expect("persist state after runtime loaded");
    let packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    let api = HubClientApi::local_operator("session-type-device-crud-client");
    let mut definition = session_type("bin/accessory.sh", "accessory");
    definition.id = "terminal-accessory".to_string();
    definition.label = "Terminal accessory".to_string();
    definition.role = "botster.accessory".to_string();
    definition.interaction = "interactive".to_string();
    definition.traits = vec!["terminal".to_string()];
    definition.lifecycle = "persistent".to_string();

    let created = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CreateSessionType {
                request_id: request_id("create-device-session-type"),
                source: SessionTypeMutationSource::Device,
                definition: definition.clone(),
            },
        )
        .expect("create device session type");
    let HubClientResponseBody::SessionTypes(created) = created.body else {
        panic!("session type response expected");
    };
    assert_eq!(created.len(), 1);
    assert!(created[0].editable);
    assert_eq!(created[0].role, "botster.accessory");
    assert_eq!(runtime.state().session_type_generation, 1);
    assert_eq!(
        runtime.state().spawn_targets[0].target_id,
        "repo:concurrent",
        "session type CRUD must mutate freshly loaded state without overwriting unrelated writes"
    );

    definition.label = "Updated terminal accessory".to_string();
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::UpdateSessionType {
            request_id: request_id("update-device-session-type"),
            source: SessionTypeMutationSource::Device,
            definition,
        },
    )
    .expect("update device session type");
    assert_eq!(runtime.state().session_type_generation, 2);

    let rejected = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::DeleteSessionType {
                request_id: request_id("delete-package-session-type"),
                source: SessionTypeMutationSource::Package {
                    package_name: "read-only.plugin".to_string(),
                },
                session_type_id: "terminal-accessory".to_string(),
            },
        )
        .expect_err("package source is read-only");
    assert!(matches!(
        rejected,
        HubClientError::SessionType {
            kind: "read_only_session_type_source",
            ..
        }
    ));

    let deleted = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::DeleteSessionType {
                request_id: request_id("delete-device-session-type"),
                source: SessionTypeMutationSource::Device,
                session_type_id: "terminal-accessory".to_string(),
            },
        )
        .expect("delete device session type");
    let HubClientResponseBody::SessionTypes(deleted) = deleted.body else {
        panic!("session type response expected");
    };
    assert!(deleted.is_empty());
    assert_eq!(runtime.state().session_type_generation, 3);
}

/// A definition that the sanitized row provably cannot reconstruct: a relative
/// working-directory path and a non-empty authored environment.
fn authored_session_type(id: &str) -> PackageSessionType {
    PackageSessionType {
        id: id.to_string(),
        label: "Authored agent".to_string(),
        description: Some("Carries an authored path and environment".to_string()),
        icon: Some("terminal".to_string()),
        role: "botster.agent".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["terminal".to_string(), "authoring".to_string()],
        lifecycle: "task".to_string(),
        execution: PackageSessionTypeExecution::RelativeExecutable,
        command: "bin/authored.sh".to_string(),
        args: vec!["--json".to_string()],
        working_directory: PackageSessionTypeWorkingDirectory::Relative {
            path: "nested/dir".to_string(),
        },
        environment: BTreeMap::from([
            ("BOTSTER_MODE".to_string(), "authored".to_string()),
            (
                "AUTHORED_SECRET_NAME".to_string(),
                "authored-value".to_string(),
            ),
        ]),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context: vec!["prompt".to_string()],
        target_id: None,
    }
}

fn read_definition(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    label: &str,
    session_type_id: &str,
) -> botster_hub::HubSessionTypeDefinition {
    let response = api
        .handle_request(
            runtime,
            packages,
            HubClientRequest::ShowSessionTypeDefinition {
                request_id: request_id(label),
                session_type_id: session_type_id.to_string(),
            },
        )
        .expect("read authored session type definition");
    let HubClientResponseBody::SessionTypeDefinition(definition) = response.body else {
        panic!("session type definition response expected");
    };
    *definition
}

fn shown_row(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    label: &str,
    session_type_id: &str,
) -> botster_hub::HubSessionType {
    let response = api
        .handle_request(
            runtime,
            packages,
            HubClientRequest::ShowSessionType {
                request_id: request_id(label),
                session_type_id: session_type_id.to_string(),
            },
        )
        .expect("show sanitized session type row");
    let HubClientResponseBody::SessionTypes(mut rows) = response.body else {
        panic!("session types response expected");
    };
    assert_eq!(rows.len(), 1);
    rows.remove(0)
}

#[test]
fn session_type_definition_round_trips_authored_path_and_environment() {
    let mut runtime = explicit_runtime("session-type-definition-round-trip");
    let packages = empty_registry();
    let api = HubClientApi::local_operator("session-type-definition-round-trip-client");

    // Two definitions: one with every optional field set, one with them all unset,
    // so a `skip_serializing_if` None-versus-absent slip cannot pass silently.
    let populated = authored_session_type("authored-populated");
    let mut sparse = authored_session_type("authored-sparse");
    sparse.description = None;
    sparse.icon = None;
    sparse.target_id = None;
    sparse.args = Vec::new();
    sparse.traits = Vec::new();

    for definition in [populated.clone(), sparse.clone()] {
        api.handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CreateSessionType {
                request_id: request_id(&format!("create-{}", definition.id)),
                source: SessionTypeMutationSource::Device,
                definition,
            },
        )
        .expect("create authored device session type");
    }

    for authored in [populated, sparse] {
        let read = read_definition(
            &api,
            &mut runtime,
            &packages,
            &format!("definition-{}", authored.id),
            &authored.id,
        );

        // The read is lossless and carries the exact mutation source Update needs.
        assert_eq!(read.definition, authored, "authoring read must be lossless");
        assert_eq!(read.source, SessionTypeMutationSource::Device);
        assert_eq!(read.session_type_id, format!("device/{}", authored.id));
        assert_eq!(
            read.definition.id, authored.id,
            "definition.id must be the bare id Update matches on, not the composite id"
        );

        // Submit the read back unchanged; the stored definition must be identical.
        api.handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::UpdateSessionType {
                request_id: request_id(&format!("round-trip-{}", authored.id)),
                source: read.source.clone(),
                definition: read.definition.clone(),
            },
        )
        .expect("submit the authoring read back through Update");

        let stored = runtime
            .state()
            .device_session_type_sources
            .iter()
            .flat_map(|source| source.session_types.iter())
            .find(|stored| stored.id == authored.id)
            .cloned()
            .expect("round-tripped definition is still stored");
        assert_eq!(
            stored, authored,
            "read-modify-write must not lose the authored working-directory path or environment"
        );
        assert_eq!(
            stored.working_directory,
            PackageSessionTypeWorkingDirectory::Relative {
                path: "nested/dir".to_string()
            }
        );
        assert!(!stored.environment.is_empty());
    }
}

#[test]
fn sanitized_session_type_row_still_cannot_reconstruct_the_authored_definition() {
    let mut runtime = explicit_runtime("session-type-sanitized-row-is-lossy");
    let packages = empty_registry();
    let api = HubClientApi::local_operator("session-type-sanitized-row-client");
    let authored = authored_session_type("authored-lossy");
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::CreateSessionType {
            request_id: request_id("create-lossy-source"),
            source: SessionTypeMutationSource::Device,
            definition: authored.clone(),
        },
    )
    .expect("create authored device session type");

    // What a client could reconstruct before this seam existed: the row derives a
    // policy string and has no environment field at all, so both are destroyed.
    let row = shown_row(&api, &mut runtime, &packages, "show-lossy", &authored.id);
    assert_eq!(row.working_directory_policy, "relative");
    let reconstructed_from_row = PackageSessionType {
        id: row.id.clone(),
        label: row.label.clone(),
        description: row.description.clone(),
        icon: row.icon.clone(),
        role: row.role.clone(),
        interaction: row.interaction.clone(),
        traits: row.traits.clone(),
        lifecycle: row.lifecycle.clone(),
        execution: row.execution.clone(),
        command: row.command.clone(),
        args: row.args.clone(),
        working_directory: PackageSessionTypeWorkingDirectory::default(),
        environment: BTreeMap::new(),
        allowed_environment_overrides: row.allowed_environment_overrides.clone(),
        context: row.context_keys.clone(),
        target_id: None,
    };
    assert_ne!(
        reconstructed_from_row, authored,
        "the sanitized row must remain insufficient to rebuild an authored definition"
    );
    assert_eq!(
        reconstructed_from_row.working_directory,
        PackageSessionTypeWorkingDirectory::PackageRoot
    );
    assert!(reconstructed_from_row.environment.is_empty());

    // And the sanitized surfaces did not move: no authored environment value and no
    // authored path appears in the published row, in list, or in the entity payload.
    let listed = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-lossy"),
            },
        )
        .expect("list session types");
    let HubClientResponseBody::SessionTypes(listed) = listed.body else {
        panic!("session types response expected");
    };
    assert_eq!(listed, vec![row.clone()]);

    let published = serde_json::to_value(&row).expect("session_type entity payload serializes");
    let published_keys = published
        .as_object()
        .expect("row serializes as an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    // serde_json orders object keys alphabetically.
    assert_eq!(
        published_keys,
        vec![
            "allowed_environment_overrides",
            "args",
            "available",
            "command",
            "context_keys",
            "description",
            "diagnostics",
            "editable",
            "execution",
            "icon",
            "id",
            "interaction",
            "label",
            "lifecycle",
            "overridden_sources",
            "role",
            "session_type_id",
            "source",
            "source_name",
            "target_id",
            "traits",
            "working_directory_policy",
        ],
        "the published session_type row shape must match the explicit contract"
    );
    let published_text = published.to_string();
    assert!(!published_text.contains("nested/dir"));
    assert!(!published_text.contains("authored-value"));
    assert!(!published_text.contains("AUTHORED_SECRET_NAME"));
}

#[test]
fn session_type_definition_refuses_package_sources_and_denied_admission() {
    let package_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-definition-package",
    );
    let _ = fs::remove_dir_all(&package_root);
    write_session_type_package(&package_root);
    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&package_root, "install definition package")
        .expect("install package");
    packages
        .enable("session-type.plugin", "enable definition package")
        .expect("enable package");

    let mut runtime = explicit_runtime("session-type-definition-package-refusal");
    let api = HubClientApi::local_operator("session-type-definition-package-client");

    let refused = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionTypeDefinition {
                request_id: request_id("definition-package-source"),
                session_type_id: "init".to_string(),
            },
        )
        .expect_err("package-owned definitions stay read-only");
    assert!(matches!(
        refused,
        HubClientError::SessionType {
            kind: "read_only_session_type_source",
            ..
        }
    ));

    let unknown = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionTypeDefinition {
                request_id: request_id("definition-unknown"),
                session_type_id: "missing".to_string(),
            },
        )
        .expect_err("unknown ids stay typed");
    assert!(matches!(
        unknown,
        HubClientError::SessionType {
            kind: "unknown_session_type",
            ..
        }
    ));

    // The production denial path: an unadmitted caller cannot read authored data.
    let denied_api = HubClientApi::new(
        HubClientIdentity {
            client_id: botster_core::ClientId("denied-definition-client".to_string()),
            role: HubClientRole::LocalOperator,
        },
        HubClientAdmission::deny_all(),
    );
    let denied = denied_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionTypeDefinition {
                request_id: request_id("definition-denied"),
                session_type_id: "init".to_string(),
            },
        )
        .expect_err("unadmitted callers are refused before policy runs");
    assert!(matches!(
        denied,
        HubClientError::AdmissionDenied {
            operation: HubClientOperation::ShowSessionTypeDefinition,
            ..
        }
    ));
}

#[test]
fn session_type_role_interaction_traits_and_lifecycle_are_orthogonal() {
    let mut runtime = explicit_runtime("session-type-orthogonal-semantics");
    let packages = empty_registry();
    let api = HubClientApi::local_operator("session-type-orthogonal-semantics-client");
    let cases = [
        (
            "interactive-agent",
            "botster.agent",
            "interactive",
            vec!["terminal"],
            "task",
        ),
        (
            "interactive-accessory",
            "botster.accessory",
            "interactive",
            vec!["terminal", "companion"],
            "persistent",
        ),
        (
            "service-accessory",
            "botster.accessory",
            "service",
            vec!["background"],
            "persistent",
        ),
    ];

    for (id, role, interaction, traits, lifecycle) in cases {
        let mut definition = session_type("bin/session.sh", id);
        definition.id = id.to_string();
        definition.label = id.replace('-', " ");
        definition.role = role.to_string();
        definition.interaction = interaction.to_string();
        definition.traits = traits.into_iter().map(str::to_string).collect();
        definition.lifecycle = lifecycle.to_string();
        api.handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CreateSessionType {
                request_id: request_id(&format!("create-{id}")),
                source: SessionTypeMutationSource::Device,
                definition,
            },
        )
        .expect("orthogonal session type should be accepted");
    }

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-orthogonal-session-types"),
            },
        )
        .expect("list orthogonal session types");
    let HubClientResponseBody::SessionTypes(session_types) = response.body else {
        panic!("session type response expected");
    };
    let semantics = session_types
        .into_iter()
        .map(|session_type| {
            (
                session_type.id,
                (
                    session_type.role,
                    session_type.interaction,
                    session_type.traits,
                    session_type.lifecycle,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        semantics["interactive-agent"],
        (
            "botster.agent".to_string(),
            "interactive".to_string(),
            vec!["terminal".to_string()],
            "task".to_string(),
        )
    );
    assert_eq!(
        semantics["interactive-accessory"],
        (
            "botster.accessory".to_string(),
            "interactive".to_string(),
            vec!["terminal".to_string(), "companion".to_string()],
            "persistent".to_string(),
        )
    );
    assert_eq!(
        semantics["service-accessory"],
        (
            "botster.accessory".to_string(),
            "service".to_string(),
            vec!["background".to_string()],
            "persistent".to_string(),
        )
    );
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

#[test]
fn session_entity_subscription_uses_core_baseline_and_rejects_other_families() {
    let mut runtime = explicit_runtime("session-entity-subscription");
    let api = HubClientApi::local_operator("session-entity-client");
    let packages = empty_registry();

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::SubscribeEntities {
                request_id: request_id("subscribe-sessions"),
                entity_type: "session".to_string(),
                subscription_id: "session-entities".to_string(),
            },
        )
        .expect("session entity family is admitted");
    let HubClientResponseBody::SessionLifecycleBaseline(baseline) = response.body else {
        panic!("expected CoreDaemon lifecycle baseline");
    };
    assert!(baseline.sessions.is_empty());

    let error = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::SubscribeEntities {
                request_id: request_id("subscribe-unrelated"),
                entity_type: "package".to_string(),
                subscription_id: "unrelated".to_string(),
            },
        )
        .expect_err("unrelated families must not be hydrated");
    assert!(matches!(
        error,
        HubClientError::InvalidRequest {
            operation: HubClientOperation::SubscribeEntities,
            ..
        }
    ));
}

fn write_session_type_package(root: &std::path::Path) {
    write_named_session_type_package(root, "session-type.plugin");
}

fn write_executable_script(root: &std::path::Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("script has parent")).expect("create script parent");
    fs::write(&path, contents).expect("write script");
    let mut permissions = fs::metadata(&path).expect("script metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("chmod script");
}

fn session_type(command: &str, mode: &str) -> PackageSessionType {
    PackageSessionType {
        id: "init".to_string(),
        label: "Test session".to_string(),
        description: None,
        icon: None,
        role: "botster.agent".to_string(),
        interaction: "interactive".to_string(),
        traits: vec!["test".to_string()],
        lifecycle: "task".to_string(),
        execution: PackageSessionTypeExecution::RelativeExecutable,
        command: command.to_string(),
        args: Vec::new(),
        working_directory: PackageSessionTypeWorkingDirectory::PackageRoot,
        environment: BTreeMap::from([("BOTSTER_MODE".to_string(), mode.to_string())]),
        allowed_environment_overrides: vec!["BOTSTER_MODE".to_string()],
        context: vec!["prompt".to_string()],
        target_id: None,
    }
}

fn write_repo_session_types(root: &std::path::Path, templates: serde_json::Value) {
    fs::create_dir_all(root.join(".botster")).expect("create repo .botster dir");
    fs::write(
        root.join(".botster/session-types.json"),
        serde_json::json!({ "session_types": templates }).to_string(),
    )
    .expect("write repo session types");
}

fn write_named_session_type_package(root: &std::path::Path, package_name: &str) {
    fs::create_dir_all(root.join("bin")).expect("create session type package root");
    fs::write(root.join("plugin.lua"), "return botster.register({})\n")
        .expect("write plugin entrypoint");
    let script = root.join("bin/init.sh");
    fs::write(
        &script,
        "#!/bin/sh\nprintf 'template:%s:%s\\n' \"$BOTSTER_SESSION_ID\" \"$BOTSTER_MODE\"\n",
    )
    .expect("write session type script");
    let mut permissions = fs::metadata(&script)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).expect("chmod session type script");
    fs::write(
        root.join("botster-package.json"),
        r#"{
  "name": "__PACKAGE_NAME__",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "session_types": [
    {
      "id": "init",
      "label": "Test agent",
      "role": "botster.agent",
      "interaction": "interactive",
      "traits": ["test"],
      "lifecycle": "task",
      "command": "bin/init.sh",
      "environment": { "BOTSTER_MODE": "default" },
      "allowed_environment_overrides": ["BOTSTER_MODE"],
      "context": ["prompt"]
    }
  ]
}
"#
        .replace("__PACKAGE_NAME__", package_name),
    )
    .expect("write session type package manifest");
}

fn capability(surface: CapabilitySurface, scope: Option<&str>) -> Capability {
    Capability {
        surface,
        scope: scope.map(ToString::to_string),
    }
}

fn plugin_manifest(name: &str, capabilities: Vec<Capability>) -> HubPackageManifest {
    HubPackageManifest {
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
        navigation: Vec::new(),
        events: botster_hub::HubPackageEvents::default(),
    }
}

fn provenance() -> PackageProvenance {
    PackageProvenance {
        source: "local-private-source".to_string(),
        checksum: Some("sha256:test".to_string()),
    }
}

fn configurable_plugin_manifest(name: &str, capabilities: Vec<Capability>) -> HubPackageManifest {
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

fn project_pipelines_manifest_with_github_feature() -> HubPackageManifest {
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

fn capability_gated_plugin_manifest() -> HubPackageManifest {
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

fn app_surface(id: &str, title: &str) -> PackageSurfaceDescriptor {
    PackageSurfaceDescriptor {
        id: id.to_string(),
        kind: PackageSurfaceKind::App,
        title: title.to_string(),
        description: Some(format!("{title} surface")),
        icon: Some("workflow".to_string()),
        order: Some(99),
        category: Some("workflows".to_string()),
        supports: vec![
            PackageSurfaceOperation::Render,
            PackageSurfaceOperation::Action,
        ],
    }
}

#[test]
fn session_types_resolve_spawn_context_and_reject_unadmitted_reads() {
    let package_root =
        std::path::PathBuf::from("target/botster-hub-test-data/client-api-session-type-package");
    let _ = fs::remove_dir_all(&package_root);
    write_session_type_package(&package_root);
    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&package_root, "install session type package")
        .expect("install session type package");
    packages
        .enable("session-type.plugin", "enable session type package")
        .expect("enable session type package");
    let mut runtime = explicit_runtime("session-type");
    let api = HubClientApi::local_operator("session-type-client");

    let list = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-session-types"),
            },
        )
        .expect("list templates");
    let HubClientResponseBody::SessionTypes(templates) = list.body else {
        panic!("session types response expected");
    };
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].id, "init");

    let rejected_env = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-rejected-env"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    environment: BTreeMap::from([(
                        "BOTSTER_UNDECLARED".to_string(),
                        "no".to_string(),
                    )]),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("undeclared env override rejected");
    assert!(matches!(
        rejected_env,
        HubClientError::SessionType {
            kind: "environment_not_admitted",
            ..
        }
    ));

    let rejected_target = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-rejected-target"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    target_id: Some("package:other-template.plugin".to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("unadmitted target override rejected");
    assert!(matches!(
        rejected_target,
        HubClientError::SessionType {
            kind: "target_not_admitted",
            ..
        }
    ));

    let rejected_cwd = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-rejected-cwd"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    cwd: Some("/tmp/outside-template-root".to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("unadmitted cwd override rejected");
    assert!(matches!(
        rejected_cwd,
        HubClientError::SessionType {
            kind: "cwd_not_admitted",
            ..
        }
    ));

    let resolved = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-generic-template-id"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    environment: BTreeMap::from([(
                        "BOTSTER_MODE".to_string(),
                        "override".to_string(),
                    )]),
                    context: botster_hub::SessionTypeContextInput {
                        prompt: Some("hello from api".to_string()),
                        ..botster_hub::SessionTypeContextInput::default()
                    },
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect("resolve bare generic template id");
    let HubClientResponseBody::ResolvedSessionType(resolved) = resolved.body else {
        panic!("resolved template response expected");
    };
    assert_eq!(resolved.session_type.id, "init");
    assert_eq!(
        resolved.environment.get("BOTSTER_MODE").map(String::as_str),
        Some("override")
    );
    assert!(resolved.environment.contains_key("BOTSTER_CONTEXT_ID"));

    let spawn = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::SpawnSessionType {
                request_id: request_id("spawn-session-type"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    session_id: Some(SessionId("session-type-api-session".to_string())),
                    context: botster_hub::SessionTypeContextInput {
                        prompt: Some("hello from spawn".to_string()),
                        ..botster_hub::SessionTypeContextInput::default()
                    },
                    ..botster_hub::SessionTypeRequest::default()
                },
                now_seconds: 1,
            },
        )
        .expect("spawn session type");
    let HubClientResponseBody::Spawned(spawned) = spawn.body else {
        panic!("spawned response expected");
    };
    assert_eq!(spawned.session.session_id.0, "session-type-api-session");

    let context = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadSessionContext {
                request_id: request_id("read-session-context"),
                session_id: SessionId("session-type-api-session".to_string()),
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
                session_id: SessionId("session-type-api-session".to_string()),
                context_id: None,
                key: None,
            },
        )
        .expect_err("unadmitted context reads are denied");
    assert!(matches!(denied, HubClientError::AdmissionDenied { .. }));
}

#[test]
fn session_type_show_rejects_ambiguous_bare_ids() {
    let first_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-first-package",
    );
    let second_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-second-package",
    );
    let _ = fs::remove_dir_all(&first_root);
    let _ = fs::remove_dir_all(&second_root);
    write_named_session_type_package(&first_root, "first-template.plugin");
    write_named_session_type_package(&second_root, "second-template.plugin");

    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&first_root, "install first session type package")
        .expect("install first session type package");
    packages
        .enable("first-template.plugin", "enable first session type package")
        .expect("enable first session type package");
    packages
        .install_local_path(&second_root, "install second session type package")
        .expect("install second session type package");
    packages
        .enable(
            "second-template.plugin",
            "enable second session type package",
        )
        .expect("enable second session type package");

    let mut runtime = explicit_runtime("session-type-show-ambiguous");
    let api = HubClientApi::local_operator("session-type-show-ambiguous-client");

    let rejected = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionType {
                request_id: request_id("show-ambiguous-template"),
                session_type_id: "init".to_string(),
            },
        )
        .expect_err("ambiguous bare template id should be rejected");
    assert!(matches!(
        rejected,
        HubClientError::SessionType {
            kind: "ambiguous_session_type",
            ..
        }
    ));

    let shown = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionType {
                request_id: request_id("show-full-template-id"),
                session_type_id: "first-template.plugin/init".to_string(),
            },
        )
        .expect("full template id remains unambiguous");
    let HubClientResponseBody::SessionTypes(templates) = shown.body else {
        panic!("session types response expected");
    };
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].session_type_id, "first-template.plugin/init");
}

#[test]
fn session_type_sources_apply_device_repo_precedence_and_reload_from_state() {
    let package_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-precedence-package",
    );
    let device_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-precedence-device",
    );
    let repo_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-precedence-repo",
    );
    let _ = fs::remove_dir_all(&package_root);
    let _ = fs::remove_dir_all(&device_root);
    let _ = fs::remove_dir_all(&repo_root);
    write_session_type_package(&package_root);
    write_executable_script(
        &device_root,
        "bin/device.sh",
        "#!/bin/sh\nprintf 'device:%s\\n' \"$BOTSTER_MODE\"\n",
    );
    write_executable_script(
        &repo_root,
        "bin/repo.sh",
        "#!/bin/sh\nprintf 'repo:%s\\n' \"$BOTSTER_MODE\"\n",
    );
    fs::create_dir_all(repo_root.join(".botster")).expect("create repo .botster dir");
    fs::write(
        repo_root.join(".botster/session-types.json"),
        serde_json::json!({
            "session_types": [{
                "id": "init",
                "label": "Repo agent",
                "role": "botster.agent",
                "interaction": "interactive",
                "traits": ["test"],
                "lifecycle": "task",
                "command": "bin/repo.sh",
                "environment": { "BOTSTER_MODE": "repo" },
                "allowed_environment_overrides": ["BOTSTER_MODE"],
                "context": ["prompt"]
            }]
        })
        .to_string(),
    )
    .expect("write repo session types");

    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&package_root, "install precedence package")
        .expect("install package");
    packages
        .enable("session-type.plugin", "enable precedence package")
        .expect("enable package");

    let config = explicit_runtime("session-type-precedence").config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![session_type("bin/device.sh", "device")],
            }];
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:main".to_string(),
                label: "repo:main".to_string(),
                root: repo_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist session type sources");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let api = HubClientApi::local_operator("session-type-precedence-client");

    let list = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-merged-session-types"),
            },
        )
        .expect("list merged templates");
    let HubClientResponseBody::SessionTypes(templates) = list.body else {
        panic!("session types response expected");
    };
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].source, "repo");
    assert_eq!(templates[0].target_id, "repo:main");
    assert!(templates[0].editable);
    assert_eq!(templates[0].overridden_sources.len(), 2);
    assert_eq!(
        templates[0].diagnostics,
        vec!["overrides 2 lower-precedence definition(s)"]
    );
    let listed = templates[0].clone();

    for (suffix, session_type_id) in [
        ("bare", "init".to_string()),
        ("qualified", listed.session_type_id.clone()),
    ] {
        let shown = api
            .handle_request(
                &mut runtime,
                &packages,
                HubClientRequest::ShowSessionType {
                    request_id: request_id(&format!("show-{suffix}-repo-template")),
                    session_type_id: session_type_id.clone(),
                },
            )
            .expect("show repo override");
        let HubClientResponseBody::SessionTypes(shown) = shown.body else {
            panic!("shown session type expected");
        };
        assert_eq!(shown, vec![listed.clone()]);

        let resolved = api
            .handle_request(
                &mut runtime,
                &packages,
                HubClientRequest::ResolveSessionType {
                    request_id: request_id(&format!("resolve-{suffix}-repo-template")),
                    session_type_id,
                    session_type_request: botster_hub::SessionTypeRequest {
                        environment: BTreeMap::from([(
                            "BOTSTER_MODE".to_string(),
                            "explicit".to_string(),
                        )]),
                        ..botster_hub::SessionTypeRequest::default()
                    },
                },
            )
            .expect("resolve repo override");
        let HubClientResponseBody::ResolvedSessionType(resolved) = resolved.body else {
            panic!("resolved session type expected");
        };
        assert_eq!(resolved.session_type, listed);
        assert_eq!(
            resolved.executable,
            repo_root.join("bin/repo.sh").display().to_string()
        );
        assert_eq!(
            resolved.environment.get("BOTSTER_MODE").map(String::as_str),
            Some("explicit")
        );
    }

    let rejected = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-repo-rejected-cwd"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    cwd: Some(device_root.display().to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("repo cwd outside target rejected");
    assert!(matches!(
        rejected,
        HubClientError::SessionType {
            kind: "cwd_not_admitted",
            ..
        }
    ));

    let mut updated_repo = session_type("bin/repo.sh", "repo-updated");
    updated_repo.label = "Updated repo agent".to_string();
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::UpdateSessionType {
            request_id: request_id("update-admitted-repo-session-type"),
            source: SessionTypeMutationSource::Repo {
                target_id: "repo:main".to_string(),
            },
            definition: updated_repo,
        },
    )
    .expect("update repo definition through admitted target policy");
    assert!(
        fs::read_to_string(repo_root.join(".botster/session-types.json"))
            .expect("read Hub-written repo session types")
            .contains("Updated repo agent")
    );
    let deleted = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::DeleteSessionType {
                request_id: request_id("delete-admitted-repo-session-type"),
                source: SessionTypeMutationSource::Repo {
                    target_id: "repo:main".to_string(),
                },
                session_type_id: "init".to_string(),
            },
        )
        .expect("delete repo definition through admitted target policy");
    let HubClientResponseBody::SessionTypes(after_delete) = deleted.body else {
        panic!("session type response expected");
    };
    assert_eq!(after_delete[0].source, "device");
    assert_eq!(runtime.state().session_type_generation, 2);
}

#[test]
fn session_type_definition_round_trips_repo_sources_and_preserves_selection() {
    let device_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-definition-device",
    );
    let repo_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-definition-repo",
    );
    let _ = fs::remove_dir_all(&device_root);
    let _ = fs::remove_dir_all(&repo_root);
    write_executable_script(
        &device_root,
        "bin/authored.sh",
        "#!/bin/sh\nprintf 'device:%s\\n' \"$BOTSTER_MODE\"\n",
    );
    write_executable_script(
        &repo_root,
        "bin/authored.sh",
        "#!/bin/sh\nprintf 'repo:%s\\n' \"$BOTSTER_MODE\"\n",
    );

    // Same bare id in both device and repo, so repo wins on precedence and the
    // device definition is only reachable through its qualified id.
    let mut device_authored = authored_session_type("authored-shared");
    device_authored.label = "Device authored agent".to_string();
    device_authored.working_directory = PackageSessionTypeWorkingDirectory::Relative {
        path: "device/nested".to_string(),
    };
    let mut repo_authored = authored_session_type("authored-shared");
    repo_authored.label = "Repo authored agent".to_string();
    repo_authored.working_directory = PackageSessionTypeWorkingDirectory::Relative {
        path: "repo/nested".to_string(),
    };
    write_repo_session_types(
        &repo_root,
        serde_json::to_value([&repo_authored]).expect("serialize repo definitions"),
    );

    let config = explicit_runtime("session-type-definition-repo")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![device_authored.clone()],
            }];
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:authoring".to_string(),
                label: "repo:authoring".to_string(),
                root: repo_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist authored session type sources");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let packages = empty_registry();
    let api = HubClientApi::local_operator("session-type-definition-repo-client");

    // A bare id selects the effective winner, matching ShowSessionType.
    let effective = read_definition(
        &api,
        &mut runtime,
        &packages,
        "definition-bare-id",
        "authored-shared",
    );
    assert_eq!(effective.definition, repo_authored);
    assert_eq!(
        effective.source,
        SessionTypeMutationSource::Repo {
            target_id: "repo:authoring".to_string()
        }
    );
    assert_eq!(effective.session_type_id, "repo:authoring/authored-shared");

    // A qualified id still reaches the overridden source's authored definition.
    let overridden = read_definition(
        &api,
        &mut runtime,
        &packages,
        "definition-qualified-id",
        "device/authored-shared",
    );
    assert_eq!(overridden.definition, device_authored);
    assert_eq!(overridden.source, SessionTypeMutationSource::Device);
    assert_eq!(overridden.session_type_id, "device/authored-shared");

    // Repo round trip through the atomic file-write path.
    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::UpdateSessionType {
            request_id: request_id("round-trip-repo-definition"),
            source: effective.source.clone(),
            definition: effective.definition.clone(),
        },
    )
    .expect("submit the repo authoring read back through Update");
    let written = fs::read_to_string(repo_root.join(".botster/session-types.json"))
        .expect("read Hub-written repo session types");
    let written: serde_json::Value =
        serde_json::from_str(&written).expect("repo session types parse");
    let stored: Vec<PackageSessionType> =
        serde_json::from_value(written["session_types"].clone()).expect("repo definitions decode");
    assert_eq!(stored, vec![repo_authored.clone()]);

    let round_tripped = read_definition(
        &api,
        &mut runtime,
        &packages,
        "definition-after-repo-round-trip",
        "authored-shared",
    );
    assert_eq!(round_tripped.definition, repo_authored);
}

#[test]
fn session_type_definition_rejects_ambiguous_bare_ids() {
    let first_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-definition-ambiguous-first",
    );
    let second_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-definition-ambiguous-second",
    );
    let _ = fs::remove_dir_all(&first_root);
    let _ = fs::remove_dir_all(&second_root);
    write_repo_session_types(
        &first_root,
        serde_json::to_value([authored_session_type("authored-ambiguous")])
            .expect("serialize first repo definitions"),
    );
    write_repo_session_types(
        &second_root,
        serde_json::to_value([authored_session_type("authored-ambiguous")])
            .expect("serialize second repo definitions"),
    );

    let config = explicit_runtime("session-type-definition-ambiguous")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets = vec![
                SpawnTarget {
                    target_id: "repo:first".to_string(),
                    label: "repo:first".to_string(),
                    root: first_root.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
                SpawnTarget {
                    target_id: "repo:second".to_string(),
                    label: "repo:second".to_string(),
                    root: second_root.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
            ];
        })
        .expect("persist ambiguous repo targets");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let packages = empty_registry();
    let api = HubClientApi::local_operator("session-type-definition-ambiguous-client");

    let ambiguous = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ShowSessionTypeDefinition {
                request_id: request_id("definition-ambiguous"),
                session_type_id: "authored-ambiguous".to_string(),
            },
        )
        .expect_err("ambiguous bare ids stay ambiguous for the authoring read");
    assert!(matches!(
        ambiguous,
        HubClientError::SessionType {
            kind: "ambiguous_session_type",
            ..
        }
    ));

    let qualified = read_definition(
        &api,
        &mut runtime,
        &packages,
        "definition-ambiguous-qualified",
        "repo:second/authored-ambiguous",
    );
    assert_eq!(
        qualified.source,
        SessionTypeMutationSource::Repo {
            target_id: "repo:second".to_string()
        }
    );
}

#[test]
fn session_type_sources_apply_device_over_package_when_repo_disabled() {
    let package_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-device-package",
    );
    let device_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-device-root",
    );
    let repo_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-disabled-repo",
    );
    let _ = fs::remove_dir_all(&package_root);
    let _ = fs::remove_dir_all(&device_root);
    let _ = fs::remove_dir_all(&repo_root);
    write_session_type_package(&package_root);
    write_executable_script(
        &device_root,
        "bin/device.sh",
        "#!/bin/sh\nprintf 'device:%s\\n' \"$BOTSTER_MODE\"\n",
    );
    write_repo_session_types(
        &repo_root,
        serde_json::json!([{
            "id": "init",
            "command": "bin/repo.sh",
            "environment": { "BOTSTER_MODE": "repo" }
        }]),
    );

    let mut packages = PackageRegistry::new(Vec::<Capability>::new().into_iter().collect());
    packages
        .install_local_path(&package_root, "install package")
        .expect("install package");
    packages
        .enable("session-type.plugin", "enable package")
        .expect("enable package");

    let config = explicit_runtime("session-type-device-over-package")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![session_type("bin/device.sh", "device")],
            }];
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:disabled".to_string(),
                label: "repo:disabled".to_string(),
                root: repo_root.clone(),
                enabled: false,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist sources");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let api = HubClientApi::local_operator("session-type-device-over-package-client");
    let resolved = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-device-over-package"),
                session_type_id: "init".to_string(),
                session_type_request: botster_hub::SessionTypeRequest::default(),
            },
        )
        .expect("resolve device template");
    let HubClientResponseBody::ResolvedSessionType(resolved) = resolved.body else {
        panic!("resolved session type expected");
    };

    assert_eq!(resolved.session_type.source, "device");
    assert_eq!(resolved.session_type.target_id, "device:local");
    assert_eq!(
        resolved.executable,
        device_root.join("bin/device.sh").display().to_string()
    );
}

#[test]
fn session_type_sources_reject_duplicate_ids_within_device_source() {
    let device_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-duplicate-device",
    );
    let _ = fs::remove_dir_all(&device_root);
    let config = explicit_runtime("session-type-duplicate-device")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![
                    session_type("bin/first.sh", "first"),
                    session_type("bin/second.sh", "second"),
                ],
            }];
        })
        .expect("persist duplicate device source");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let api = HubClientApi::local_operator("session-type-duplicate-device-client");
    let error = api
        .handle_request(
            &mut runtime,
            &empty_registry(),
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-duplicate-device"),
            },
        )
        .expect_err("duplicate device ids are rejected");

    assert!(matches!(
        error,
        HubClientError::SessionType {
            kind: "invalid_device_session_types",
            ..
        }
    ));
}

#[test]
fn session_type_sources_reject_duplicate_ids_within_repo_source() {
    let repo_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-duplicate-repo-root",
    );
    let _ = fs::remove_dir_all(&repo_root);
    write_repo_session_types(
        &repo_root,
        serde_json::json!([
            { "id": "init", "command": "bin/first.sh" },
            { "id": "init", "command": "bin/second.sh" }
        ]),
    );
    let config = explicit_runtime("session-type-duplicate-repo")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets = vec![SpawnTarget {
                target_id: "repo:duplicate".to_string(),
                label: "repo:duplicate".to_string(),
                root: repo_root.clone(),
                enabled: true,
                kind: "directory".to_string(),
                base_ref: None,
                metadata: BTreeMap::new(),
            }];
        })
        .expect("persist duplicate repo target");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let api = HubClientApi::local_operator("session-type-duplicate-repo-client");
    let error = api
        .handle_request(
            &mut runtime,
            &empty_registry(),
            HubClientRequest::ListSessionTypes {
                request_id: request_id("list-duplicate-repo"),
            },
        )
        .expect_err("duplicate repo ids are rejected");

    assert!(matches!(
        error,
        HubClientError::SessionType {
            kind: "invalid_repo_session_types",
            ..
        }
    ));
}

#[test]
fn session_type_sources_reject_ambiguous_same_rank_repo_ids() {
    let first_repo =
        std::path::PathBuf::from("target/botster-hub-test-data/client-api-session-type-first-repo");
    let second_repo = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-second-repo",
    );
    let _ = fs::remove_dir_all(&first_repo);
    let _ = fs::remove_dir_all(&second_repo);
    write_repo_session_types(
        &first_repo,
        serde_json::json!([{
            "id": "init",
            "label": "First repo agent",
            "role": "botster.agent",
            "interaction": "interactive",
            "traits": ["test"],
            "lifecycle": "task",
            "command": "bin/first.sh"
        }]),
    );
    write_repo_session_types(
        &second_repo,
        serde_json::json!([{
            "id": "init",
            "label": "Second repo agent",
            "role": "botster.agent",
            "interaction": "interactive",
            "traits": ["test"],
            "lifecycle": "task",
            "command": "bin/second.sh"
        }]),
    );
    let config = explicit_runtime("session-type-ambiguous-repos")
        .config()
        .clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.spawn_targets = vec![
                SpawnTarget {
                    target_id: "repo:first".to_string(),
                    label: "repo:first".to_string(),
                    root: first_repo.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
                SpawnTarget {
                    target_id: "repo:second".to_string(),
                    label: "repo:second".to_string(),
                    root: second_repo.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
            ];
        })
        .expect("persist ambiguous repo targets");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let api = HubClientApi::local_operator("session-type-ambiguous-repos-client");
    let error = api
        .handle_request(
            &mut runtime,
            &empty_registry(),
            HubClientRequest::ShowSessionType {
                request_id: request_id("show-ambiguous-repo-template"),
                session_type_id: "init".to_string(),
            },
        )
        .expect_err("same-rank repo ids are ambiguous");

    assert!(matches!(
        error,
        HubClientError::SessionType {
            kind: "ambiguous_session_type",
            ..
        }
    ));
}

#[test]
fn device_global_session_types_eligible_at_admitted_spawn_point() {
    let device_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-device-global-eligible-device",
    );
    let target_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-device-global-eligible-target",
    );
    let other_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-device-global-eligible-other",
    );
    let _ = fs::remove_dir_all(&device_root);
    let _ = fs::remove_dir_all(&target_root);
    let _ = fs::remove_dir_all(&other_root);
    fs::create_dir_all(&target_root).expect("create target root");
    fs::create_dir_all(target_root.join("nested")).expect("create relative cwd dir");
    fs::create_dir_all(&other_root).expect("create other root");
    write_executable_script(
        &device_root,
        "bin/device.sh",
        "#!/bin/sh\nprintf 'device:%s\\n' \"$BOTSTER_MODE\"\n",
    );

    let mut device_global = session_type("bin/device.sh", "device");
    device_global.id = "alpha".to_string();
    device_global.label = "Device Alpha".to_string();
    let mut device_relative = session_type("bin/device.sh", "relative");
    device_relative.id = "relative".to_string();
    device_relative.label = "Device Relative".to_string();
    device_relative.working_directory = PackageSessionTypeWorkingDirectory::Relative {
        path: "nested".to_string(),
    };
    let mut device_zebra = session_type("bin/device.sh", "zebra");
    device_zebra.id = "zebra".to_string();
    device_zebra.label = "Device Zebra".to_string();

    // Repo on T2 only shares bare id "alpha" with device — must not hide device at T.
    write_repo_session_types(
        &other_root,
        serde_json::json!([{
            "id": "alpha",
            "label": "Repo Alpha On Other",
            "role": "botster.agent",
            "interaction": "interactive",
            "traits": ["test"],
            "lifecycle": "task",
            "command": "bin/repo.sh",
            "environment": { "BOTSTER_MODE": "repo" },
            "allowed_environment_overrides": ["BOTSTER_MODE"],
            "context": ["prompt"]
        }]),
    );
    write_executable_script(&other_root, "bin/repo.sh", "#!/bin/sh\nprintf 'repo\\n'\n");

    // Repo on T wins bare id "zebra" over device for list/spawn at T.
    write_repo_session_types(
        &target_root,
        serde_json::json!([{
            "id": "zebra",
            "label": "Repo Zebra",
            "role": "botster.agent",
            "interaction": "interactive",
            "traits": ["test"],
            "lifecycle": "task",
            "command": "bin/repo.sh",
            "environment": { "BOTSTER_MODE": "repo" },
            "allowed_environment_overrides": ["BOTSTER_MODE"],
            "context": ["prompt"]
        }]),
    );
    write_executable_script(
        &target_root,
        "bin/repo.sh",
        "#!/bin/sh\nprintf 'repo-t\\n'\n",
    );

    let config = explicit_runtime("device-global-eligible").config().clone();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store
        .update(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![device_global, device_relative, device_zebra],
            }];
            state.spawn_targets = vec![
                SpawnTarget {
                    target_id: "tgt_hub".to_string(),
                    label: "Hub".to_string(),
                    root: target_root.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
                SpawnTarget {
                    target_id: "tgt_other".to_string(),
                    label: "Other".to_string(),
                    root: other_root.clone(),
                    enabled: true,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
                SpawnTarget {
                    target_id: "tgt_disabled".to_string(),
                    label: "Disabled".to_string(),
                    root: target_root.clone(),
                    enabled: false,
                    kind: "directory".to_string(),
                    base_ref: None,
                    metadata: BTreeMap::new(),
                },
            ];
        })
        .expect("persist device global sources");

    let mut runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime");
    let api = HubClientApi::local_operator("device-global-eligible-client");
    let packages = empty_registry();

    // Management catalog keeps the global effective path and storage provenance.
    // Non-colliding device relative stays as device with device:local provenance.
    let catalog = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypes {
                request_id: request_id("catalog-device-global"),
            },
        )
        .expect("management catalog");
    let HubClientResponseBody::SessionTypes(catalog) = catalog.body else {
        panic!("session types response expected");
    };
    let catalog_device_relative = catalog
        .iter()
        .find(|row| row.session_type_id == "device/relative")
        .expect("device relative remains in management catalog");
    assert_eq!(catalog_device_relative.target_id, "device:local");
    assert_eq!(catalog_device_relative.source, "device");
    // Bare alpha collides globally with repo-on-other: catalog winner is repo (unchanged).
    let catalog_alpha = catalog
        .iter()
        .find(|row| row.id == "alpha")
        .expect("alpha row in management catalog");
    assert_eq!(catalog_alpha.source, "repo");
    assert_eq!(catalog_alpha.session_type_id, "tgt_other/alpha");

    // List-for-T includes device Global with list-context target_id = T.
    let listed = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypesForTarget {
                request_id: request_id("list-for-hub"),
                target_id: "tgt_hub".to_string(),
            },
        )
        .expect("list for admitted hub target");
    let HubClientResponseBody::SessionTypes(listed) = listed.body else {
        panic!("session types response expected");
    };
    let ids = listed
        .iter()
        .map(|row| row.session_type_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec!["device/alpha", "device/relative", "tgt_hub/zebra"],
        "device globals eligible; repo wins zebra; stable lexical order"
    );
    for row in &listed {
        assert_eq!(row.target_id, "tgt_hub");
        assert!(row.available);
    }
    let zebra = listed
        .iter()
        .find(|row| row.session_type_id == "tgt_hub/zebra")
        .expect("repo zebra");
    assert_eq!(zebra.source, "repo");
    assert!(
        zebra
            .overridden_sources
            .iter()
            .any(|source| source.kind == "device"),
        "repo overrides device on T"
    );

    // Cross-target collision: other target's repo alpha must not hide device alpha on hub.
    let other_list = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypesForTarget {
                request_id: request_id("list-for-other"),
                target_id: "tgt_other".to_string(),
            },
        )
        .expect("list for other target");
    let HubClientResponseBody::SessionTypes(other_list) = other_list.body else {
        panic!("session types response expected");
    };
    let other_ids = other_list
        .iter()
        .map(|row| row.session_type_id.as_str())
        .collect::<Vec<_>>();
    assert!(other_ids.contains(&"tgt_other/alpha"));
    assert!(
        other_ids.contains(&"device/relative"),
        "device relative remains multi-target"
    );
    assert!(
        !other_ids.contains(&"tgt_hub/zebra"),
        "hub-only repo type must not appear on other"
    );
    // On other, bare alpha is repo winner.
    let other_alpha = other_list
        .iter()
        .find(|row| row.id == "alpha")
        .expect("alpha on other");
    assert_eq!(other_alpha.source, "repo");
    assert_eq!(other_alpha.session_type_id, "tgt_other/alpha");

    // List/spawn parity for every listed hub row.
    for row in &listed {
        let resolved = api
            .handle_request(
                &mut runtime,
                &packages,
                HubClientRequest::ResolveSessionType {
                    request_id: request_id(&format!("resolve-{}", row.session_type_id)),
                    session_type_id: row.session_type_id.clone(),
                    session_type_request: botster_hub::SessionTypeRequest {
                        target_id: Some("tgt_hub".to_string()),
                        ..botster_hub::SessionTypeRequest::default()
                    },
                },
            )
            .unwrap_or_else(|error| panic!("resolve {} at hub: {error:?}", row.session_type_id));
        let HubClientResponseBody::ResolvedSessionType(resolved) = resolved.body else {
            panic!("resolved session type expected");
        };
        assert_eq!(resolved.session_type.session_type_id, row.session_type_id);
        assert_eq!(resolved.session_type.target_id, "tgt_hub");
    }

    // Precedence loser is not listed and must not materialize at T.
    assert!(
        !listed
            .iter()
            .any(|row| row.session_type_id == "device/zebra"),
        "device/zebra is overridden by repo at hub and must not appear in list"
    );
    let loser_rejected = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-hidden-device-zebra"),
                session_type_id: "device/zebra".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    target_id: Some("tgt_hub".to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("qualified precedence loser must not spawn at T");
    assert!(
        matches!(
            loser_rejected,
            HubClientError::SessionType {
                kind: "session_type_not_eligible" | "unknown_session_type",
                ..
            }
        ),
        "expected not-eligible/unknown for hidden loser, got {loser_rejected:?}"
    );

    // Relative device cwd binds under T root, not device root.
    let relative = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-relative-at-hub"),
                session_type_id: "device/relative".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    target_id: Some("tgt_hub".to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect("resolve relative device at hub");
    let HubClientResponseBody::ResolvedSessionType(relative) = relative.body else {
        panic!("resolved session type expected");
    };
    assert_eq!(
        relative.working_directory,
        target_root.join("nested").display().to_string()
    );
    assert_eq!(
        relative.executable,
        device_root.join("bin/device.sh").display().to_string(),
        "command still under device source root"
    );

    // Explicit cwd outside T is rejected.
    let cwd_rejected = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ResolveSessionType {
                request_id: request_id("resolve-cwd-escape"),
                session_type_id: "device/alpha".to_string(),
                session_type_request: botster_hub::SessionTypeRequest {
                    target_id: Some("tgt_hub".to_string()),
                    cwd: Some(device_root.display().to_string()),
                    ..botster_hub::SessionTypeRequest::default()
                },
            },
        )
        .expect_err("cwd outside admitted T rejected");
    assert!(matches!(
        cwd_rejected,
        HubClientError::SessionType {
            kind: "cwd_not_admitted",
            ..
        }
    ));

    // Disabled / missing targets typed-reject (never empty-list-as-no-types).
    let disabled = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypesForTarget {
                request_id: request_id("list-disabled"),
                target_id: "tgt_disabled".to_string(),
            },
        )
        .expect_err("disabled target rejected");
    assert!(matches!(
        disabled,
        HubClientError::SessionType {
            kind: "target_not_admitted",
            ..
        }
    ));
    let missing = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListSessionTypesForTarget {
                request_id: request_id("list-missing"),
                target_id: "tgt_missing".to_string(),
            },
        )
        .expect_err("missing target rejected");
    assert!(matches!(
        missing,
        HubClientError::SessionType {
            kind: "target_not_found",
            ..
        }
    ));

    // Device without repo collision on a clean target still lists device zebra.
    // (hub has repo zebra; other has only device relative + device zebra + repo alpha)
    assert!(
        other_list
            .iter()
            .any(|row| row.session_type_id == "device/zebra"),
        "device zebra remains available where repo does not override"
    );
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
fn package_navigation_uses_explicit_manifest_entries_and_route_diagnostics() {
    let api = HubClientApi::local_operator("package-navigation-explicit-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    let mut manifest = plugin_manifest("navigation.plugin", vec![surfaces]);
    manifest.surfaces = vec![app_surface("workbench", "Workbench")];
    manifest.navigation = vec![PackageNavigationEntry {
        id: "primary".to_string(),
        label: "Primary Workbench".to_string(),
        icon: Some("workflow".to_string()),
        description: Some("Open the workbench".to_string()),
        target: PackageNavigationTarget::Surface {
            surface_id: "workbench".to_string(),
        },
    }];
    packages
        .install(manifest, provenance(), "install navigation package")
        .expect("install navigation package");
    let mut runtime = explicit_runtime("package-navigation-explicit");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackageNavigation {
                request_id: request_id("list-package-navigation-explicit"),
            },
        )
        .expect("list package navigation");
    let HubClientResponseBody::PackageNavigation(rows) = response.body else {
        panic!("package navigation response expected");
    };
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.package_name, "navigation.plugin");
    assert_eq!(row.item_id, "primary");
    assert_eq!(row.label, "Primary Workbench");
    assert_eq!(row.icon.as_deref(), Some("workflow"));
    assert_eq!(
        row.target,
        botster_hub::HubClientPackageNavigationTarget::Surface {
            surface_id: "workbench".to_string()
        }
    );
}

#[test]
fn package_navigation_derives_default_app_surface_entries_without_order_authority() {
    let api = HubClientApi::local_operator("package-navigation-default-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    let mut manifest = plugin_manifest("default-nav.plugin", vec![surfaces]);
    manifest.surfaces = vec![app_surface("home", "Home")];
    packages
        .install(manifest, provenance(), "install default nav package")
        .expect("install default nav package");
    packages
        .enable("default-nav.plugin", "enable default nav package")
        .expect("enable default nav package");
    let mut runtime = explicit_runtime("package-navigation-default");

    let response = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ListPackageNavigation {
                request_id: request_id("list-package-navigation-default"),
            },
        )
        .expect("list package navigation");
    let HubClientResponseBody::PackageNavigation(rows) = response.body else {
        panic!("package navigation response expected");
    };
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.item_id, "home");
    assert_eq!(row.label, "Home");
    let serialized = format!("{row:?}");
    assert!(!serialized.contains("order"));
    assert!(!serialized.contains("priority"));
}

#[test]
fn plugin_surface_admission_is_shared_by_the_in_process_client_api() {
    let api = HubClientApi::local_operator("plugin-surface-admission-client");
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    let mut manifest = plugin_manifest("surface.plugin", vec![surfaces]);
    let mut action_only = app_surface("action-only", "Action only");
    action_only.supports = vec![PackageSurfaceOperation::Action];
    manifest.surfaces = vec![action_only];
    packages
        .install(manifest, provenance(), "install surface package")
        .expect("install surface package");
    let mut runtime = explicit_runtime("plugin-surface-admission");

    let undeclared = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::PluginSurfaceRender {
                request_id: request_id("render-undeclared-surface"),
                package_name: "surface.plugin".to_string(),
                surface_id: "missing".to_string(),
                payload: serde_json::json!({}),
            },
        )
        .expect_err("undeclared surfaces must be rejected before runtime dispatch");
    assert!(matches!(
        undeclared,
        HubClientError::Plugin { ref code, .. } if code == "undeclared_plugin_surface"
    ));

    let unsupported = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::PluginSurfaceRender {
                request_id: request_id("render-unsupported-surface"),
                package_name: "surface.plugin".to_string(),
                surface_id: "action-only".to_string(),
                payload: serde_json::json!({}),
            },
        )
        .expect_err("unsupported surface operations must be rejected before runtime dispatch");
    assert!(matches!(
        unsupported,
        HubClientError::Plugin { ref code, .. } if code == "unsupported_plugin_surface_operation"
    ));
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

fn read_screen_until(
    api: &HubClientApi,
    runtime: &mut HubRuntime,
    packages: &PackageRegistry,
    session_id: &SessionId,
    needle: &str,
    logical_clock: &mut u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let response = api
            .handle_request(
                runtime,
                packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("read-screen-until"),
                    session_id: session_id.clone(),
                    now_seconds: *logical_clock,
                },
            )
            .expect("read screen through client api");
        *logical_clock += 1;
        let HubClientResponseBody::ReadScreen(screen) = response.body else {
            panic!("read screen should return typed response");
        };
        if screen.text.contains(needle) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {needle:?} in ReadScreen");
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

fn history_payload(event: &HubClientEvent) -> Option<(&SubscriptionId, &[u8])> {
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
fn late_attach_receives_opaque_history_before_later_live_output() {
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
    read_screen_until(
        &first_api,
        &mut runtime,
        &packages,
        &session_id,
        "before-late",
        &mut logical_clock,
    );

    let late_attach = late_api
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
    let HubClientResponseBody::Events(mut events) = late_attach.body else {
        panic!("late attach should return initial events");
    };
    logical_clock += 1;

    let readback = late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadScreen {
                request_id: request_id("late-history-readback-before-drain"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("read screen between late attach and first drain");
    logical_clock += 1;
    let HubClientResponseBody::ReadScreen(screen) = readback.body else {
        panic!("read screen should return typed response");
    };
    assert_eq!(
        screen.text.matches("before-late").count(),
        1,
        "readback before late drain should contain prior output exactly once, got {:?}",
        screen.text
    );

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

    events.extend(drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-after-late",
        &mut logical_clock,
    ));
    let attaching_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::AttachState {
                    subscription_id,
                    state: TerminalAttachState::Attaching,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late subscription should enter attaching state");
    let history_events = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            history_payload(event).and_then(|(subscription_id, data)| {
                (subscription_id == &late_subscription).then_some((index, data))
            })
        })
        .collect::<Vec<_>>();
    let history_data = history_events
        .iter()
        .flat_map(|(_, data)| data.iter().copied())
        .collect::<Vec<_>>();
    assert!(
        !history_data.is_empty(),
        "late subscription should receive opaque initial state, got {events:?}"
    );
    // Opacity is typed pass-through (Snapshot/Scrollback via history_payload), not
    // UTF-8 absence. ghostty-terminal-snapshot-v1 / GHOSTSNP embeds screen cell
    // bytes that may contain the same text as ReadScreen; hub must not re-emit
    // that prior live content as TerminalOutput, decode wire magic, or use
    // opaque bytes as the renderable surface. ReadScreen (asserted above)
    // remains the authority for visible text.
    // Falsifiable: late sub must not re-receive prior live TerminalOutput for
    // the before-late marker (history_payload-prefiltered Snapshot/Scrollback
    // checks are tautological and intentionally omitted).
    let initial_history_as_terminal_output = events.iter().any(|event| {
        matches!(
            event,
            HubClientEvent::TerminalOutput {
                subscription_id,
                data,
                ..
            } if subscription_id == &late_subscription
                && data.windows(b"before-late".len()).any(|window| window == b"before-late")
        )
    });
    assert!(
        !initial_history_as_terminal_output,
        "renderable prior output must not be re-emitted as TerminalOutput history for the late subscription, got {events:?}"
    );
    let history_index = history_events
        .first()
        .map(|(index, _)| *index)
        .unwrap_or_else(|| {
            panic!("late subscription should receive prior history, got {events:?}")
        });
    let last_history_index = history_events
        .last()
        .map(|(index, _)| *index)
        .expect("history event should have a last index");
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
    let attached_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::AttachState {
                    subscription_id,
                    state: TerminalAttachState::Attached,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late subscription should become attached after history");
    let first_terminal_output_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late subscription should receive terminal output");

    assert!(
        attaching_index < history_index
            && last_history_index < attached_index
            && attached_index < first_terminal_output_index
            && attached_index < live_index,
        "late subscription should observe attaching < history < attached < live, got {events:?}"
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

    let late_attach = late_api
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
    let HubClientResponseBody::Events(mut events) = late_attach.body else {
        panic!("late no-history attach should return initial events");
    };
    logical_clock += 1;

    let readback = late_api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadScreen {
                request_id: request_id("no-history-readback-before-input"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("read blank screen before sending live output");
    logical_clock += 1;
    let HubClientResponseBody::ReadScreen(screen) = readback.body else {
        panic!("read screen should return typed response");
    };
    assert!(
        screen.text.is_empty(),
        "idle session should have no prior renderable output, got {:?}",
        screen.text
    );

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

    events.extend(drain_events_until(
        &late_api,
        &mut runtime,
        &packages,
        &session_id,
        &late_subscription,
        b"after:live-only",
        &mut logical_clock,
    ));

    assert!(
        !events.iter().any(|event| {
            matches!(
                event,
                HubClientEvent::Scrollback {
                    subscription_id,
                    ..
                } if subscription_id == &late_subscription
            )
        }),
        "idle subscription should not receive fabricated scrollback, got {events:?}"
    );
    let attaching_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::AttachState {
                    subscription_id,
                    state: TerminalAttachState::Attaching,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late no-history subscription should enter attaching state");
    let attached_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::AttachState {
                    subscription_id,
                    state: TerminalAttachState::Attached,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late no-history subscription should become attached");
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
                    && data.windows(b"after:live-only".len())
                        .any(|window| window == b"after:live-only")
            )
        })
        .expect("late no-history subscription should receive live output");
    let last_initial_state_index = events.iter().rposition(|event| {
        matches!(
            event,
            HubClientEvent::Snapshot {
                subscription_id,
                ..
            } | HubClientEvent::Scrollback {
                subscription_id,
                ..
            } if subscription_id == &late_subscription
        )
    });
    let first_terminal_output_index = events
        .iter()
        .position(|event| {
            matches!(
                event,
                HubClientEvent::TerminalOutput {
                    subscription_id,
                    ..
                } if subscription_id == &late_subscription
            )
        })
        .expect("late no-history subscription should receive terminal output");
    assert!(
        attaching_index < attached_index
            && last_initial_state_index.is_none_or(|index| index < attached_index)
            && attached_index < first_terminal_output_index
            && attached_index < live_index,
        "idle subscription should observe attaching < optional initial state < attached < live, got {events:?}"
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

    read_screen_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        "ready",
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

    read_screen_until(
        &api,
        &mut runtime,
        &packages,
        &session_id,
        "ready",
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
fn read_screen_and_snapshot_return_typed_daemon_readback_responses() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("daemon-readback-ops");
    let session_id = SessionId("daemon-readback-session".to_string());
    let mut logical_clock = 1;

    api.handle_request(
        &mut runtime,
        &packages,
        HubClientRequest::Spawn {
            request_id: request_id("readback-spawn"),
            session_id: session_id.clone(),
            command: "printf 'screen-ready\\n'; sleep 5".to_string(),
            now_seconds: logical_clock,
        },
    )
    .expect("spawn readback session");
    logical_clock += 1;

    let deadline = Instant::now() + Duration::from_secs(5);
    let screen = loop {
        let read_screen = api
            .handle_request(
                &mut runtime,
                &packages,
                HubClientRequest::ReadScreen {
                    request_id: request_id("read-screen"),
                    session_id: session_id.clone(),
                    now_seconds: logical_clock,
                },
            )
            .expect("daemon-backed read_screen should return typed response");
        logical_clock += 1;
        let HubClientResponseBody::ReadScreen(screen) = read_screen.body else {
            panic!("read_screen should return readback body");
        };
        if screen.text.contains("screen-ready") {
            break screen;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for screen-ready"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(screen.session_id, session_id);

    let capture_snapshot = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::CaptureSnapshot {
                request_id: request_id("capture-snapshot"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("daemon-backed capture_snapshot should return typed response");
    let HubClientResponseBody::CaptureSnapshot(snapshot) = capture_snapshot.body else {
        panic!("capture_snapshot should return snapshot body");
    };
    assert_eq!(snapshot.session_id, session_id);
    assert_eq!(snapshot.rows, 24);
    assert_eq!(snapshot.cols, 80);
    assert_eq!(
        snapshot.payload_format.as_deref(),
        Some("ghostty-terminal-snapshot-v1")
    );
    assert!(snapshot.payload_bytes > 0);

    logical_clock += 1;
    let shutdown = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Shutdown {
                request_id: request_id("readback-shutdown"),
                session_id: session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("shutdown readback session through client api");
    let HubClientResponseBody::Events(events) = shutdown.body else {
        panic!("shutdown should return events");
    };
    assert!(events.is_empty());
}

#[test]
fn read_mode_flags_returns_exact_authoritative_values_and_session_attribution() {
    let api = HubClientApi::local_operator("mode-flags-client-api-test");
    let packages = empty_registry();
    let mut runtime = explicit_runtime("mode-flags-readback");
    let off_session_id = SessionId("mode-flags-off-session".to_string());
    let on_session_id = SessionId("mode-flags-on-session".to_string());
    let mut logical_clock = 1;

    for (session_id, command) in [
        (off_session_id.clone(), "sleep 5"),
        (
            on_session_id.clone(),
            "printf '\\033[?1000h\\033[?1006h'; sleep 5",
        ),
    ] {
        api.handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::Spawn {
                request_id: request_id(&format!("spawn-{}", session_id.0)),
                session_id,
                command: command.to_string(),
                now_seconds: logical_clock,
            },
        )
        .expect("spawn mode-flags session");
        logical_clock += 1;
    }

    let off = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadModeFlags {
                request_id: request_id("read-mode-flags-off"),
                session_id: off_session_id.clone(),
                now_seconds: logical_clock,
            },
        )
        .expect("read authoritative mouse-off flags");
    logical_clock += 1;
    let HubClientResponseBody::ModeFlags(off) = off.body else {
        panic!("read_mode_flags should return a typed mode body");
    };
    assert_eq!(off.session_id, off_session_id);
    assert_eq!(off.mouse_mode, 0);
    assert_ne!(off.mode_generation, 0);
    // Full ModeFlags projection is present (not mouse-only).
    let _ = (
        off.kitty_enabled,
        off.cursor_visible,
        off.bracketed_paste,
        off.alt_screen,
        off.focus_reporting,
        off.application_cursor,
        off.mode_revision,
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let on = loop {
        let response = api
            .handle_request(
                &mut runtime,
                &packages,
                HubClientRequest::ReadModeFlags {
                    request_id: request_id("read-mode-flags-on"),
                    session_id: on_session_id.clone(),
                    now_seconds: logical_clock,
                },
            )
            .expect("read authoritative mouse-on flags");
        logical_clock += 1;
        let HubClientResponseBody::ModeFlags(mode_flags) = response.body else {
            panic!("read_mode_flags should return a typed mode body");
        };
        if mode_flags.mouse_mode == 9 {
            break mode_flags;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for exact combined mouse mode, last value {}",
            mode_flags.mouse_mode
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(on.session_id, on_session_id);
    assert_eq!(on.mouse_mode, 9);
    assert_ne!(on.mode_generation, 0);
    assert!(on.mode_revision >= 1);

    let missing = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::ReadModeFlags {
                request_id: request_id("read-mode-flags-missing"),
                session_id: SessionId("missing-mode-flags-session".to_string()),
                now_seconds: logical_clock,
            },
        )
        .expect_err("unknown session must not default to mouse-off");
    assert_eq!(
        missing,
        HubClientError::Runtime {
            request_id: request_id("read-mode-flags-missing"),
            operation: HubClientOperation::ReadModeFlags,
            kind: botster_hub::HubClientRuntimeErrorKind::UnknownSession,
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
    assert_eq!(surface.kind, PackageSurfaceKind::App);
    assert_eq!(surface.title, "Workflow Home");
    assert_eq!(surface.description.as_deref(), Some("Workflow dashboard"));
    assert_eq!(surface.icon.as_deref(), Some("workflow"));
    assert_eq!(surface.order, Some(10));
    assert_eq!(surface.category.as_deref(), Some("workflows"));
    assert_eq!(
        surface.supports,
        [
            PackageSurfaceOperation::Render,
            PackageSurfaceOperation::Action
        ]
    );
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
    assert_eq!(records.lifecycle.len(), 1);
    assert_eq!(records.lifecycle[0].package_name, "workflow.plugin");
    assert_eq!(records.lifecycle[0].state, HubClientPackageState::Enabled);
    assert!(!records.lifecycle[0].loaded);
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
