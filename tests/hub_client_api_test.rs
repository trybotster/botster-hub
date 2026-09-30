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
use botster_core_daemon::{
    GuardedWriteDecision, GuardedWriteDeliveryState, LifecycleBaselineBudget,
    LifecycleBaselineStop, ObserveLifecycleBudget, ObserveLifecycleCursor, ObserveLifecycleStop,
    ReadinessEvidence,
};
use botster_hub::session_types::{
    list_session_types, list_session_types_for_target, materialize_session_type, show_session_type,
    show_session_type_definition,
};
use botster_hub::{
    CoreEngineOptions, DataDirectoryOption, DeviceSessionTypeSource, FileHubStateStore,
    HostIdentityOptions, HubClientApi, HubClientError, HubClientOperation, HubClientPackage,
    HubClientPackageClassification, HubClientPackageState, HubClientRequest, HubClientResponseBody,
    HubPackageManifest, HubRuntime, HubStartupOptions, PackageProvenance, PackageRegistry,
    PackageSessionType, PackageSessionTypeExecution, PackageSessionTypeWorkingDirectory,
    RuntimeEnvironment, SessionDefaults, SessionTypeMutationSource, SpawnTarget, TransportBindings,
};
use botster_hub_client::{
    DaemonConnection, DaemonRequest, DaemonResponse, DaemonResponseKind,
    DaemonSessionTypeDefinition, DaemonSessionTypeEditableDefinition,
    DaemonSessionTypeMutationSource,
};
use botster_hub_test_support::{IsolatedHub, IsolatedHubBuilder};
use botster_terminal_protocol_client::TerminalInputCommand;
use botster_ui_contract::{
    PackageNavigationEntry, PackageNavigationTarget, PackageSurfaceDescriptor, PackageSurfaceKind,
    PackageSurfaceOperation,
};

mod support;
use botster_hub::test_internals::TestHubStateStoreExt;
use support::{
    bind_shared_terminal_adapter, candidate_hub_binary_path, candidate_session_worker_binary_path,
    inject_terminal_command,
};

fn isolated_hub(name: &str) -> IsolatedHub {
    IsolatedHubBuilder::new()
        .hub_bin(candidate_hub_binary_path())
        .session_worker_bin(candidate_session_worker_binary_path())
        .root("/tmp/bhca")
        .name(format!("client-api-{name}"))
        .start()
        .expect("start isolated hub")
}

fn daemon_definition(definition: &PackageSessionType) -> DaemonSessionTypeDefinition {
    serde_json::from_value(serde_json::to_value(definition).expect("serialize session type"))
        .expect("convert session type to daemon definition")
}

fn daemon_request(connection: &mut DaemonConnection, request: DaemonRequest) -> DaemonResponse {
    let response = connection.request(&request).expect("daemon request");
    assert_ne!(
        response.kind,
        DaemonResponseKind::OperatorError,
        "daemon request failed: {:?}",
        response.error
    );
    response
}

fn daemon_error_response(
    connection: &mut DaemonConnection,
    request: DaemonRequest,
) -> DaemonResponse {
    let response = connection.request(&request).expect("daemon request");
    assert_eq!(response.kind, DaemonResponseKind::OperatorError);
    response
}

fn daemon_read_definition(
    connection: &mut DaemonConnection,
    session_type_id: &str,
) -> DaemonSessionTypeEditableDefinition {
    daemon_request(
        connection,
        DaemonRequest::ShowSessionTypeDefinition {
            session_type_id: session_type_id.to_string(),
        },
    )
    .session_type_definition
    .expect("authoring response carries a definition")
}

fn explicit_runtime(name: &str) -> HubRuntime {
    let session_worker_path = candidate_session_worker_binary_path().to_path_buf();
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
        core_engine: CoreEngineOptions {
            session_worker_path: Some(session_worker_path),
            ..CoreEngineOptions::default()
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("explicit runtime config should build");

    HubRuntime::new(config).expect("hub runtime starts")
}

use botster_hub::test_internals::LocalClient;

const CORE_WAIT: Duration = Duration::from_secs(30);

fn attach_bound_subscription(
    runtime: &mut HubRuntime,
    api: &LocalClient,
    session_id: &SessionId,
    subscription_id: &SubscriptionId,
    now_seconds: u64,
) -> botster_core_test_support::terminal_adapter::SharedFakeTerminalAdapter {
    // Attach and bind run as one Core operation inside the shared helper.
    let _ = now_seconds;
    bind_shared_terminal_adapter(
        runtime,
        api.client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
    )
}

#[test]
fn session_type_device_crud_uses_unix_control_and_package_mutation_is_read_only() {
    let hub = isolated_hub("session-type-device-crud");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");
    daemon_request(
        &mut connection,
        DaemonRequest::CreateSpawnTarget {
            target_id: Some("repo:concurrent".to_string()),
            label: Some("Concurrently persisted target".to_string()),
            root: std::path::PathBuf::from("."),
            enabled: false,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    );
    let mut definition = session_type("bin/accessory.sh", "accessory");
    definition.id = "terminal-accessory".to_string();
    definition.label = "Terminal accessory".to_string();
    definition.role = "botster.accessory".to_string();
    definition.interaction = "interactive".to_string();
    definition.traits = vec!["terminal".to_string()];
    definition.lifecycle = "persistent".to_string();

    let created = daemon_request(
        &mut connection,
        DaemonRequest::CreateSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            definition: daemon_definition(&definition),
        },
    );
    assert_eq!(created.kind, DaemonResponseKind::SessionTypes);
    assert_eq!(created.session_types.len(), 1);
    assert!(created.session_types[0].editable);
    assert_eq!(created.session_types[0].role, "botster.accessory");
    let targets = daemon_request(&mut connection, DaemonRequest::ListSpawnTargets);
    assert!(
        targets
            .spawn_targets
            .iter()
            .any(|target| target.target_id == "repo:concurrent"),
        "session type CRUD must preserve unrelated durable writes"
    );

    definition.label = "Updated terminal accessory".to_string();
    let updated = daemon_request(
        &mut connection,
        DaemonRequest::UpdateSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            definition: daemon_definition(&definition),
        },
    );
    assert_eq!(updated.kind, DaemonResponseKind::SessionTypes);
    assert_eq!(updated.session_types[0].label, "Updated terminal accessory");

    let rejected = daemon_error_response(
        &mut connection,
        DaemonRequest::DeleteSessionType {
            source: DaemonSessionTypeMutationSource::Package {
                package_name: "read-only.plugin".to_string(),
            },
            session_type_id: "terminal-accessory".to_string(),
        },
    );
    assert_eq!(
        rejected.error.as_ref().map(|error| error.code.as_str()),
        Some("read_only_session_type_source")
    );

    let deleted = daemon_request(
        &mut connection,
        DaemonRequest::DeleteSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            session_type_id: "terminal-accessory".to_string(),
        },
    );
    assert_eq!(deleted.kind, DaemonResponseKind::SessionTypes);
    assert!(deleted.session_types.is_empty());
    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
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

#[test]
fn session_type_definition_round_trips_authored_path_and_environment() {
    let hub = isolated_hub("session-type-definition-round-trip");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");

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
        daemon_request(
            &mut connection,
            DaemonRequest::CreateSessionType {
                source: DaemonSessionTypeMutationSource::Device,
                definition: daemon_definition(&definition),
            },
        );
    }

    for authored in [populated, sparse] {
        let read = daemon_read_definition(&mut connection, &authored.id);

        // The read is lossless and carries the exact mutation source Update needs.
        assert_eq!(
            read.definition,
            daemon_definition(&authored),
            "authoring read must be lossless"
        );
        assert_eq!(read.source, DaemonSessionTypeMutationSource::Device);
        assert_eq!(read.session_type_id, format!("device/{}", authored.id));
        assert_eq!(
            read.definition.id, authored.id,
            "definition.id must be the bare id Update matches on, not the composite id"
        );

        // Change one field and submit every other authored field unchanged.
        let mut updated = read.definition.clone();
        updated.label = format!("Updated {}", authored.id);
        daemon_request(
            &mut connection,
            DaemonRequest::UpdateSessionType {
                source: read.source.clone(),
                definition: updated.clone(),
            },
        );
        let stored = daemon_read_definition(&mut connection, &authored.id).definition;
        assert_eq!(
            stored, updated,
            "read-modify-write must not lose the authored working-directory path or environment"
        );
        assert_eq!(
            stored.working_directory,
            botster_hub_client::DaemonSessionTypeWorkingDirectory::Relative {
                path: "nested/dir".to_string()
            }
        );
        assert!(!stored.environment.is_empty());
    }
    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
}

#[test]
fn sanitized_session_type_row_still_cannot_reconstruct_the_authored_definition() {
    let hub = isolated_hub("session-type-sanitized-row-is-lossy");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");
    let authored = authored_session_type("authored-lossy");
    daemon_request(
        &mut connection,
        DaemonRequest::CreateSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            definition: daemon_definition(&authored),
        },
    );

    // What a client could reconstruct before this seam existed: the row derives a
    // policy string and has no environment field at all, so both are destroyed.
    let shown = daemon_request(
        &mut connection,
        DaemonRequest::ShowSessionType {
            session_type_id: authored.id.clone(),
        },
    );
    assert_eq!(shown.session_types.len(), 1);
    let row = shown.session_types[0].clone();
    assert_eq!(row.working_directory_policy, "relative");
    let reconstructed_from_row = DaemonSessionTypeDefinition {
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
        working_directory: botster_hub_client::DaemonSessionTypeWorkingDirectory::default(),
        environment: BTreeMap::new(),
        allowed_environment_overrides: row.allowed_environment_overrides.clone(),
        context: row.context_keys.clone(),
        target_id: None,
    };
    assert_ne!(
        reconstructed_from_row,
        daemon_definition(&authored),
        "the sanitized row must remain insufficient to rebuild an authored definition"
    );
    assert_eq!(
        reconstructed_from_row.working_directory,
        botster_hub_client::DaemonSessionTypeWorkingDirectory::PackageRoot
    );
    assert!(reconstructed_from_row.environment.is_empty());

    // And the sanitized surfaces did not move: no authored environment value and no
    // authored path appears in the published row, in list, or in the entity payload.
    let listed = daemon_request(&mut connection, DaemonRequest::ListSessionTypes);
    assert_eq!(listed.session_types, vec![row.clone()]);

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
    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
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

    let runtime = explicit_runtime("session-type-definition-package-refusal");

    let refused =
        show_session_type_definition(&packages.packages(), &runtime.state(), &"init".to_string())
            .expect_err("package-owned definitions stay read-only");
    assert!(refused.kind == "read_only_session_type_source");

    let unknown = show_session_type_definition(
        &packages.packages(),
        &runtime.state(),
        &"missing".to_string(),
    )
    .expect_err("unknown ids stay typed");
    assert!(unknown.kind == "unknown_session_type");
}

#[test]
fn session_type_role_interaction_traits_and_lifecycle_are_orthogonal() {
    let hub = isolated_hub("session-type-orthogonal-semantics");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");
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
        daemon_request(
            &mut connection,
            DaemonRequest::CreateSessionType {
                source: DaemonSessionTypeMutationSource::Device,
                definition: daemon_definition(&definition),
            },
        );
    }

    let response = daemon_request(&mut connection, DaemonRequest::ListSessionTypes);
    let semantics = response
        .session_types
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
    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
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
fn session_entity_baseline_of_an_empty_hub_is_one_complete_page() {
    let runtime = explicit_runtime("session-entity-subscription");

    // The baseline the session entity family starts from: one bounded Core page.
    let page = runtime
        .lifecycle_baseline_page(
            None,
            None,
            LifecycleBaselineBudget {
                max_rows: 32,
                max_bytes: 64 * 1024,
                max_elapsed: Duration::from_millis(25),
            },
        )
        .wait(CORE_WAIT)
        .expect("core bridge")
        .expect("lifecycle baseline page");
    assert!(page.sessions.is_empty());
    assert_eq!(page.stop, LifecycleBaselineStop::Complete);
}

#[test]
fn session_entity_subscription_returns_a_bounded_page_not_a_complete_baseline() {
    let runtime = explicit_runtime("session-entity-paged-baseline");
    let api = LocalClient::new("session-entity-paged-client");

    for index in 0..33 {
        api.spawn(
            &runtime,
            &SessionId(format!("paged-session-{index:02}")),
            &"sleep 30".to_string(),
        );
    }

    assert_eq!(
        runtime
            .list_sessions()
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge")
            .expect("list spawned sessions")
            .len(),
        33
    );
    let mut resume = None;
    for now in 40..48 {
        let slice = runtime
            .observe_lifecycle_slice(
                now,
                resume.as_ref(),
                ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge")
            .expect("observe spawned sessions");
        if matches!(
            slice.stop,
            ObserveLifecycleStop::Complete | ObserveLifecycleStop::Resync { .. }
        ) {
            break;
        }
        resume = Some(ObserveLifecycleCursor {
            pass_id: slice.pass_id,
            last_visited: slice.last_visited,
        });
    }

    let budget = LifecycleBaselineBudget {
        max_rows: 32,
        max_bytes: 64 * 1024,
        max_elapsed: Duration::from_millis(25),
    };
    let first_page = runtime
        .lifecycle_baseline_page(None, None, budget)
        .wait(CORE_WAIT)
        .expect("core bridge")
        .expect("first lifecycle baseline page");
    assert!(
        first_page.stop != LifecycleBaselineStop::Complete || first_page.sessions.len() < 33,
        "local API must not present one page as the complete 33-row baseline"
    );
    assert!(first_page.sessions.len() <= 32);

    let mut snapshot = Some(first_page.snapshot_sequence.clone());
    let mut after = first_page.next.clone();
    let mut rows = first_page.sessions.clone();
    let mut complete = first_page.stop == LifecycleBaselineStop::Complete;
    for _ in 0..8 {
        if complete {
            break;
        }
        let page = runtime
            .lifecycle_baseline_page(snapshot.as_ref(), after.as_ref(), budget)
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge")
            .expect("continue baseline pages");
        rows.extend(page.sessions);
        complete = page.stop == LifecycleBaselineStop::Complete;
        snapshot = Some(page.snapshot_sequence);
        after = page.next;
    }
    assert!(complete, "paged local API must finish the baseline");
    assert_eq!(rows.len(), 33);

    for index in 0..33 {
        let _ = api.shutdown(&runtime, &SessionId(format!("paged-session-{index:02}")));
    }
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
fn session_types_resolve_and_reject_ownerless_spawn_and_unadmitted_reads() {
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

    let list = list_session_types(&packages.packages(), &runtime.state()).expect("list templates");
    let templates = list;
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].id, "init");

    let rejected_env = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest {
            environment: BTreeMap::from([("BOTSTER_UNDECLARED".to_string(), "no".to_string())]),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("undeclared env override rejected");
    assert!(rejected_env.kind == "environment_not_admitted");

    let rejected_target = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest {
            target_id: Some("package:other-template.plugin".to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("unadmitted target override rejected");
    assert!(rejected_target.kind == "target_not_admitted");

    let rejected_cwd = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest {
            cwd: Some("/tmp/outside-template-root".to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("unadmitted cwd override rejected");
    assert!(rejected_cwd.kind == "cwd_not_admitted");

    let resolved = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest {
            environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "override".to_string())]),
            context: botster_hub::SessionTypeContextInput {
                prompt: Some("hello from api".to_string()),
                ..botster_hub::SessionTypeContextInput::default()
            },
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect("resolve bare generic template id");
    let resolved = resolved;
    assert_eq!(resolved.session_type.id, "init");
    assert_eq!(
        resolved.environment.get("BOTSTER_MODE").map(String::as_str),
        Some("override")
    );
    assert!(resolved.environment.contains_key("BOTSTER_CONTEXT_ID"));

    let spawn_request_id = request_id("spawn-session-type");
    let ownerless_spawn = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::SpawnSessionType {
                request_id: spawn_request_id.clone(),
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
        .wait(&runtime)
        .expect_err("session type spawning requires the daemon control owner");
    assert!(matches!(
        ownerless_spawn,
        HubClientError::InvalidRequest {
            request_id,
            operation: HubClientOperation::SpawnSessionType,
            message,
        } if request_id == spawn_request_id
            && message == "session type spawning requires the daemon control owner"
    ));
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

    let runtime = explicit_runtime("session-type-show-ambiguous");

    let rejected = show_session_type(&packages.packages(), &runtime.state(), &"init".to_string())
        .map(|template| vec![template])
        .expect_err("ambiguous bare template id should be rejected");
    assert!(rejected.kind == "ambiguous_session_type");

    let shown = show_session_type(
        &packages.packages(),
        &runtime.state(),
        &"first-template.plugin/init".to_string(),
    )
    .map(|template| vec![template])
    .expect("full template id remains unambiguous");
    let templates = shown;
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");

    let list =
        list_session_types(&packages.packages(), &runtime.state()).expect("list merged templates");
    let templates = list;
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
        let shown = show_session_type(&packages.packages(), &runtime.state(), &session_type_id)
            .map(|template| vec![template])
            .expect("show repo override");
        assert_eq!(shown, vec![listed.clone()], "{suffix} id");

        let resolved = materialize_session_type(
            runtime.config(),
            &packages.packages(),
            &runtime.state(),
            &session_type_id,
            botster_hub::SessionTypeRequest {
                environment: BTreeMap::from([("BOTSTER_MODE".to_string(), "explicit".to_string())]),
                ..botster_hub::SessionTypeRequest::default()
            },
        )
        .map(|materialized| materialized.resolved)
        .expect("resolve repo override");
        let resolved = resolved;
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

    let rejected = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest {
            cwd: Some(device_root.display().to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("repo cwd outside target rejected");
    assert!(rejected.kind == "cwd_not_admitted");
}

#[test]
fn repo_session_type_mutations_use_unix_control_admission() {
    let repo_root = std::path::PathBuf::from(
        "target/botster-hub-test-data/client-api-session-type-repo-mutation",
    );
    let _ = fs::remove_dir_all(&repo_root);
    write_repo_session_types(
        &repo_root,
        serde_json::to_value([session_type("bin/repo.sh", "repo")])
            .expect("serialize repo definition"),
    );
    let repo_root = repo_root.canonicalize().expect("canonical repo root");
    let hub = isolated_hub("session-type-repo-mutation");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");

    daemon_request(
        &mut connection,
        DaemonRequest::CreateSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            definition: daemon_definition(&session_type("bin/device.sh", "device")),
        },
    );
    daemon_request(
        &mut connection,
        DaemonRequest::CreateSpawnTarget {
            target_id: Some("repo:main".to_string()),
            label: Some("repo:main".to_string()),
            root: repo_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    );

    let mut updated_repo = session_type("bin/repo.sh", "repo-updated");
    updated_repo.label = "Updated repo agent".to_string();
    daemon_request(
        &mut connection,
        DaemonRequest::UpdateSessionType {
            source: DaemonSessionTypeMutationSource::Repo {
                target_id: "repo:main".to_string(),
            },
            definition: daemon_definition(&updated_repo),
        },
    );
    assert!(
        fs::read_to_string(repo_root.join(".botster/session-types.json"))
            .expect("read Hub-written repo session types")
            .contains("Updated repo agent")
    );

    let deleted = daemon_request(
        &mut connection,
        DaemonRequest::DeleteSessionType {
            source: DaemonSessionTypeMutationSource::Repo {
                target_id: "repo:main".to_string(),
            },
            session_type_id: "init".to_string(),
        },
    );
    assert_eq!(deleted.session_types.len(), 1);
    assert_eq!(deleted.session_types[0].source, "device");

    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
    fs::remove_dir_all(&repo_root).expect("remove repo fixture");
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

    let repo_root = repo_root.canonicalize().expect("canonical repo root");
    let hub = isolated_hub("session-type-definition-repo");
    let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect to daemon");
    daemon_request(
        &mut connection,
        DaemonRequest::CreateSessionType {
            source: DaemonSessionTypeMutationSource::Device,
            definition: daemon_definition(&device_authored),
        },
    );
    daemon_request(
        &mut connection,
        DaemonRequest::CreateSpawnTarget {
            target_id: Some("repo:authoring".to_string()),
            label: Some("repo:authoring".to_string()),
            root: repo_root.clone(),
            enabled: true,
            kind: Some("directory".to_string()),
            base_ref: None,
            metadata: BTreeMap::new(),
        },
    );

    // A bare id selects the effective winner, matching ShowSessionType.
    let effective = daemon_read_definition(&mut connection, "authored-shared");
    assert_eq!(effective.definition, daemon_definition(&repo_authored));
    assert_eq!(
        effective.source,
        DaemonSessionTypeMutationSource::Repo {
            target_id: "repo:authoring".to_string()
        }
    );
    assert_eq!(effective.session_type_id, "repo:authoring/authored-shared");

    // A qualified id still reaches the overridden source's authored definition.
    let overridden = daemon_read_definition(&mut connection, "device/authored-shared");
    assert_eq!(overridden.definition, daemon_definition(&device_authored));
    assert_eq!(overridden.source, DaemonSessionTypeMutationSource::Device);
    assert_eq!(overridden.session_type_id, "device/authored-shared");

    // Repo round trip through the atomic file-write path.
    let mut updated = effective.definition.clone();
    updated.label = "Updated repo authored agent".to_string();
    daemon_request(
        &mut connection,
        DaemonRequest::UpdateSessionType {
            source: effective.source.clone(),
            definition: updated.clone(),
        },
    );
    let written = fs::read_to_string(repo_root.join(".botster/session-types.json"))
        .expect("read Hub-written repo session types");
    let written: serde_json::Value =
        serde_json::from_str(&written).expect("repo session types parse");
    let stored: Vec<PackageSessionType> =
        serde_json::from_value(written["session_types"].clone()).expect("repo definitions decode");
    let mut expected_repo = repo_authored.clone();
    expected_repo.label = updated.label.clone();
    assert_eq!(stored, vec![expected_repo]);

    let round_tripped = daemon_read_definition(&mut connection, "authored-shared");
    assert_eq!(round_tripped.definition, updated);
    drop(connection);
    hub.shutdown().expect("shutdown isolated hub");
    fs::remove_dir_all(&device_root).expect("remove device fixture");
    fs::remove_dir_all(&repo_root).expect("remove repo fixture");
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let packages = empty_registry();

    let ambiguous = show_session_type_definition(
        &packages.packages(),
        &runtime.state(),
        &"authored-ambiguous".to_string(),
    )
    .expect_err("ambiguous bare ids stay ambiguous for the authoring read");
    assert!(ambiguous.kind == "ambiguous_session_type");

    let qualified = show_session_type_definition(
        &packages.packages(),
        &runtime.state(),
        &"repo:second/authored-ambiguous".to_string(),
    )
    .expect("read qualified authored session type definition");
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let resolved = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"init".to_string(),
        botster_hub::SessionTypeRequest::default(),
    )
    .map(|materialized| materialized.resolved)
    .expect("resolve device template");
    let resolved = resolved;

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
        .update_test_fixture(&config, |state| {
            state.device_session_type_sources = vec![DeviceSessionTypeSource {
                root: device_root.clone(),
                session_types: vec![
                    session_type("bin/first.sh", "first"),
                    session_type("bin/second.sh", "second"),
                ],
            }];
        })
        .expect("persist duplicate device source");

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let error = list_session_types(&empty_registry().packages(), &runtime.state())
        .expect_err("duplicate device ids are rejected");

    assert!(error.kind == "invalid_device_session_types");
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let error = list_session_types(&empty_registry().packages(), &runtime.state())
        .expect_err("duplicate repo ids are rejected");

    assert!(error.kind == "invalid_repo_session_types");
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime state");
    let error = show_session_type(
        &empty_registry().packages(),
        &runtime.state(),
        &"init".to_string(),
    )
    .map(|template| vec![template])
    .expect_err("same-rank repo ids are ambiguous");

    assert!(error.kind == "ambiguous_session_type");
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
        .update_test_fixture(&config, |state| {
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

    let runtime = HubRuntime::load_from_store(config, &store).expect("reload runtime");
    let packages = empty_registry();

    // Management catalog keeps the global effective path and storage provenance.
    // Non-colliding device relative stays as device with device:local provenance.
    let catalog =
        list_session_types(&packages.packages(), &runtime.state()).expect("management catalog");
    let catalog = catalog;
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
    let listed = list_session_types_for_target(
        &packages.packages(),
        &runtime.state(),
        &"tgt_hub".to_string(),
    )
    .expect("list for admitted hub target");
    let listed = listed;
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
    let other_list = list_session_types_for_target(
        &packages.packages(),
        &runtime.state(),
        &"tgt_other".to_string(),
    )
    .expect("list for other target");
    let other_list = other_list;
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
        let resolved = materialize_session_type(
            runtime.config(),
            &packages.packages(),
            &runtime.state(),
            &row.session_type_id.clone(),
            botster_hub::SessionTypeRequest {
                target_id: Some("tgt_hub".to_string()),
                ..botster_hub::SessionTypeRequest::default()
            },
        )
        .map(|materialized| materialized.resolved)
        .unwrap_or_else(|error| panic!("resolve {} at hub: {error:?}", row.session_type_id));
        let resolved = resolved;
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
    let loser_rejected = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"device/zebra".to_string(),
        botster_hub::SessionTypeRequest {
            target_id: Some("tgt_hub".to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("qualified precedence loser must not spawn at T");
    assert!(
        matches!(
            loser_rejected.kind,
            "session_type_not_eligible" | "unknown_session_type"
        ),
        "expected not-eligible/unknown for hidden loser, got {loser_rejected:?}"
    );

    // Relative device cwd binds under T root, not device root.
    let relative = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"device/relative".to_string(),
        botster_hub::SessionTypeRequest {
            target_id: Some("tgt_hub".to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect("resolve relative device at hub");
    let relative = relative;
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
    let cwd_rejected = materialize_session_type(
        runtime.config(),
        &packages.packages(),
        &runtime.state(),
        &"device/alpha".to_string(),
        botster_hub::SessionTypeRequest {
            target_id: Some("tgt_hub".to_string()),
            cwd: Some(device_root.display().to_string()),
            ..botster_hub::SessionTypeRequest::default()
        },
    )
    .map(|materialized| materialized.resolved)
    .expect_err("cwd outside admitted T rejected");
    assert!(cwd_rejected.kind == "cwd_not_admitted");

    // Disabled / missing targets typed-reject (never empty-list-as-no-types).
    let disabled = list_session_types_for_target(
        &packages.packages(),
        &runtime.state(),
        &"tgt_disabled".to_string(),
    )
    .expect_err("disabled target rejected");
    assert!(disabled.kind == "target_not_admitted");
    let missing = list_session_types_for_target(
        &packages.packages(),
        &runtime.state(),
        &"tgt_missing".to_string(),
    )
    .expect_err("missing target rejected");
    assert!(missing.kind == "target_not_found");

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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    let rows = response;
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .flat_map(botster_hub::test_internals::package_navigation_entries)
        .collect::<Vec<_>>();
    let rows = response;
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .flat_map(botster_hub::test_internals::package_navigation_entries)
        .collect::<Vec<_>>();
    let rows = response;
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
    let surfaces = capability(CapabilitySurface::Surfaces, None);
    let mut packages = PackageRegistry::new(vec![surfaces.clone()].into_iter().collect());
    let mut manifest = plugin_manifest("surface.plugin", vec![surfaces]);
    let mut action_only = app_surface("action-only", "Action only");
    action_only.supports = vec![PackageSurfaceOperation::Action];
    manifest.surfaces = vec![action_only];
    packages
        .install(manifest, provenance(), "install surface package")
        .expect("install surface package");

    let undeclared = botster_hub::test_internals::admit_plugin_surface_operation(
        &packages,
        "surface.plugin",
        "missing",
        PackageSurfaceOperation::Render,
        request_id("render-undeclared-surface"),
        HubClientOperation::PluginSurfaceRender,
    )
    .expect_err("undeclared surfaces must be rejected before runtime dispatch");
    assert!(matches!(
        undeclared,
        HubClientError::Plugin { ref code, .. } if code == "undeclared_plugin_surface"
    ));

    let unsupported = botster_hub::test_internals::admit_plugin_surface_operation(
        &packages,
        "surface.plugin",
        "action-only",
        PackageSurfaceOperation::Render,
        request_id("render-unsupported-surface"),
        HubClientOperation::PluginSurfaceRender,
    )
    .expect_err("unsupported surface operations must be rejected before runtime dispatch");
    assert!(matches!(
        unsupported,
        HubClientError::Plugin { ref code, .. } if code == "unsupported_plugin_surface_operation"
    ));
}

#[test]
fn package_availability_projects_core_resolution_matrix_to_client_rows() {
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    let rows = response;
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    let rows = response;
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    let rows = response;
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
    api: &LocalClient,
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    needle: &[u8],
    logical_clock: &mut u64,
) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let needle_text = String::from_utf8_lossy(needle);

    while Instant::now() < deadline {
        let _ = runtime
            .observe_lifecycle_slice(
                *logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge");
        let screen = api
            .read_screen(runtime, session_id, *logical_clock)
            .expect("read screen through core");
        *logical_clock += 1;
        if screen.text.contains(needle_text.as_ref()) {
            return screen.text.as_bytes().to_vec();
        }
        thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for {needle_text:?} on ReadScreen")
}

fn read_screen_until(
    api: &LocalClient,
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    needle: &str,
    logical_clock: &mut u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let _ = runtime
            .observe_lifecycle_slice(
                *logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge");
        let screen = api
            .read_screen(runtime, session_id, *logical_clock)
            .expect("read screen through core");
        *logical_clock += 1;
        if screen.text.contains(needle) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {needle:?} in ReadScreen");
}

fn observe_for(runtime: &mut HubRuntime, logical_clock: &mut u64, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        let _ = runtime
            .observe_lifecycle_slice(
                *logical_clock,
                None,
                botster_core_daemon::ObserveLifecycleBudget {
                    max_sessions: 32,
                    max_encoded_result_bytes: 64 * 1024,
                    max_elapsed: Duration::from_millis(25),
                },
            )
            .wait(std::time::Duration::from_secs(30))
            .expect("core bridge");
        *logical_clock += 1;
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn late_attach_receives_opaque_history_before_later_live_output() {
    let first_api = LocalClient::new("late-history-first-client");
    let late_api = LocalClient::new("late-history-late-client");
    let mut runtime = explicit_runtime("late-history");
    let session_id = SessionId("late-history-session".to_string());
    let first_subscription = SubscriptionId("late-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("late-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api.spawn(&runtime, &session_id.clone(), &"printf 'before-late\\n'; while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string());
    logical_clock += 1;

    let first_adapter = attach_bound_subscription(
        &mut runtime,
        &first_api,
        &session_id,
        &first_subscription,
        logical_clock,
    );
    logical_clock += 1;
    read_screen_until(
        &first_api,
        &mut runtime,
        &session_id,
        "before-late",
        &mut logical_clock,
    );

    attach_bound_subscription(
        &mut runtime,
        &late_api,
        &session_id,
        &late_subscription,
        logical_clock,
    );
    logical_clock += 1;

    let readback = late_api
        .read_screen(&runtime, &session_id.clone(), logical_clock)
        .expect("read screen between late attach and first drain");
    logical_clock += 1;
    let screen = readback;
    assert_eq!(
        screen.text.matches("before-late").count(),
        1,
        "readback before late drain should contain prior output exactly once, got {:?}",
        screen.text
    );

    inject_terminal_command(
        &first_adapter,
        &TerminalInputCommand::RawBytes {
            operation_id: 1,
            data: b"live-after-late\n".to_vec(),
        },
    );
    logical_clock += 1;
    read_screen_until(
        &late_api,
        &mut runtime,
        &session_id,
        "after:live-after-late",
        &mut logical_clock,
    );
    assert!(
        runtime
            .list_terminal_subscriptions(botster_hub_client::MAX_CONTROL_RESPONSE_BYTES)
            .wait(std::time::Duration::from_secs(30))
            .expect("core inventory")
            .expect("inventory fits the test allowance")
            .records
            .iter()
            .any(|row| row.subscription_id == late_subscription && row.adapter_bound),
        "late attach must be adapter-bound"
    );
}

#[test]
fn late_attach_without_prior_output_does_not_fabricate_history() {
    let first_api = LocalClient::new("no-history-first-client");
    let late_api = LocalClient::new("no-history-late-client");
    let mut runtime = explicit_runtime("no-history");
    let session_id = SessionId("no-history-session".to_string());
    let first_subscription = SubscriptionId("no-history-first-subscription".to_string());
    let late_subscription = SubscriptionId("no-history-late-subscription".to_string());
    let mut logical_clock = 100;

    first_api.spawn(
        &runtime,
        &session_id.clone(),
        &"while IFS= read -r line; do printf 'after:%s\\n' \"$line\"; done".to_string(),
    );
    logical_clock += 1;

    let first_adapter = attach_bound_subscription(
        &mut runtime,
        &first_api,
        &session_id,
        &first_subscription,
        logical_clock,
    );
    logical_clock += 1;

    attach_bound_subscription(
        &mut runtime,
        &late_api,
        &session_id,
        &late_subscription,
        logical_clock,
    );
    logical_clock += 1;

    let readback = late_api
        .read_screen(&runtime, &session_id.clone(), logical_clock)
        .expect("read blank screen before sending live output");
    logical_clock += 1;
    let screen = readback;
    assert!(
        screen.text.is_empty(),
        "idle session should have no prior renderable output, got {:?}",
        screen.text
    );

    inject_terminal_command(
        &first_adapter,
        &TerminalInputCommand::RawBytes {
            operation_id: 1,
            data: b"live-only\n".to_vec(),
        },
    );
    logical_clock += 1;
    read_screen_until(
        &late_api,
        &mut runtime,
        &session_id,
        "after:live-only",
        &mut logical_clock,
    );
    assert!(
        runtime
            .list_terminal_subscriptions(botster_hub_client::MAX_CONTROL_RESPONSE_BYTES)
            .wait(std::time::Duration::from_secs(30))
            .expect("core inventory")
            .expect("inventory fits the test allowance")
            .records
            .iter()
            .any(|row| row.subscription_id == late_subscription && row.adapter_bound),
        "no-history late attach must be adapter-bound"
    );
}

#[test]
fn local_client_api_exercises_status_spawn_attach_detach_shutdown_and_events() {
    let api = LocalClient::new("local-client-api-test");
    let second_api = LocalClient::new("local-client-api-test-two");
    let mut runtime = explicit_runtime("session-flow");
    let session_id = session_id();
    let subscription_id = subscription_id();
    let second_subscription_id = SubscriptionId("hub-client-api-subscription-two".to_string());
    let mut logical_clock = 100;

    assert!(
        runtime
            .list_sessions()
            .wait(CORE_WAIT)
            .expect("core bridge")
            .expect("list sessions")
            .is_empty(),
        "a fresh hub lists no sessions"
    );

    let spawn = api.spawn(
        &runtime,
        &session_id.clone(),
        &"printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
            .to_string(),
    );
    logical_clock += 1;
    assert_eq!(spawn.session_id, session_id);
    assert_eq!(spawn.lifecycle, SessionLifecycleState::Running);

    let first_adapter = attach_bound_subscription(
        &mut runtime,
        &api,
        &session_id,
        &subscription_id,
        logical_clock,
    );
    logical_clock += 1;
    let second_adapter = attach_bound_subscription(
        &mut runtime,
        &second_api,
        &session_id,
        &second_subscription_id,
        logical_clock,
    );
    logical_clock += 1;

    read_screen_until(&api, &mut runtime, &session_id, "ready", &mut logical_clock);

    inject_terminal_command(
        &first_adapter,
        &TerminalInputCommand::Resize {
            operation_id: 1,
            rows: 30,
            cols: 100,
            width_px: 0,
            height_px: 0,
        },
    );
    logical_clock += 1;

    inject_terminal_command(
        &first_adapter,
        &TerminalInputCommand::RawBytes {
            operation_id: 2,
            data: b"ping-hub\n".to_vec(),
        },
    );
    logical_clock += 1;

    read_screen_until(
        &api,
        &mut runtime,
        &session_id,
        "echo:ping-hub",
        &mut logical_clock,
    );
    assert!(
        runtime
            .list_terminal_subscriptions(botster_hub_client::MAX_CONTROL_RESPONSE_BYTES)
            .wait(std::time::Duration::from_secs(30))
            .expect("core inventory")
            .expect("inventory fits the test allowance")
            .records
            .iter()
            .filter(|row| row.adapter_bound)
            .count()
            >= 2,
        "both local subscriptions must be adapter-bound"
    );

    api.detach(
        &runtime,
        &session_id.clone(),
        &subscription_id.clone(),
        logical_clock,
    )
    .expect("detach through client api");
    logical_clock += 1;

    inject_terminal_command(
        &second_adapter,
        &TerminalInputCommand::RawBytes {
            operation_id: 1,
            data: b"after-detach\n".to_vec(),
        },
    );
    logical_clock += 1;

    read_screen_until(
        &second_api,
        &mut runtime,
        &session_id,
        "echo:after-detach",
        &mut logical_clock,
    );
    observe_for(&mut runtime, &mut logical_clock, Duration::from_millis(200));

    second_api
        .shutdown(&runtime, &session_id.clone())
        .expect("shutdown through client api");
}

#[test]
fn guarded_notification_write_is_hub_admitted_and_core_delivered() {
    let api = HubClientApi::local_operator("local-client-api-test");
    let client = LocalClient::new("local-client-api-test");
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
    client.spawn(
        &runtime,
        &session_id.clone(),
        &"printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done"
            .to_string(),
    );
    logical_clock += 1;

    // Before this client attaches, Core refuses its write typed (NotSubscribed);
    // the client sees "not attached", never an Ok that lost the bytes.
    let unattached = api
        .handle_request(
            &mut runtime,
            &packages,
            HubClientRequest::GuardedNotificationWrite {
                request_id: request_id("guarded-unattached"),
                session_id: session_id.clone(),
                package_name: "workflow.plugin".to_string(),
                data: b"unattached\n".to_vec(),
                readiness: ReadinessEvidence::ready(ModeFlags {
                    cursor_visible: true,
                    ..ModeFlags::default()
                }),
                now_seconds: logical_clock,
            },
        )
        .wait(&runtime)
        .expect_err("an unattached client's guarded write must be refused");
    logical_clock += 1;
    assert_eq!(
        unattached,
        HubClientError::Runtime {
            request_id: request_id("guarded-unattached"),
            operation: HubClientOperation::GuardedNotificationWrite,
            kind: botster_hub::HubClientRuntimeErrorKind::NotAttached,
        }
    );

    attach_bound_subscription(
        &mut runtime,
        &client,
        &session_id,
        &subscription_id,
        logical_clock,
    );
    logical_clock += 1;

    read_screen_until(
        &client,
        &mut runtime,
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
        .wait(&runtime)
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
        &client,
        &mut runtime,
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
        .wait(&runtime)
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
    let api = LocalClient::new("local-client-api-test");
    let runtime = explicit_runtime("daemon-readback-ops");
    let session_id = SessionId("daemon-readback-session".to_string());
    let mut logical_clock = 1;

    api.spawn(
        &runtime,
        &session_id.clone(),
        &"printf 'screen-ready\\n'; sleep 5".to_string(),
    );
    logical_clock += 1;

    let deadline = Instant::now() + Duration::from_secs(5);
    let screen = loop {
        let read_screen = api
            .read_screen(&runtime, &session_id.clone(), logical_clock)
            .expect("daemon-backed read_screen should return typed response");
        logical_clock += 1;
        let screen = read_screen;
        if screen.text.contains("screen-ready") {
            break screen;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for screen-ready"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert!(screen.text.contains("screen-ready"));
    assert!(screen.unavailable.is_none());

    let capture_snapshot = api
        .capture_snapshot(&runtime, &session_id.clone(), logical_clock)
        .expect("daemon-backed capture_snapshot should return typed response");
    let snapshot = capture_snapshot;
    assert_eq!(snapshot.rows, 24);
    assert_eq!(snapshot.cols, 80);
    assert!(!snapshot.capture_id.0.is_empty());
    assert!(snapshot.total_bytes > 0);
    assert!(snapshot.pages >= 1);
    assert!(snapshot.unavailable.is_none());

    api.shutdown(&runtime, &session_id)
        .expect("shutdown readback session");
}

#[test]
fn read_mode_flags_returns_exact_authoritative_values_and_session_attribution() {
    let api = LocalClient::new("mode-flags-client-api-test");
    let runtime = explicit_runtime("mode-flags-readback");
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
        api.spawn(&runtime, &session_id, &command.to_string());
        logical_clock += 1;
    }

    let off = api
        .read_mode_flags(&runtime, &off_session_id.clone(), logical_clock)
        .expect("read authoritative mouse-off flags");
    logical_clock += 1;
    assert_eq!(off.mode_flags.mouse_mode, 0);
    assert!(off.unavailable.is_none());
    // Full ModeFlags projection is present (not mouse-only).
    let _ = (
        off.mode_flags.kitty_enabled,
        off.mode_flags.cursor_visible,
        off.mode_flags.bracketed_paste,
        off.mode_flags.alt_screen,
        off.mode_flags.focus_reporting,
        off.mode_flags.application_cursor,
        off.rows,
        off.cols,
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let on = loop {
        let response = api
            .read_mode_flags(&runtime, &on_session_id.clone(), logical_clock)
            .expect("read authoritative mouse-on flags");
        logical_clock += 1;
        let mode_flags = response;
        if mode_flags.mode_flags.mouse_mode == 9 {
            break mode_flags;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for exact combined mouse mode, last value {}",
            mode_flags.mode_flags.mouse_mode
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(on.mode_flags.mouse_mode, 9);
    assert!(on.unavailable.is_none());

    let missing = api
        .read_mode_flags(
            &runtime,
            &SessionId("missing-mode-flags-session".to_string()),
            logical_clock,
        )
        .expect_err("unknown session must not default to mouse-off");
    assert!(
        matches!(
            &missing,
            botster_core_daemon::CoreDaemonError::UnknownSession(_)
        ) || matches!(
            &missing,
            botster_core_daemon::CoreDaemonError::Engine(
                botster_core::DefaultBotsterEngineError::Runtime(error)
            ) if error.kind == botster_core::SessionRuntimeErrorKind::SessionNotFound
        ),
        "an unknown session is a typed unknown-session error: {missing:?}"
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

    let response = packages
        .packages()
        .into_iter()
        .map(|record| HubClientPackage::from_record(&packages, record))
        .collect::<Vec<_>>();
    let records = response;
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
        .wait(&runtime)
        .expect("plugin lifecycle status through client api");
    let HubClientResponseBody::PluginLifecycle(records) = response.body else {
        panic!("plugin lifecycle response expected");
    };
    assert_eq!(records.lifecycle.len(), 1);
    assert_eq!(records.lifecycle[0].package_name, "workflow.plugin");
    assert_eq!(records.lifecycle[0].state, HubClientPackageState::Enabled);
    assert!(!records.lifecycle[0].loaded);
}
