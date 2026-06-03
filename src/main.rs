use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    ClientId, CoreSessionMetadata, RequestId, ResizePayload, SessionId, SessionLifecycleState,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId,
    TerminalAttachState, TransportEgress,
};
use botster_hub::{
    DataDirectoryOption, FileHubStateStore, HubClientApi, HubClientEvent, HubClientObservationKind,
    HubClientPackageClassification, HubClientPackageState, HubClientRequest, HubClientResponseBody,
    HubDaemon, HubDaemonState, HubRuntime, HubStartupOptions, HubStateLoadSource, HubStateStore,
    PackageAction, RuntimeEnvironment, SessionDefaults, TransportBindings,
    build_default_config_for_runtime, default_package_policy, host_profile,
};

const SMOKE_MARKER: &str = "botster-hub-smoke-ok";
const SMOKE_TIMEOUT: Duration = Duration::from_secs(5);

fn main() {
    match env::args().nth(1).as_deref() {
        Some("start") => {
            if let Err(error) = start_daemon(env::args().skip(2).collect()) {
                eprintln!("botster-hub start error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("status") => {
            if let Err(error) = operator_status(env::args().skip(2).collect()) {
                eprintln!("botster-hub status error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("sessions") => {
            if let Err(error) = operator_sessions(env::args().skip(2).collect()) {
                eprintln!("botster-hub sessions error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("packages") => {
            if let Err(error) = operator_packages(env::args().skip(2).collect(), false) {
                eprintln!("botster-hub packages error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("providers") => {
            if let Err(error) = operator_packages(env::args().skip(2).collect(), true) {
                eprintln!("botster-hub providers error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("inspect") => {
            if let Err(error) = operator_inspect(env::args().skip(2).collect()) {
                eprintln!("botster-hub inspect error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("run-one") => {
            if let Err(error) = run_one(env::args().skip(2).collect()) {
                eprintln!("botster-hub run-one error: {error}");
                process::exit(1);
            }
            return;
        }
        _ => {}
    }

    match boot_summary() {
        Ok(config) => {
            let runtime = HubRuntime::new(config);
            let profile = host_profile();
            let package_policy = default_package_policy();
            println!(
                "{} first-party host profile ready for {}: {} roles, {} core capability surfaces, {} package grants",
                profile.id,
                runtime.config().host.id,
                profile.responsibilities().len(),
                profile.capability_surfaces().len(),
                package_policy.registry().granted_capabilities().len()
            );
        }
        Err(error) => {
            eprintln!("botster-hub config error: {error}");
            process::exit(1);
        }
    }
}

fn boot_summary() -> Result<botster_hub::HubConfig, botster_hub::HubConfigError> {
    let environment = RuntimeEnvironment::from_current_process();
    build_default_config_for_runtime(&environment)
}

fn start_daemon(args: Vec<String>) -> Result<(), StartError> {
    let options = DataDirOptions::parse(args, "start")?;
    let config = explicit_config(options.data_directory)?;

    let mut daemon = HubDaemon::start(config)?;
    print_daemon_status("started", &daemon.status());
    let stopped = daemon.stop();
    print_daemon_status("stopped", &stopped);

    Ok(())
}

fn operator_status(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataDirOptions::parse(args, "status")?;
    let config = explicit_config(options.data_directory)?;
    let mut daemon = HubDaemon::start(config)?;
    print_daemon_status("status", &daemon.status());
    daemon.stop();
    Ok(())
}

fn operator_sessions(args: Vec<String>) -> Result<(), OperatorError> {
    let command = SessionCommand::parse(args)?;
    let mut daemon = HubDaemon::start(explicit_config(command.data_directory)?)?;
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-cli");
    let runtime = daemon
        .runtime_mut()
        .ok_or(OperatorError::DaemonNotRunning)?;

    match command.action {
        SessionAction::List => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::ListSessions {
                    request_id: request_id("cli-sessions-list"),
                },
            )?;
            print_client_response(response.body);
        }
        SessionAction::Spawn {
            session_id,
            command,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Spawn {
                    request_id: request_id("cli-sessions-spawn"),
                    session_id,
                    command,
                },
            )?;
            print_client_response(response.body);
        }
        SessionAction::Attach {
            session_id,
            subscription_id,
        } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Attach {
                    request_id: request_id("cli-sessions-attach"),
                    session_id,
                    subscription_id,
                    now_seconds: 1,
                },
            )?;
            print_client_response(response.body);
        }
        SessionAction::SendInput { session_id, data } => {
            let response = api.handle_request(
                runtime,
                &packages,
                HubClientRequest::Input {
                    request_id: request_id("cli-sessions-send-input"),
                    session_id,
                    data,
                    now_seconds: 1,
                },
            )?;
            print_client_response(response.body);
        }
    }

    daemon.stop();
    Ok(())
}

fn operator_packages(args: Vec<String>, providers_only: bool) -> Result<(), OperatorError> {
    let command = PackageCommand::parse(args, providers_only)?;
    let mut daemon = HubDaemon::start(explicit_config(command.data_directory)?)?;

    match command.action {
        PackageActionCommand::List => {}
        PackageActionCommand::EnableLocalPath(path) => {
            let package_name = {
                let record = daemon
                    .package_registry_mut()
                    .install_local_path(path, "operator CLI enable local package")?;
                record.manifest.name.clone()
            };
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "operator CLI enable local package")?;
            persist_package_registry(&daemon)?;
            print_package_decision(&decision);
        }
        PackageActionCommand::EnableName(package_name) => {
            let decision = daemon
                .package_registry_mut()
                .enable(&package_name, "operator CLI enable package")?;
            persist_package_registry(&daemon)?;
            print_package_decision(&decision);
        }
        PackageActionCommand::Disable(package_name) => {
            let decision = daemon
                .package_registry_mut()
                .disable(&package_name, "operator CLI disable package")?;
            persist_package_registry(&daemon)?;
            print_package_decision(&decision);
        }
    }

    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-cli");
    let runtime = daemon
        .runtime_mut()
        .ok_or(OperatorError::DaemonNotRunning)?;
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListPackages {
            request_id: request_id("cli-packages-list"),
        },
    )?;
    print_packages_response(response.body, providers_only);
    daemon.stop();
    Ok(())
}

fn operator_inspect(args: Vec<String>) -> Result<(), OperatorError> {
    let command = InspectCommand::parse(args)?;
    let mut daemon = HubDaemon::start(explicit_config(command.data_directory)?)?;
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-cli");
    let runtime = daemon
        .runtime_mut()
        .ok_or(OperatorError::DaemonNotRunning)?;
    let response = api.handle_request(
        runtime,
        &packages,
        HubClientRequest::ListSessions {
            request_id: request_id("cli-inspect-sessions"),
        },
    )?;
    let HubClientResponseBody::Sessions(sessions) = response.body else {
        return Err(OperatorError::UnexpectedResponse("sessions"));
    };

    println!("inspect=session");
    if let Some(session) = sessions
        .into_iter()
        .find(|session| session.session_id == command.session_id)
    {
        println!("session_id={}", session.session_id.0);
        println!("lifecycle={}", session_lifecycle_label(&session.lifecycle));
    } else {
        println!("session_id={}", command.session_id.0);
        println!("found=false");
    }
    daemon.stop();
    Ok(())
}

fn explicit_config(
    data_directory: PathBuf,
) -> Result<botster_hub::HubConfig, botster_hub::HubConfigError> {
    HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(data_directory),
        session_defaults: SessionDefaults {
            working_directory: Some(PathBuf::from(".")),
            ..SessionDefaults::default()
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
}

fn persist_package_registry(daemon: &HubDaemon) -> Result<(), OperatorError> {
    let runtime = daemon.runtime().ok_or(OperatorError::DaemonNotRunning)?;
    let config = runtime.config().clone();
    let snapshot = daemon.package_registry().snapshot();
    let store = FileHubStateStore::for_data_directory(&config.data_directory);
    store.update(&config, |state| {
        state.package_registry = snapshot;
    })?;
    Ok(())
}

fn print_daemon_status(label: &str, status: &botster_hub::HubDaemonStatus) {
    println!("event={label}");
    println!(
        "lifecycle_state={}",
        lifecycle_state_label(status.lifecycle_state)
    );
    println!("host_id={}", status.host_id);
    println!("host_display_name={}", status.host_display_name);
    println!("schema_version={}", status.schema_version);
    println!("data_dir_configured={}", status.data_dir_configured);
    println!("core_initialized={}", status.core_initialized);
    println!("state_source={}", state_source_label(status.state_source));
    println!("package_count={}", status.package_count);
    println!("enabled_package_count={}", status.enabled_package_count);
    println!("provider_count={}", status.provider_count);
    println!("enabled_provider_count={}", status.enabled_provider_count);
}

fn lifecycle_state_label(state: HubDaemonState) -> &'static str {
    match state {
        HubDaemonState::Created => "created",
        HubDaemonState::Running => "running",
        HubDaemonState::Stopped => "stopped",
    }
}

fn state_source_label(source: HubStateLoadSource) -> &'static str {
    match source {
        HubStateLoadSource::Loaded => "loaded",
        HubStateLoadSource::Initialized => "initialized",
    }
}

fn print_client_response(body: HubClientResponseBody) {
    match body {
        HubClientResponseBody::Status(status) => {
            println!("response=status");
            println!("profile_id={}", status.profile_id);
            println!("host_id={}", status.host_id);
            println!("session_count={}", status.session_count);
            println!("package_count={}", status.package_count);
        }
        HubClientResponseBody::Sessions(sessions) => {
            println!("response=sessions");
            println!("session_count={}", sessions.len());
            for session in sessions {
                println!(
                    "session id={} lifecycle={}",
                    session.session_id.0,
                    session_lifecycle_label(&session.lifecycle)
                );
            }
        }
        HubClientResponseBody::Spawned(spawned) => {
            println!("response=spawned");
            println!("session_id={}", spawned.session.session_id.0);
            println!(
                "lifecycle={}",
                session_lifecycle_label(&spawned.session.lifecycle)
            );
            print_events(&spawned.events);
        }
        HubClientResponseBody::Events(events) => {
            println!("response=events");
            print_events(&events);
        }
        HubClientResponseBody::Packages(_) => {
            print_packages_response(body, false);
        }
        HubClientResponseBody::PluginLifecycle(records) => {
            println!("response=plugin_lifecycle");
            println!("package_count={}", records.len());
            for record in records {
                println!(
                    "package name={} state={} loaded={}",
                    record.package_name,
                    package_state_label(record.state),
                    record.loaded
                );
            }
        }
    }
}

fn print_packages_response(body: HubClientResponseBody, providers_only: bool) {
    let HubClientResponseBody::Packages(packages) = body else {
        return;
    };
    let packages: Vec<_> = packages
        .into_iter()
        .filter(|package| {
            !providers_only
                || matches!(
                    package.classification,
                    HubClientPackageClassification::Provider
                )
        })
        .collect();
    println!(
        "response={}",
        if providers_only {
            "providers"
        } else {
            "packages"
        }
    );
    println!("package_count={}", packages.len());
    for package in packages {
        println!(
            "package name={} version={} classification={} state={} capabilities={} provider_profile_admitted={}",
            package.package_name,
            package.version,
            package_classification_label(package.classification),
            package_state_label(package.state),
            package.requested_capabilities.len(),
            package.provider_profile_admitted
        );
    }
}

fn print_events(events: &[HubClientEvent]) {
    println!("event_count={}", events.len());
    for event in events {
        match event {
            HubClientEvent::SessionLifecycle { session_id, state } => {
                println!(
                    "event=session_lifecycle session_id={} state={}",
                    session_id.0,
                    session_lifecycle_label(state)
                );
            }
            HubClientEvent::TerminalOutput {
                session_id,
                subscription_id,
                data,
            } => {
                println!(
                    "event=terminal_output session_id={} subscription_id={} bytes={}",
                    session_id.0,
                    subscription_id.0,
                    data.len()
                );
            }
            HubClientEvent::Snapshot {
                session_id,
                subscription_id,
                bytes,
            } => {
                println!(
                    "event=snapshot session_id={} subscription_id={} bytes={bytes}",
                    session_id.0, subscription_id.0
                );
            }
            HubClientEvent::Scrollback {
                session_id,
                subscription_id,
                bytes,
            } => {
                println!(
                    "event=scrollback session_id={} subscription_id={} bytes={bytes}",
                    session_id.0, subscription_id.0
                );
            }
            HubClientEvent::ProcessExit {
                session_id,
                subscription_id,
                code,
            } => {
                println!(
                    "event=process_exit session_id={} subscription_id={} code={}",
                    session_id.0,
                    subscription_id.0,
                    code.map_or_else(|| "none".to_string(), |code| code.to_string())
                );
            }
            HubClientEvent::AttachState {
                session_id,
                subscription_id,
                state,
            } => {
                println!(
                    "event=attach_state session_id={} subscription_id={} state={}",
                    session_id.0,
                    subscription_id.0,
                    attach_state_label(state)
                );
            }
            HubClientEvent::RuntimeObservation { kind } => {
                println!(
                    "event=runtime_observation kind={}",
                    observation_label(*kind)
                );
            }
        }
    }
}

fn print_package_decision(decision: &botster_hub::PackageDecision) {
    println!("decision=package");
    println!("package_name={}", decision.package_name);
    println!("action={}", package_action_label(decision.action));
    println!("state={}", package_state_label(decision.state.into()));
    println!(
        "classification={}",
        package_classification_label(decision.classification.into())
    );
}

fn request_id(value: &str) -> RequestId {
    RequestId(value.to_string())
}

fn session_lifecycle_label(state: &SessionLifecycleState) -> &'static str {
    match state {
        SessionLifecycleState::Starting => "starting",
        SessionLifecycleState::Running => "running",
        SessionLifecycleState::Stopping => "stopping",
        SessionLifecycleState::Exited { .. } => "exited",
        SessionLifecycleState::Failed { .. } => "failed",
    }
}

fn attach_state_label(state: &TerminalAttachState) -> &'static str {
    match state {
        TerminalAttachState::Attaching => "attaching",
        TerminalAttachState::Attached => "attached",
        TerminalAttachState::Detached => "detached",
    }
}

fn observation_label(kind: HubClientObservationKind) -> &'static str {
    match kind {
        HubClientObservationKind::SessionActivity => "session_activity",
        HubClientObservationKind::Subscription => "subscription",
        HubClientObservationKind::Backpressure => "backpressure",
    }
}

fn package_classification_label(classification: HubClientPackageClassification) -> &'static str {
    match classification {
        HubClientPackageClassification::Plugin => "plugin",
        HubClientPackageClassification::Provider => "provider",
    }
}

fn package_state_label(state: HubClientPackageState) -> &'static str {
    match state {
        HubClientPackageState::Installed => "installed",
        HubClientPackageState::Enabled => "enabled",
        HubClientPackageState::Disabled => "disabled",
    }
}

fn package_action_label(action: PackageAction) -> &'static str {
    match action {
        PackageAction::Install => "install",
        PackageAction::Enable => "enable",
        PackageAction::Disable => "disable",
        PackageAction::Pin => "pin",
        PackageAction::Prepare => "prepare",
    }
}

struct DataDirOptions {
    data_directory: PathBuf,
}

impl DataDirOptions {
    fn parse(args: Vec<String>, command: &'static str) -> Result<Self, OperatorError> {
        if args.len() != 2 || args.first().map(String::as_str) != Some("--data-dir") {
            return Err(OperatorError::Usage(command));
        }

        Ok(Self {
            data_directory: PathBuf::from(&args[1]),
        })
    }
}

struct SessionCommand {
    data_directory: PathBuf,
    action: SessionAction,
}

enum SessionAction {
    List,
    Spawn {
        session_id: SessionId,
        command: String,
    },
    Attach {
        session_id: SessionId,
        subscription_id: SubscriptionId,
    },
    SendInput {
        session_id: SessionId,
        data: Vec<u8>,
    },
}

impl SessionCommand {
    fn parse(args: Vec<String>) -> Result<Self, OperatorError> {
        let Some(action) = args.first().map(String::as_str) else {
            return Err(OperatorError::Usage("sessions"));
        };
        match action {
            "list" => {
                let options = DataDirOptions::parse(args[1..].to_vec(), "sessions list")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::List,
                })
            }
            "spawn" => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("sessions spawn"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions spawn")?;
                let mut cursor = 3;
                let mut session_id = SessionId("botster-hub-cli-session".to_string());
                if args.get(cursor).map(String::as_str) == Some("--session-id") {
                    let Some(value) = args.get(cursor + 1) else {
                        return Err(OperatorError::Usage("sessions spawn"));
                    };
                    session_id = SessionId(value.clone());
                    cursor += 2;
                }
                if args.get(cursor).map(String::as_str) != Some("--") || args.len() <= cursor + 1 {
                    return Err(OperatorError::Usage("sessions spawn"));
                }
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::Spawn {
                        session_id,
                        command: args[cursor + 1..].join(" "),
                    },
                })
            }
            "attach" => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("sessions attach"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions attach")?;
                let session_id = SessionId(args[3].clone());
                let subscription_id =
                    if args.get(4).map(String::as_str) == Some("--subscription-id") {
                        let Some(value) = args.get(5) else {
                            return Err(OperatorError::Usage("sessions attach"));
                        };
                        SubscriptionId(value.clone())
                    } else {
                        SubscriptionId("botster-hub-cli-subscription".to_string())
                    };
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::Attach {
                        session_id,
                        subscription_id,
                    },
                })
            }
            "send-input" => {
                if args.len() < 6 {
                    return Err(OperatorError::Usage("sessions send-input"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions send-input")?;
                if args.get(4).map(String::as_str) != Some("--") {
                    return Err(OperatorError::Usage("sessions send-input"));
                }
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::SendInput {
                        session_id: SessionId(args[3].clone()),
                        data: args[5..].join(" ").into_bytes(),
                    },
                })
            }
            _ => Err(OperatorError::Usage("sessions")),
        }
    }
}

struct PackageCommand {
    data_directory: PathBuf,
    action: PackageActionCommand,
}

enum PackageActionCommand {
    List,
    EnableLocalPath(PathBuf),
    EnableName(String),
    Disable(String),
}

impl PackageCommand {
    fn parse(args: Vec<String>, providers_only: bool) -> Result<Self, OperatorError> {
        let Some(action) = args.first().map(String::as_str) else {
            return Err(OperatorError::Usage(if providers_only {
                "providers"
            } else {
                "packages"
            }));
        };

        match action {
            "list" => {
                let options = DataDirOptions::parse(args[1..].to_vec(), "packages list")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::List,
                })
            }
            "enable" if !providers_only => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("packages enable"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "packages enable")?;
                if args.get(3).map(String::as_str) == Some("--path") {
                    let Some(path) = args.get(4) else {
                        return Err(OperatorError::Usage("packages enable"));
                    };
                    Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::EnableLocalPath(PathBuf::from(path)),
                    })
                } else {
                    Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::EnableName(args[3].clone()),
                    })
                }
            }
            "disable" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages disable"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "packages disable")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Disable(args[3].clone()),
                })
            }
            _ => Err(OperatorError::Usage(if providers_only {
                "providers list"
            } else {
                "packages"
            })),
        }
    }
}

struct InspectCommand {
    data_directory: PathBuf,
    session_id: SessionId,
}

impl InspectCommand {
    fn parse(args: Vec<String>) -> Result<Self, OperatorError> {
        if args.len() != 3 {
            return Err(OperatorError::Usage("inspect"));
        }
        let options = DataDirOptions::parse(args[0..2].to_vec(), "inspect")?;
        Ok(Self {
            data_directory: options.data_directory,
            session_id: SessionId(args[2].clone()),
        })
    }
}

fn run_one(args: Vec<String>) -> Result<(), RunOneError> {
    let options = RunOneOptions::parse(args)?;
    let command = options.command.clone();
    let config = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(options.data_directory),
        session_defaults: SessionDefaults {
            working_directory: Some(PathBuf::from(".")),
            ..SessionDefaults::default()
        },
        transports: TransportBindings {
            local_socket: None,
            tcp: Vec::new(),
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))?;

    let profile = host_profile();
    let host_id = config.host.id.clone();
    let mut runtime = HubRuntime::load(config)?;
    let request = SessionSpawnRequest {
        request_id: RequestId("botster-hub-smoke-spawn".to_string()),
        session_id: SessionId("botster-hub-smoke-session".to_string()),
        executable: "/bin/sh".to_string(),
        arguments: run_one_shell_arguments(command),
        working_directory: SpawnWorkingDirectory {
            path: ".".to_string(),
        },
        environment: SpawnEnvironment::default(),
        initial_pty_size: Some(ResizePayload { rows: 24, cols: 80 }),
    };
    let session_id = request.session_id.clone();
    let client_id = ClientId("botster-hub-smoke-client".to_string());
    let subscription_id = SubscriptionId("botster-hub-smoke-subscription".to_string());
    let mut logical_clock = 1;

    let spawn = runtime.spawn_session(request, CoreSessionMetadata::new())?;
    runtime.attach_client(
        client_id.clone(),
        session_id.clone(),
        subscription_id.clone(),
        logical_clock,
    )?;
    logical_clock += 1;

    runtime.resize(
        client_id.clone(),
        session_id.clone(),
        30,
        100,
        logical_clock,
    )?;
    logical_clock += 1;

    let observed = drain_until_marker(&mut runtime, &session_id, &mut logical_clock)?;
    let detach = runtime.detach_client(
        client_id,
        session_id.clone(),
        subscription_id,
        logical_clock,
    )?;
    logical_clock += 1;
    let shutdown =
        runtime.shutdown_session(session_id.clone(), "run-one complete", logical_clock)?;

    println!(
        "{} first-party host profile booted for {} through DefaultBotsterEngine",
        profile.id, host_id
    );
    println!("spawned_session={}", spawn.handle.session_id.0);
    println!("observed_marker={SMOKE_MARKER}");
    println!("observed_bytes={}", observed.len());
    println!("detach_observations={}", detach.observations.len());
    println!("shutdown_observations={}", shutdown.observations.len());

    Ok(())
}

fn drain_until_marker(
    runtime: &mut HubRuntime,
    session_id: &SessionId,
    logical_clock: &mut u64,
) -> Result<Vec<u8>, RunOneError> {
    let deadline = Instant::now() + SMOKE_TIMEOUT;
    let marker = SMOKE_MARKER.as_bytes();
    let mut observed = Vec::new();

    while Instant::now() < deadline {
        let output = runtime.drain_runtime_once(session_id, *logical_clock)?;
        *logical_clock += 1;

        for (_, frame) in output.client_egress {
            if let TransportEgress::TerminalOutput { data, .. } = frame {
                observed.extend(data);
            }
        }

        if observed
            .windows(marker.len())
            .any(|window| window == marker)
        {
            return Ok(observed);
        }

        thread::sleep(Duration::from_millis(20));
    }

    Err(RunOneError::TimedOut)
}

fn run_one_shell_arguments(command: RunOneCommand) -> Vec<String> {
    let mut arguments = vec![
        "-c".to_string(),
        "printf 'botster-hub-smoke-started\\n'; \"$@\"; sleep 1".to_string(),
        "botster-hub-run-one".to_string(),
        command.executable,
    ];
    arguments.extend(command.arguments);
    arguments
}

struct RunOneOptions {
    data_directory: PathBuf,
    command: RunOneCommand,
}

impl RunOneOptions {
    fn parse(args: Vec<String>) -> Result<Self, RunOneError> {
        if args.len() < 4 || args.first().map(String::as_str) != Some("--data-dir") {
            return Err(RunOneError::Usage);
        }

        let data_directory = PathBuf::from(&args[1]);
        if args.get(2).map(String::as_str) != Some("--") {
            return Err(RunOneError::Usage);
        }

        let executable = args[3].clone();
        let arguments = args[4..].to_vec();
        if executable.trim().is_empty() {
            return Err(RunOneError::Usage);
        }

        Ok(Self {
            data_directory,
            command: RunOneCommand {
                executable,
                arguments,
            },
        })
    }
}

#[derive(Clone)]
struct RunOneCommand {
    executable: String,
    arguments: Vec<String>,
}

#[derive(Debug)]
enum StartError {
    Operator(OperatorError),
    Config(botster_hub::HubConfigError),
    Daemon(botster_hub::HubDaemonError),
}

#[derive(Debug)]
enum OperatorError {
    Usage(&'static str),
    UnexpectedResponse(&'static str),
    DaemonNotRunning,
    Config(botster_hub::HubConfigError),
    Client(botster_hub::HubClientError),
    Daemon(botster_hub::HubDaemonError),
    Package(botster_hub::PackageRegistryError),
    State(botster_hub::HubStateStoreError),
}

#[derive(Debug)]
enum RunOneError {
    Usage,
    Config(botster_hub::HubConfigError),
    Runtime(botster_hub::HubRuntimeError),
    State(botster_hub::HubStateStoreError),
    TimedOut,
}

impl fmt::Display for StartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operator(error) => write!(formatter, "{error}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
        }
    }
}

impl fmt::Display for OperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(command) => write!(formatter, "{}", usage_for(command)),
            Self::UnexpectedResponse(response) => {
                write!(formatter, "unexpected client API response for {response}")
            }
            Self::DaemonNotRunning => write!(formatter, "hub daemon runtime is not running"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "client API error: {error:?}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Package(error) => write!(formatter, "package policy error: {error:?}"),
            Self::State(error) => write!(formatter, "{error}"),
        }
    }
}

fn usage_for(command: &str) -> &'static str {
    match command {
        "start" => "usage: botster-hub start --data-dir <path>",
        "status" => "usage: botster-hub status --data-dir <path>",
        "sessions" => "usage: botster-hub sessions <list|spawn|attach|send-input> ...",
        "sessions list" => "usage: botster-hub sessions list --data-dir <path>",
        "sessions spawn" => {
            "usage: botster-hub sessions spawn --data-dir <path> [--session-id <id>] -- <command>"
        }
        "sessions attach" => {
            "usage: botster-hub sessions attach --data-dir <path> <session-id> [--subscription-id <id>]"
        }
        "sessions send-input" => {
            "usage: botster-hub sessions send-input --data-dir <path> <session-id> -- <bytes>"
        }
        "packages" => "usage: botster-hub packages <list|enable|disable> ...",
        "packages list" => "usage: botster-hub packages list --data-dir <path>",
        "packages enable" => {
            "usage: botster-hub packages enable --data-dir <path> (--path <package-dir-or-manifest>|<name>)"
        }
        "packages disable" => "usage: botster-hub packages disable --data-dir <path> <name>",
        "providers" | "providers list" => "usage: botster-hub providers list --data-dir <path>",
        "inspect" => "usage: botster-hub inspect --data-dir <path> <session-id>",
        _ => "usage: botster-hub <start|status|sessions|packages|providers|inspect|run-one>",
    }
}

impl fmt::Display for RunOneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(
                formatter,
                "usage: botster-hub run-one --data-dir <path> -- <command> [args...]"
            ),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::TimedOut => write!(
                formatter,
                "timed out waiting for {SMOKE_MARKER}; command must print the smoke marker"
            ),
        }
    }
}

impl From<botster_hub::HubConfigError> for RunOneError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::HubConfigError> for StartError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::HubDaemonError> for StartError {
    fn from(error: botster_hub::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<OperatorError> for StartError {
    fn from(error: OperatorError) -> Self {
        Self::Operator(error)
    }
}

impl From<botster_hub::HubConfigError> for OperatorError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::HubClientError> for OperatorError {
    fn from(error: botster_hub::HubClientError) -> Self {
        Self::Client(error)
    }
}

impl From<botster_hub::HubDaemonError> for OperatorError {
    fn from(error: botster_hub::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<botster_hub::PackageRegistryError> for OperatorError {
    fn from(error: botster_hub::PackageRegistryError) -> Self {
        Self::Package(error)
    }
}

impl From<botster_hub::HubStateStoreError> for OperatorError {
    fn from(error: botster_hub::HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<botster_hub::HubRuntimeError> for RunOneError {
    fn from(error: botster_hub::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<botster_hub::HubStateStoreError> for RunOneError {
    fn from(error: botster_hub::HubStateStoreError) -> Self {
        Self::State(error)
    }
}
