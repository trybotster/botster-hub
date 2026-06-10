use std::env;
use std::fmt;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    ClientId, CoreSessionMetadata, RequestId, ResizePayload, SessionId, SessionLifecycleState,
    SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, TransportEgress,
};
use botster_hub::{
    DaemonCompatibility, DaemonEvent, DaemonOperatorError, DaemonPackage, DaemonRequest,
    DaemonResponse, DaemonResponseKind, DaemonSession, DaemonStatus, DataDirectoryOption,
    HubClientApi, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState, HubRuntime,
    HubStartupOptions, HubStateLoadSource, RuntimeEnvironment, SessionDefaults, TransportBindings,
    build_default_config_for_runtime, daemon_transport_request, default_package_policy,
    host_profile, run_tui, serve_daemon, serve_mcp_stdio, stream_attach,
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
        Some("dogfood") => {
            if let Err(error) = dogfood(env::args().skip(2).collect()) {
                eprintln!("botster-hub dogfood error: {error}");
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
        Some("shutdown") => {
            if let Err(error) = operator_shutdown(env::args().skip(2).collect()) {
                eprintln!("botster-hub shutdown error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("mcp-serve") => {
            if let Err(error) = mcp_serve(env::args().skip(2).collect()) {
                eprintln!("botster-hub mcp-serve error: {error}");
                process::exit(1);
            }
            return;
        }
        Some("tui") => {
            if let Err(error) = operator_tui(env::args().skip(2).collect()) {
                eprintln!("botster-hub tui error: {error}");
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
    let options = StartOptions::parse(args)?;
    let config = explicit_config_with_worker(options.data_directory, options.session_worker_bin)?;

    let stopped = serve_daemon(config)?;
    let status = DaemonStatus {
        lifecycle_state: lifecycle_state_label(stopped.lifecycle_state).to_string(),
        compatibility: DaemonCompatibility::current(),
        host_id: stopped.host_id,
        host_display_name: stopped.host_display_name,
        schema_version: stopped.schema_version,
        data_dir_configured: stopped.data_dir_configured,
        core_initialized: stopped.core_initialized,
        state_source: state_source_label(stopped.state_source).to_string(),
        package_count: stopped.package_count,
        enabled_package_count: stopped.enabled_package_count,
        provider_count: stopped.provider_count,
        enabled_provider_count: stopped.enabled_provider_count,
        session_count: 0,
        recovered_sessions: stopped
            .recovered_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        stale_sessions: stopped
            .stale_sessions
            .iter()
            .map(|session_id| session_id.0.clone())
            .collect(),
        diagnostics: Vec::new(),
    };
    print_daemon_transport_status("stopped", &status);

    Ok(())
}

fn dogfood(args: Vec<String>) -> Result<(), DogfoodError> {
    let options = DogfoodOptions::parse(args)?;
    let data_directory = options.data_directory()?;
    std::fs::create_dir_all(&data_directory).map_err(|source| DogfoodError::CreateDataDir {
        path: data_directory.clone(),
        source,
    })?;

    let hub_bin = env::current_exe().map_err(DogfoodError::CurrentExe)?;
    let mut child = spawn_dogfood_daemon(&hub_bin, &data_directory, &options)?;

    if let Err(error) = wait_for_dogfood_ready(&data_directory, &mut child) {
        cleanup_dogfood_child(&mut child);
        return Err(error);
    }

    let config = explicit_config(data_directory.clone())?;
    if let Err(error) = verify_dogfood_session_worker(&config) {
        let _ = daemon_transport_request(&config, DaemonRequest::DaemonShutdown);
        cleanup_dogfood_child(&mut child);
        return Err(error);
    }

    let response = daemon_transport_request(
        &config,
        DaemonRequest::EnablePackageLocalPath {
            path: options.package_path(),
        },
    )?;
    if response.kind == DaemonResponseKind::OperatorError {
        let _ = daemon_transport_request(&config, DaemonRequest::DaemonShutdown);
        cleanup_dogfood_child(&mut child);
        return Err(DogfoodError::PackageEnable(
            response
                .error
                .map(|error| error.message)
                .unwrap_or_else(|| "package enable failed".to_string()),
        ));
    }

    let package_state = response
        .package_decision
        .as_ref()
        .map(|decision| decision.state.as_str())
        .unwrap_or("enabled");

    print_dogfood_ready(&data_directory, options.default_data_dir, package_state);

    let status = child.wait().map_err(DogfoodError::WaitDaemon)?;
    if status.success() {
        Ok(())
    } else {
        Err(DogfoodError::DaemonExited(status.to_string()))
    }
}

fn verify_dogfood_session_worker(config: &botster_hub::HubConfig) -> Result<(), DogfoodError> {
    let response = daemon_transport_request(
        config,
        DaemonRequest::Spawn {
            session_id: "dogfood-worker-smoke".to_string(),
            command: "printf 'dogfood-worker-ok\\n'; sleep 1".to_string(),
        },
    )?;
    if response.kind == DaemonResponseKind::OperatorError {
        return Err(DogfoodError::SessionWorker(
            response
                .error
                .map(|error| error.message)
                .unwrap_or_else(|| "session worker smoke failed".to_string()),
        ));
    }

    let _ = daemon_transport_request(
        config,
        DaemonRequest::ShutdownSession {
            session_id: "dogfood-worker-smoke".to_string(),
        },
    );
    Ok(())
}

fn spawn_dogfood_daemon(
    hub_bin: &Path,
    data_directory: &Path,
    options: &DogfoodOptions,
) -> Result<Child, DogfoodError> {
    if !hub_bin.is_file() {
        return Err(DogfoodError::MissingHubBinary(hub_bin.to_path_buf()));
    }
    let session_worker_bin = options.session_worker_bin(hub_bin)?;

    let mut command = Command::new(hub_bin);
    command
        .arg("start")
        .arg("--data-dir")
        .arg(data_directory)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command.arg("--session-worker-bin").arg(&session_worker_bin);

    command.spawn().map_err(|source| DogfoodError::SpawnDaemon {
        path: hub_bin.to_path_buf(),
        source,
    })
}

fn wait_for_dogfood_ready(data_directory: &Path, child: &mut Child) -> Result<(), DogfoodError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let config = explicit_config(data_directory.to_path_buf())?;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().map_err(DogfoodError::PollDaemon)? {
            return Err(DogfoodError::DaemonExited(status.to_string()));
        }
        if daemon_transport_request(&config, DaemonRequest::Status).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }

    Err(DogfoodError::ReadinessTimeout)
}

fn cleanup_dogfood_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn print_dogfood_ready(data_directory: &Path, default_data_dir: bool, package_state: &str) {
    let dir = data_directory.display();
    println!("dogfood=ready");
    if default_data_dir {
        println!("data_dir=isolated:{dir}");
    } else {
        println!("data_dir={dir}");
    }
    println!("package name=project-pipelines state={package_state}");
    println!("tui=botster-hub tui --data-dir {dir}");
    println!("mcp=botster-hub mcp-serve --data-dir {dir}");
    println!("status=botster-hub status --data-dir {dir}");
    println!(
        "shutdown=run botster-hub shutdown --data-dir {dir} from another terminal for graceful shutdown; Ctrl-C hard-stops the foreground launcher"
    );
    println!("web=local web entrypoint unavailable in this repo; use TUI or MCP for local dogfood");
}

fn operator_status(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataDirOptions::parse(args, "status")?;
    let config = explicit_config(options.data_directory)?;
    let response = daemon_transport_request(&config, DaemonRequest::Status)?;
    let Some(status) = response.status else {
        return Err(OperatorError::UnexpectedResponse("status"));
    };
    print_daemon_transport_status("status", &status);
    Ok(())
}

fn operator_sessions(args: Vec<String>) -> Result<(), OperatorError> {
    let command = SessionCommand::parse(args)?;
    let config = explicit_config(command.data_directory)?;

    match command.action {
        SessionAction::List => {
            let response = daemon_transport_request(&config, DaemonRequest::ListSessions)?;
            print_daemon_response(response)?;
        }
        SessionAction::Spawn {
            session_id,
            command,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::Spawn {
                    session_id: session_id.0,
                    command,
                },
            )?;
            print_daemon_response(response)?;
        }
        SessionAction::Attach {
            session_id,
            subscription_id,
        } => {
            stream_attach(&config, session_id, subscription_id, &mut io::stdout())?;
        }
        SessionAction::SendInput { session_id, data } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::SendInput {
                    session_id: session_id.0,
                    data: String::from_utf8_lossy(&data).to_string(),
                },
            )?;
            print_daemon_response(response)?;
        }
        SessionAction::Resize {
            session_id,
            rows,
            cols,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::Resize {
                    session_id: session_id.0,
                    rows,
                    cols,
                },
            )?;
            print_daemon_response(response)?;
        }
        SessionAction::Detach {
            session_id,
            subscription_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::Detach {
                    session_id: session_id.0,
                    subscription_id: subscription_id.0,
                },
            )?;
            print_daemon_response(response)?;
        }
        SessionAction::Shutdown { session_id } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::ShutdownSession {
                    session_id: session_id.0,
                },
            )?;
            print_daemon_response(response)?;
        }
    }

    Ok(())
}

fn operator_shutdown(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataDirOptions::parse(args, "shutdown")?;
    let config = explicit_config(options.data_directory)?;
    let response = daemon_transport_request(&config, DaemonRequest::DaemonShutdown)?;
    print_daemon_response(response)?;
    Ok(())
}

fn mcp_serve(args: Vec<String>) -> Result<(), McpCliError> {
    let options = DataDirOptions::parse(args, "mcp-serve")?;
    let config = explicit_config(options.data_directory)?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve_mcp_stdio(config, BufReader::new(stdin.lock()), stdout.lock())?;
    Ok(())
}

fn operator_tui(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataDirOptions::parse(args, "tui")?;
    let config = explicit_config(options.data_directory)?;
    run_tui(config)?;
    Ok(())
}

fn operator_packages(args: Vec<String>, providers_only: bool) -> Result<(), OperatorError> {
    let command = PackageCommand::parse(args, providers_only)?;
    let config = explicit_config(command.data_directory)?;

    match command.action {
        PackageActionCommand::List => {
            let response = daemon_transport_request(&config, DaemonRequest::ListPackages)?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::InstallLocalPath(path) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::InstallPackageLocalPath { path })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::Show(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::ShowPackage { package_name })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::EnableLocalPath(path) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::EnablePackageLocalPath { path })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::EnableName(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::EnablePackage { package_name })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::Disable(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::DisablePackage { package_name })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::Remove(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::RemovePackage { package_name })?;
            print_packages_response(response, providers_only)?;
        }
    }
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
    explicit_config_with_worker(data_directory, None)
}

fn explicit_config_with_worker(
    data_directory: PathBuf,
    session_worker_path: Option<PathBuf>,
) -> Result<botster_hub::HubConfig, botster_hub::HubConfigError> {
    HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(data_directory),
        session_defaults: SessionDefaults {
            working_directory: Some(PathBuf::from(".")),
            ..SessionDefaults::default()
        },
        core_engine: botster_hub::CoreEngineOptions {
            session_worker_path,
            ..botster_hub::CoreEngineOptions::default()
        },
        transports: TransportBindings {
            ..TransportBindings::default()
        },
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None, None))
}

fn print_daemon_transport_status(label: &str, status: &DaemonStatus) {
    println!("event={label}");
    println!("lifecycle_state={}", status.lifecycle_state);
    println!("protocol={}", status.compatibility.protocol);
    println!("protocol_version={}", status.compatibility.protocol_version);
    println!(
        "conformance_fixture_revision={}",
        status.compatibility.conformance_fixture_revision
    );
    println!("features={}", status.compatibility.features.join(","));
    println!("host_id={}", status.host_id);
    println!("host_display_name={}", status.host_display_name);
    println!("schema_version={}", status.schema_version);
    println!("data_dir_configured={}", status.data_dir_configured);
    println!("core_initialized={}", status.core_initialized);
    println!("state_source={}", status.state_source);
    println!("package_count={}", status.package_count);
    println!("enabled_package_count={}", status.enabled_package_count);
    println!("provider_count={}", status.provider_count);
    println!("enabled_provider_count={}", status.enabled_provider_count);
    println!("session_count={}", status.session_count);
    println!(
        "recovered_session_count={}",
        status.recovered_sessions.len()
    );
    for session_id in &status.recovered_sessions {
        println!("recovered_session id={session_id}");
    }
    println!("stale_session_count={}", status.stale_sessions.len());
    for session_id in &status.stale_sessions {
        println!("stale_session id={session_id}");
    }
}

fn print_daemon_response(response: DaemonResponse) -> Result<(), OperatorError> {
    let mut operator_error = None;
    match response.kind {
        DaemonResponseKind::Status => {
            if let Some(status) = response.status {
                print_daemon_transport_status("status", &status);
            }
        }
        DaemonResponseKind::Sessions => {
            println!("response=sessions");
            println!("session_count={}", response.sessions.len());
            for session in response.sessions {
                print_daemon_session(&session);
            }
        }
        DaemonResponseKind::Spawned => {
            println!("response=spawned");
            if let Some(session) = response.sessions.first() {
                println!("session_id={}", session.session_id);
                println!("lifecycle={}", session.lifecycle);
            }
            print_daemon_events(&response.events);
        }
        DaemonResponseKind::Events => {
            println!("response=events");
            print_daemon_events(&response.events);
        }
        DaemonResponseKind::Packages => {
            print_packages(&response.packages, false);
        }
        DaemonResponseKind::PackageDecision => {
            if let Some(decision) = response.package_decision {
                print_package_decision(&decision);
            }
            print_packages(&response.packages, false);
        }
        DaemonResponseKind::PluginLifecycle => {
            println!("response=plugin_lifecycle");
            println!("plugin_count={}", response.lifecycle.len());
            for lifecycle in response.lifecycle {
                println!(
                    "plugin package_name={} state={} loaded={}",
                    lifecycle.package_name, lifecycle.state, lifecycle.loaded
                );
            }
        }
        DaemonResponseKind::PluginMcpTools => {
            println!("response=plugin_mcp_tools");
            println!("tool_count={}", response.plugin_tools.len());
            for tool in response.plugin_tools {
                let name = tool
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("<unknown>");
                println!("tool name={name}");
            }
        }
        DaemonResponseKind::PluginMcpToolResult => {
            println!("response=plugin_mcp_tool_result");
            println!("result={}", response.plugin_tool_result);
        }
        DaemonResponseKind::PluginSurface => {
            println!("response=plugin_surface");
            if let Some(surface) = response.plugin_surface {
                let json = serde_json::to_string(&surface)
                    .unwrap_or_else(|_| "{\"error\":\"unserializable\"}".to_string());
                println!("surface={json}");
            }
        }
        DaemonResponseKind::PluginActionResult => {
            println!("response=plugin_action_result");
            if let Some(result) = response.plugin_action_result {
                let json = serde_json::to_string(&result)
                    .unwrap_or_else(|_| "{\"error\":\"unserializable\"}".to_string());
                println!("result={json}");
            }
        }
        DaemonResponseKind::SessionCleanup => {
            println!("response=session_cleanup");
            if let Some(cleanup) = response.cleanup {
                println!("session_id={}", cleanup.session_id);
                println!("outcome={}", cleanup.outcome);
            }
        }
        DaemonResponseKind::Identity
        | DaemonResponseKind::MessagePosted
        | DaemonResponseKind::Messages
        | DaemonResponseKind::MessageAcked
        | DaemonResponseKind::SessionNotified => {
            println!("response=coordination");
            if let Some(coordination) = response.coordination {
                let json = serde_json::to_string(&coordination)
                    .unwrap_or_else(|_| "{\"error\":\"unserializable\"}".to_string());
                println!("coordination={json}");
            }
        }
        DaemonResponseKind::OperatorError => {
            println!("response=operator_error");
            if let Some(error) = response.error {
                println!("error_code={}", error.code);
                println!("request_id={}", error.request_id);
                println!("operation={}", error.operation);
                println!("message={}", error.message);
                operator_error = Some(error);
            }
        }
        DaemonResponseKind::Shutdown => {
            println!("response=shutdown");
            if let Some(status) = response.status {
                println!("lifecycle_state={}", status.lifecycle_state);
            }
        }
    }

    if let Some(error) = operator_error {
        return Err(OperatorError::DaemonOperator(error));
    }

    Ok(())
}

fn print_daemon_session(session: &DaemonSession) {
    println!(
        "session id={} lifecycle={}",
        session.session_id, session.lifecycle
    );
}

fn print_daemon_events(events: &[DaemonEvent]) {
    println!("event_count={}", events.len());
    for event in events {
        match event {
            DaemonEvent::SessionLifecycle { session_id, state } => {
                println!("event=session_lifecycle session_id={session_id} state={state}");
            }
            DaemonEvent::TerminalOutput {
                session_id,
                subscription_id,
                data,
            } => {
                println!(
                    "event=terminal_output session_id={session_id} subscription_id={subscription_id} bytes={}",
                    data.len()
                );
            }
            DaemonEvent::Snapshot {
                session_id,
                subscription_id,
                bytes,
            } => {
                println!(
                    "event=snapshot session_id={session_id} subscription_id={subscription_id} bytes={bytes}"
                );
            }
            DaemonEvent::Scrollback {
                session_id,
                subscription_id,
                bytes,
            } => {
                println!(
                    "event=scrollback session_id={session_id} subscription_id={subscription_id} bytes={bytes}"
                );
            }
            DaemonEvent::ProcessExit {
                session_id,
                subscription_id,
                code,
            } => {
                println!(
                    "event=process_exit session_id={session_id} subscription_id={subscription_id} code={}",
                    code.map_or_else(|| "none".to_string(), |code| code.to_string())
                );
            }
            DaemonEvent::AttachState {
                session_id,
                subscription_id,
                state,
            } => {
                println!(
                    "event=attach_state session_id={session_id} subscription_id={subscription_id} state={state}"
                );
            }
            DaemonEvent::RuntimeObservation { kind } => {
                println!("event=runtime_observation kind={kind}");
            }
        }
    }
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

fn print_packages_response(
    response: DaemonResponse,
    providers_only: bool,
) -> Result<(), OperatorError> {
    if response.kind == DaemonResponseKind::OperatorError {
        return print_daemon_response(response);
    }
    if let Some(decision) = response.package_decision.as_ref() {
        print_package_decision(decision);
    }
    print_packages(&response.packages, providers_only);
    Ok(())
}

fn print_packages(packages: &[DaemonPackage], providers_only: bool) {
    let packages: Vec<_> = packages
        .iter()
        .filter(|package| !providers_only || package.classification == "provider")
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
            package.classification,
            package.state,
            package.requested_capabilities.len(),
            package.provider_profile_admitted
        );
    }
}

fn print_package_decision(decision: &botster_hub::DaemonPackageDecision) {
    println!("decision=package");
    println!("package_name={}", decision.package_name);
    println!("action={}", decision.action);
    println!("state={}", decision.state);
    println!("classification={}", decision.classification);
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

struct StartOptions {
    data_directory: PathBuf,
    session_worker_bin: Option<PathBuf>,
}

impl StartOptions {
    fn parse(args: Vec<String>) -> Result<Self, OperatorError> {
        if args.len() != 2 && args.len() != 4 {
            return Err(OperatorError::Usage("start"));
        }
        if args.first().map(String::as_str) != Some("--data-dir") {
            return Err(OperatorError::Usage("start"));
        }
        let session_worker_bin = match args.get(2).map(String::as_str) {
            None => None,
            Some("--session-worker-bin") => args.get(3).map(PathBuf::from),
            Some(_) => return Err(OperatorError::Usage("start")),
        };

        Ok(Self {
            data_directory: PathBuf::from(&args[1]),
            session_worker_bin,
        })
    }
}

struct DogfoodOptions {
    data_directory: Option<PathBuf>,
    session_worker_bin: Option<PathBuf>,
    default_data_dir: bool,
}

impl DogfoodOptions {
    fn parse(args: Vec<String>) -> Result<Self, DogfoodError> {
        let mut data_directory = None;
        let mut session_worker_bin = None;
        let mut cursor = 0;

        while cursor < args.len() {
            match args[cursor].as_str() {
                "--data-dir" => {
                    let Some(value) = args.get(cursor + 1) else {
                        return Err(DogfoodError::Usage);
                    };
                    data_directory = Some(PathBuf::from(value));
                    cursor += 2;
                }
                "--session-worker-bin" => {
                    let Some(value) = args.get(cursor + 1) else {
                        return Err(DogfoodError::Usage);
                    };
                    session_worker_bin = Some(PathBuf::from(value));
                    cursor += 2;
                }
                _ => return Err(DogfoodError::Usage),
            }
        }

        Ok(Self {
            default_data_dir: data_directory.is_none(),
            data_directory,
            session_worker_bin,
        })
    }

    fn package_path(&self) -> PathBuf {
        PathBuf::from("examples/project-pipelines")
    }

    fn session_worker_bin(&self, hub_bin: &Path) -> Result<PathBuf, DogfoodError> {
        if let Some(path) = self.session_worker_bin.as_ref() {
            if path.is_file() {
                return Ok(path.clone());
            }
            return Err(DogfoodError::MissingSessionWorkerBinary(path.clone()));
        }

        let Some(bin_dir) = hub_bin.parent() else {
            return Err(DogfoodError::MissingSessionWorkerBinary(PathBuf::from(
                "botster-session-worker",
            )));
        };
        let path = bin_dir.join("botster-session-worker");
        if path.is_file() {
            Ok(path)
        } else if bin_dir.file_name().and_then(|name| name.to_str()) == Some("deps")
            && let Some(debug_dir) = bin_dir.parent()
        {
            let debug_path = debug_dir.join("botster-session-worker");
            if debug_path.is_file() {
                Ok(debug_path)
            } else {
                Err(DogfoodError::MissingSessionWorkerBinary(debug_path))
            }
        } else {
            Err(DogfoodError::MissingSessionWorkerBinary(path))
        }
    }

    fn data_directory(&self) -> Result<PathBuf, DogfoodError> {
        if let Some(path) = self.data_directory.as_ref() {
            return Ok(path.clone());
        }

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(DogfoodError::Clock)?
            .as_nanos();
        Ok(PathBuf::from("target")
            .join("botster-hub-dogfood")
            .join(format!("{}-{nanos}", process::id())))
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
    Resize {
        session_id: SessionId,
        rows: u16,
        cols: u16,
    },
    Detach {
        session_id: SessionId,
        subscription_id: SubscriptionId,
    },
    Shutdown {
        session_id: SessionId,
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
            "resize" => {
                if args.len() != 6 {
                    return Err(OperatorError::Usage("sessions resize"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions resize")?;
                let rows = args[4]
                    .parse::<u16>()
                    .map_err(|_| OperatorError::Usage("sessions resize"))?;
                let cols = args[5]
                    .parse::<u16>()
                    .map_err(|_| OperatorError::Usage("sessions resize"))?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::Resize {
                        session_id: SessionId(args[3].clone()),
                        rows,
                        cols,
                    },
                })
            }
            "detach" => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("sessions detach"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions detach")?;
                let subscription_id =
                    if args.get(4).map(String::as_str) == Some("--subscription-id") {
                        let Some(value) = args.get(5) else {
                            return Err(OperatorError::Usage("sessions detach"));
                        };
                        SubscriptionId(value.clone())
                    } else {
                        SubscriptionId("botster-hub-cli-subscription".to_string())
                    };
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::Detach {
                        session_id: SessionId(args[3].clone()),
                        subscription_id,
                    },
                })
            }
            "shutdown" => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("sessions shutdown"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "sessions shutdown")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::Shutdown {
                        session_id: SessionId(args[3].clone()),
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
    InstallLocalPath(PathBuf),
    Show(String),
    EnableLocalPath(PathBuf),
    EnableName(String),
    Disable(String),
    Remove(String),
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
            "install" if !providers_only => {
                if args.len() != 5 || args.get(3).map(String::as_str) != Some("--path") {
                    return Err(OperatorError::Usage("packages install"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "packages install")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::InstallLocalPath(PathBuf::from(&args[4])),
                })
            }
            "show" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages show"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "packages show")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Show(args[3].clone()),
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
            "remove" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages remove"));
                }
                let options = DataDirOptions::parse(args[1..3].to_vec(), "packages remove")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Remove(args[3].clone()),
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

    let spawn = runtime.spawn_session(request, CoreSessionMetadata::new(), logical_clock)?;
    logical_clock += 1;
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
    runtime.detach_client(
        client_id,
        session_id.clone(),
        subscription_id,
        logical_clock,
    )?;
    logical_clock += 1;
    runtime.shutdown_session(session_id.clone(), logical_clock)?;

    println!(
        "{} first-party host profile booted for {} through CoreDaemon",
        profile.id, host_id
    );
    println!("spawned_session={}", spawn.session_id.0);
    println!("observed_marker={SMOKE_MARKER}");
    println!("observed_bytes={}", observed.len());
    println!("daemon_session_path=core_daemon");

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
    Operator(Box<OperatorError>),
    Config(botster_hub::HubConfigError),
    Daemon(botster_hub::HubDaemonError),
    Transport(botster_hub::DaemonTransportError),
}

#[derive(Debug)]
enum DogfoodError {
    Usage,
    Clock(std::time::SystemTimeError),
    CurrentExe(io::Error),
    CreateDataDir { path: PathBuf, source: io::Error },
    MissingHubBinary(PathBuf),
    MissingSessionWorkerBinary(PathBuf),
    SpawnDaemon { path: PathBuf, source: io::Error },
    PollDaemon(io::Error),
    ReadinessTimeout,
    Config(botster_hub::HubConfigError),
    Transport(botster_hub::DaemonTransportError),
    SessionWorker(String),
    PackageEnable(String),
    WaitDaemon(io::Error),
    DaemonExited(String),
}

#[derive(Debug)]
enum OperatorError {
    Usage(&'static str),
    UnexpectedResponse(&'static str),
    DaemonNotRunning,
    Config(botster_hub::HubConfigError),
    Client(botster_hub::HubClientError),
    DaemonOperator(DaemonOperatorError),
    Daemon(botster_hub::HubDaemonError),
    Transport(botster_hub::DaemonTransportError),
    Tui(botster_hub::TuiError),
    Package(botster_hub::PackageRegistryError),
    State(botster_hub::HubStateStoreError),
}

#[derive(Debug)]
enum McpCliError {
    Usage(Box<OperatorError>),
    Config(botster_hub::HubConfigError),
    Serve(botster_hub::McpServeError),
}

#[derive(Debug)]
enum RunOneError {
    Usage,
    Config(botster_hub::HubConfigError),
    Runtime(botster_hub::HubRuntimeError),
    State(botster_hub::HubStateStoreError),
    TimedOut,
}

impl fmt::Display for McpCliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(error) => write!(formatter, "{error}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Serve(error) => write!(formatter, "{error}"),
        }
    }
}

impl fmt::Display for StartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operator(error) => write!(formatter, "{error}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
        }
    }
}

impl fmt::Display for DogfoodError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "{}", usage_for("dogfood")),
            Self::Clock(error) => write!(formatter, "{error}"),
            Self::CurrentExe(error) => {
                write!(formatter, "resolve current botster-hub binary: {error}")
            }
            Self::CreateDataDir { path, source } => {
                write!(
                    formatter,
                    "create dogfood data dir {}: {source}",
                    path.display()
                )
            }
            Self::MissingHubBinary(path) => {
                write!(
                    formatter,
                    "missing botster-hub binary at {}",
                    path.display()
                )
            }
            Self::MissingSessionWorkerBinary(path) => write!(
                formatter,
                "missing botster-session-worker binary at {}; pass --session-worker-bin <path>",
                path.display()
            ),
            Self::SpawnDaemon { path, source } => {
                write!(
                    formatter,
                    "spawn dogfood daemon {}: {source}",
                    path.display()
                )
            }
            Self::PollDaemon(error) => write!(formatter, "poll dogfood daemon: {error}"),
            Self::ReadinessTimeout => {
                write!(formatter, "timed out waiting for dogfood daemon readiness")
            }
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::SessionWorker(message) => {
                write!(formatter, "verify botster-session-worker: {message}")
            }
            Self::PackageEnable(message) => {
                write!(formatter, "enable project-pipelines package: {message}")
            }
            Self::WaitDaemon(error) => write!(formatter, "wait for dogfood daemon: {error}"),
            Self::DaemonExited(status) => write!(formatter, "dogfood daemon exited with {status}"),
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
            Self::DaemonOperator(error) => {
                write!(
                    formatter,
                    "operator error: {} {}",
                    error.code, error.message
                )
            }
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Tui(error) => write!(formatter, "{error}"),
            Self::Package(error) => write!(formatter, "package policy error: {error:?}"),
            Self::State(error) => write!(formatter, "{error}"),
        }
    }
}

fn usage_for(command: &str) -> &'static str {
    match command {
        "start" => "usage: botster-hub start --data-dir <path> [--session-worker-bin <path>]",
        "dogfood" => "usage: botster-hub dogfood [--data-dir <path>] [--session-worker-bin <path>]",
        "status" => "usage: botster-hub status --data-dir <path>",
        "sessions" => {
            "usage: botster-hub sessions <list|spawn|attach|send-input|resize|detach|shutdown> ..."
        }
        "sessions list" => "usage: botster-hub sessions list --data-dir <path>",
        "sessions spawn" => {
            "usage: botster-hub sessions spawn --data-dir <path> [--session-id <id>] -- <command>"
        }
        "sessions attach" => {
            "usage: botster-hub sessions attach --data-dir <path> <session-id> [--subscription-id <id>]"
        }
        "sessions resize" => {
            "usage: botster-hub sessions resize --data-dir <path> <session-id> <rows> <cols>"
        }
        "sessions detach" => {
            "usage: botster-hub sessions detach --data-dir <path> <session-id> [--subscription-id <id>]"
        }
        "sessions send-input" => {
            "usage: botster-hub sessions send-input --data-dir <path> <session-id> -- <bytes>"
        }
        "sessions shutdown" => {
            "usage: botster-hub sessions shutdown --data-dir <path> <session-id>"
        }
        "shutdown" => "usage: botster-hub shutdown --data-dir <path>",
        "mcp-serve" => "usage: botster-hub mcp-serve --data-dir <path>",
        "tui" => "usage: botster-hub tui --data-dir <path>",
        "packages" => "usage: botster-hub packages <install|list|show|enable|disable|remove> ...",
        "packages install" => {
            "usage: botster-hub packages install --data-dir <path> --path <package-dir-or-manifest>"
        }
        "packages list" => "usage: botster-hub packages list --data-dir <path>",
        "packages show" => "usage: botster-hub packages show --data-dir <path> <name>",
        "packages enable" => {
            "usage: botster-hub packages enable --data-dir <path> (--path <package-dir-or-manifest>|<name>)"
        }
        "packages disable" => "usage: botster-hub packages disable --data-dir <path> <name>",
        "packages remove" => "usage: botster-hub packages remove --data-dir <path> <name>",
        "providers" | "providers list" => "usage: botster-hub providers list --data-dir <path>",
        "inspect" => "usage: botster-hub inspect --data-dir <path> <session-id>",
        _ => {
            "usage: botster-hub <start|dogfood|status|sessions|shutdown|mcp-serve|tui|packages|providers|inspect|run-one>"
        }
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

impl From<botster_hub::DaemonTransportError> for StartError {
    fn from(error: botster_hub::DaemonTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<OperatorError> for StartError {
    fn from(error: OperatorError) -> Self {
        Self::Operator(Box::new(error))
    }
}

impl From<botster_hub::HubConfigError> for DogfoodError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::DaemonTransportError> for DogfoodError {
    fn from(error: botster_hub::DaemonTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<botster_hub::HubConfigError> for OperatorError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<OperatorError> for McpCliError {
    fn from(error: OperatorError) -> Self {
        Self::Usage(Box::new(error))
    }
}

impl From<botster_hub::HubConfigError> for McpCliError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::McpServeError> for McpCliError {
    fn from(error: botster_hub::McpServeError) -> Self {
        Self::Serve(error)
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

impl From<botster_hub::DaemonTransportError> for OperatorError {
    fn from(error: botster_hub::DaemonTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<botster_hub::TuiError> for OperatorError {
    fn from(error: botster_hub::TuiError) -> Self {
        Self::Tui(error)
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

impl From<botster_core_daemon::CoreDaemonError> for RunOneError {
    fn from(error: botster_core_daemon::CoreDaemonError) -> Self {
        Self::Runtime(error.into())
    }
}

impl From<botster_hub::HubStateStoreError> for RunOneError {
    fn from(error: botster_hub::HubStateStoreError) -> Self {
        Self::State(error)
    }
}
