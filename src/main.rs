use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::os::unix::net::UnixListener;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::{
    AesGcmEnvelope, AesGcmKey, ClientId, CoreSessionMetadata, RequestId, ResizePayload, SessionId,
    SessionLifecycleState, SessionSpawnRequest, SpawnEnvironment, SpawnWorkingDirectory,
    SubscriptionId, TransportEgress, decrypt_aes_gcm, encrypt_aes_gcm,
};
use botster_hub::{
    DaemonApp, DaemonCompatibility, DaemonEvent, DaemonOperatorError, DaemonPackage,
    DaemonPackageActionStatus, DaemonPackagePin, DaemonRequest, DaemonResponse, DaemonResponseKind,
    DaemonSession, DaemonSpawnTarget, DaemonStatus, DaemonWorktree, DataDirectoryOption,
    HubClientApi, HubClientRequest, HubClientResponseBody, HubDaemon, HubDaemonState, HubRuntime,
    HubStartupOptions, HubStateLoadSource, RuntimeEnvironment, SessionDefaults, TransportBindings,
    daemon_transport_request, host_profile, serve_daemon, serve_mcp_stdio, stream_attach,
};
use botster_hub_client::{
    DaemonDiagnostic, DaemonLocalWebrtcBootstrap, DaemonLocalWebrtcDeliveryChunk,
    DaemonLocalWebrtcDeliveryKind, DaemonPackageUpdateStatus, LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION,
    LOCAL_WEBRTC_MAX_DELIVERY_BYTES, LOCAL_WEBRTC_MAX_FRAME_BYTES,
};
use serde::{Deserialize, Serialize};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCIceGatheringState,
    RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::{
    Receiver as AsyncReceiver, Sender as AsyncSender, block_on, channel, default_runtime, sleep,
    timeout,
};

const SMOKE_MARKER: &str = "botster-hub-smoke-ok";
const SMOKE_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_RUNTIME_DAEMON_METADATA_FILE: &str = ".botster-hub-runtime-daemon.json";
const LOCAL_RUNTIME_DAEMON_READINESS_BUDGET: Duration = Duration::from_secs(30);
const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE: &str = "local-webrtc-sender-terminal.json";
const LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT: Duration = Duration::from_secs(2);
const TEST_INCOMPATIBLE_DAEMON_ENV: &str = "BOTSTER_HUB_TEST_INCOMPATIBLE_DAEMON";
const TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV: &str =
    "BOTSTER_HUB_TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS";

mod operator_console;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum CommandOutcome {
    Completed,
    DaemonStopped,
    ForegroundAppExited { code: i32, description: String },
}

pub(crate) struct ConsolePackageState {
    package_name: String,
    state: String,
}

fn main() {
    let command = env::args().nth(1);
    if command.is_none() {
        if !should_open_operator_console(io::stdin().is_terminal(), io::stdout().is_terminal()) {
            eprintln!(
                "botster-hub error: interactive operator console requires terminal stdin and stdout; scripts must use an explicit subcommand"
            );
            process::exit(1);
        }
        if let Err(error) = run_operator_console() {
            eprintln!("botster-hub console error: {error}");
            process::exit(1);
        }
        return;
    }

    let command = command.expect("command checked above");
    let command_args = match command.as_str() {
        command if stateful_command(command) => {
            match canonicalize_data_dir_args(command, env::args().skip(2).collect()) {
                Ok(args) => args,
                Err(error) => {
                    eprintln!("botster-hub {command} error: {error}");
                    process::exit(1);
                }
            }
        }
        _ => env::args().skip(2).collect(),
    };

    match dispatch_command(&command, command_args) {
        Ok(CommandOutcome::Completed | CommandOutcome::DaemonStopped) => {}
        Ok(CommandOutcome::ForegroundAppExited { code, .. }) => process::exit(code),
        Err(error) => {
            eprintln!("botster-hub {command} error: {error}");
            process::exit(1);
        }
    }
}

fn should_open_operator_console(stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
    stdin_is_terminal && stdout_is_terminal
}

fn dispatch_command(command: &str, args: Vec<String>) -> Result<CommandOutcome, String> {
    match command {
        "start" => start_daemon(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "up" => local_runtime_up(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "down" => local_runtime_down(args)
            .map(|()| CommandOutcome::DaemonStopped)
            .map_err(|error| error.to_string()),
        "doctor" => local_runtime_doctor(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "smoke" => local_runtime_smoke(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "status" => operator_status(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "sessions" => operator_sessions(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "session-templates" => operator_session_templates(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "spawn-targets" => operator_spawn_targets(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "context" => operator_context(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "shutdown" => operator_shutdown(args)
            .map(|()| CommandOutcome::DaemonStopped)
            .map_err(|error| error.to_string()),
        "mcp-serve" => mcp_serve(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "open" => operator_open_alias(args).map_err(|error| error.to_string()),
        "reload" => operator_reload_alias(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "apps" => operator_apps(args).map_err(|error| error.to_string()),
        "packages" => operator_packages(args, false)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "providers" => operator_packages(args, true)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "inspect" => operator_inspect(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "run-one" => run_one(args)
            .map(|()| CommandOutcome::Completed)
            .map_err(|error| error.to_string()),
        "help" | "--help" | "-h" => {
            print_global_help();
            Ok(CommandOutcome::Completed)
        }
        _ => Err(format!("unknown command\n{}", usage_for(command))),
    }
}

fn run_operator_console() -> Result<(), String> {
    let signals = operator_console::ConsoleSignals::install()
        .map_err(|error| format!("install Ctrl-C handling: {error}"))?;
    signals.begin_startup_or_inline();
    let data_directory = DataDirectoryOption::RuntimeDefault
        .resolve(&RuntimeEnvironment::from_current_process())
        .map_err(|error| error.to_string())?;
    let runtime =
        prepare_operator_console_runtime(data_directory.clone()).map_err(console_start_error)?;
    let daemon_state = match runtime.daemon_ownership {
        LocalRuntimeDaemonOwnership::Started => "started",
        LocalRuntimeDaemonOwnership::Reused => "reused",
    };
    operator_console::print_intro(&data_directory, daemon_state, &runtime.packages);
    operator_console::run(
        data_directory.clone(),
        &signals,
        |selector| resolve_console_app_mode(&runtime.config, selector),
        |command, args| {
            canonicalize_data_dir_args(command, args).map_err(|error| error.to_string())
        },
        dispatch_command,
    )
    .map_err(|error| error.to_string())
}

fn console_start_error(error: LocalRuntimeError) -> String {
    match error {
        LocalRuntimeError::MissingSessionWorkerBinary(path) => format!(
            "missing botster-session-worker binary at {}. Install the complete Botster distribution, or in a source checkout run `cargo build --locked -p botster-core --bin botster-session-worker` so the worker is beside botster-hub, then rerun `botster-hub`",
            path.display()
        ),
        other => other.to_string(),
    }
}

fn resolve_console_app_mode(
    config: &botster_hub::HubConfig,
    selector: &str,
) -> Result<operator_console::CommandMode, String> {
    let response = daemon_transport_request(config, DaemonRequest::ListApps)
        .map_err(|error| error.to_string())?;
    let app = resolve_app_selector(&response.apps, selector).map_err(|error| error.to_string())?;
    if app.kind == "terminal_app" {
        Ok(operator_console::CommandMode::Foreground)
    } else {
        Ok(operator_console::CommandMode::Inline)
    }
}

fn stateful_command(command: &str) -> bool {
    matches!(
        command,
        "start"
            | "up"
            | "down"
            | "doctor"
            | "smoke"
            | "status"
            | "sessions"
            | "session-templates"
            | "spawn-targets"
            | "context"
            | "shutdown"
            | "mcp-serve"
            | "open"
            | "reload"
            | "apps"
            | "packages"
            | "providers"
            | "inspect"
    )
}

fn canonicalize_data_dir_args(
    command: &str,
    args: Vec<String>,
) -> Result<Vec<String>, OperatorError> {
    canonicalize_data_dir_args_for_environment(
        command,
        args,
        &RuntimeEnvironment::from_current_process(),
    )
}

fn canonicalize_data_dir_args_for_environment(
    command: &str,
    args: Vec<String>,
    environment: &RuntimeEnvironment,
) -> Result<Vec<String>, OperatorError> {
    let mut explicit_data_directory = None;
    let mut remaining = Vec::new();
    let mut cursor = 0;
    while cursor < args.len() {
        if args[cursor] == "--" {
            remaining.extend(args[cursor..].iter().cloned());
            break;
        } else if args[cursor] == "--data-dir" {
            if explicit_data_directory.is_some() {
                return Err(OperatorError::Usage(command_usage(command)));
            }
            let Some(value) = args.get(cursor + 1) else {
                return Err(OperatorError::Usage(command_usage(command)));
            };
            explicit_data_directory = Some(PathBuf::from(value));
            cursor += 2;
        } else {
            remaining.push(args[cursor].clone());
            cursor += 1;
        }
    }

    let data_directory = match explicit_data_directory {
        Some(path) => DataDirectoryOption::Explicit(path),
        None => DataDirectoryOption::RuntimeDefault,
    }
    .resolve(environment)?;
    let insert_at = if command == "packages"
        && remaining.first().map(String::as_str) == Some("config")
        && remaining.get(1).map(String::as_str) == Some("set")
    {
        2
    } else if matches!(
        command,
        "sessions"
            | "session-templates"
            | "spawn-targets"
            | "open"
            | "reload"
            | "apps"
            | "packages"
            | "providers"
    ) && !remaining.is_empty()
    {
        1
    } else {
        0
    };
    remaining.splice(
        insert_at..insert_at,
        [
            "--data-dir".to_string(),
            data_directory.to_string_lossy().into_owned(),
        ],
    );
    Ok(remaining)
}

fn command_usage(command: &str) -> &'static str {
    match command {
        "start" => "start",
        "up" => "up",
        "down" => "down",
        "doctor" => "doctor",
        "smoke" => "smoke",
        "status" => "status",
        "sessions" => "sessions",
        "session-templates" => "session-templates",
        "spawn-targets" => "spawn-targets",
        "context" => "context",
        "shutdown" => "shutdown",
        "mcp-serve" => "mcp-serve",
        "open" => "open",
        "reload" => "reload",
        "apps" => "apps",
        "packages" => "packages",
        "providers" => "providers",
        "inspect" => "inspect",
        _ => "global",
    }
}

fn start_daemon(args: Vec<String>) -> Result<(), StartError> {
    let options = StartOptions::parse(args)?;
    let config = explicit_config_with_worker(options.data_directory, options.session_worker_bin)?;

    // Integration tests run the production binary, so keep the incompatible
    // daemon fixture behind both an explicit fixture opt-in and test mode.
    if env::var_os(TEST_INCOMPATIBLE_DAEMON_ENV).is_some()
        && env::var("BOTSTER_ENV").as_deref() == Ok("test")
    {
        return serve_test_incompatible_daemon(&config).map_err(StartError::Transport);
    }

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
        lifecycle_counters: Default::default(),
        diagnostics: Vec::new(),
    };
    print_daemon_transport_status("stopped", &status);

    Ok(())
}

fn local_runtime_up(args: Vec<String>) -> Result<(), LocalRuntimeError> {
    let outcome = prepare_local_runtime(LocalRuntimeOptions::parse(args)?)?;
    let status = daemon_transport_request(&outcome.config, DaemonRequest::Status)?;
    let apps = daemon_transport_request(&outcome.config, DaemonRequest::ListApps)?;
    let status = status
        .status
        .as_ref()
        .ok_or(botster_hub::DaemonTransportError::UnexpectedResponse)?;
    print_local_runtime_ready(&outcome, status, &apps.apps);
    Ok(())
}

fn local_runtime_down(args: Vec<String>) -> Result<(), LocalRuntimeError> {
    let options = DataArgs::parse(args, "down")?;
    if !options.arguments.is_empty() {
        return Err(LocalRuntimeError::Usage);
    }
    let config = explicit_config(options.data_directory.clone())?;
    let owned_daemon_pid = owned_runtime_daemon_pid(&options.data_directory, &config)?;
    let response = match daemon_transport_request(&config, DaemonRequest::DaemonShutdown) {
        Ok(response) => response,
        Err(botster_hub::DaemonTransportError::Compatibility(error)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, &config)? {
                println!("daemon=recovered_stale");
                return Ok(());
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(error.to_string()));
        }
        Err(botster_hub::DaemonTransportError::Protocol(message)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, &config)? {
                println!("daemon=recovered_stale");
                return Ok(());
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(message.to_string()));
        }
        Err(error) => return Err(error.into()),
    };
    print_daemon_response(response)?;
    if let Some(pid) = owned_daemon_pid {
        wait_for_runtime_daemon_exit(pid)?;
        remove_configured_local_socket(&config)?;
        remove_runtime_daemon_metadata(&options.data_directory)?;
    }
    Ok(())
}

fn local_runtime_doctor(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataArgs::parse(args, "doctor")?;
    if !options.arguments.is_empty() {
        return Err(OperatorError::Usage("doctor"));
    }
    let config = explicit_config(options.data_directory.clone())?;
    println!("doctor=local_runtime");
    options.print_source();

    let status_response = match daemon_transport_request(&config, DaemonRequest::Status) {
        Ok(response) => response,
        Err(
            botster_hub::DaemonTransportError::NotRunning
            | botster_hub::DaemonTransportError::ClientDisconnected,
        ) => {
            print_runtime_check(
                "daemon_running",
                RuntimeCheckStatus::Fail,
                "daemon is not running",
            );
            print_remediation(&format!(
                "botster-hub up --data-dir {}",
                options.data_directory.display()
            ));
            return Err(OperatorError::DaemonNotRunning);
        }
        Err(botster_hub::DaemonTransportError::Compatibility(error)) => {
            print_runtime_check(
                "daemon_compatible",
                RuntimeCheckStatus::Fail,
                "running daemon is incompatible or stale",
            );
            print_daemon_diagnostics(&error.diagnostics);
            print_remediation(&format!(
                "stop the stale botster-hub process, remove the stale local socket if needed, then run botster-hub up --data-dir {}",
                options.data_directory.display()
            ));
            return Err(OperatorError::App(error.to_string()));
        }
        Err(botster_hub::DaemonTransportError::Protocol(message)) => {
            print_runtime_check(
                "daemon_compatible",
                RuntimeCheckStatus::Fail,
                "running daemon did not speak the current protocol",
            );
            print_remediation(&format!(
                "stop the stale botster-hub process, remove the stale local socket if needed, then run botster-hub up --data-dir {}",
                options.data_directory.display()
            ));
            return Err(OperatorError::App(message.to_string()));
        }
        Err(error) => return Err(error.into()),
    };

    let status = status_response
        .status
        .as_ref()
        .ok_or(OperatorError::UnexpectedResponse("status"))?;
    print_runtime_check(
        "daemon_running",
        RuntimeCheckStatus::Pass,
        "daemon socket answered",
    );
    print_runtime_check(
        "daemon_compatible",
        RuntimeCheckStatus::Pass,
        &format!(
            "protocol={} protocol_version={} conformance_fixture_revision={}",
            status.compatibility.protocol,
            status.compatibility.protocol_version,
            status.compatibility.conformance_fixture_revision
        ),
    );
    print_runtime_check(
        "core_initialized",
        if status.core_initialized {
            RuntimeCheckStatus::Pass
        } else {
            RuntimeCheckStatus::Fail
        },
        &format!("core_initialized={}", status.core_initialized),
    );
    print_runtime_check(
        "package_registry",
        RuntimeCheckStatus::Pass,
        &format!(
            "package_count={} enabled_package_count={} provider_count={} enabled_provider_count={}",
            status.package_count,
            status.enabled_package_count,
            status.provider_count,
            status.enabled_provider_count
        ),
    );
    print_daemon_diagnostics(&status_response.diagnostics);
    print_daemon_diagnostics(&status.diagnostics);

    let packages = daemon_transport_request(&config, DaemonRequest::ListPackages)?;
    print_runtime_check(
        "packages_list",
        RuntimeCheckStatus::Pass,
        &format!("package_count={}", packages.packages.len()),
    );
    print_first_party_package_check(&packages.packages, "project-pipelines");
    print_first_party_package_check(&packages.packages, "botster-web");
    print_first_party_package_check(&packages.packages, "botster-tui");
    print_first_party_package_check(&packages.packages, "botster-workspaces");

    let apps = daemon_transport_request(&config, DaemonRequest::ListApps)?;
    let web_app = apps
        .apps
        .iter()
        .find(|app| app.package_name == "botster-web" && app.entrypoint_id == "web-client");
    match web_app {
        Some(app) if app.lifecycle_state == "running" && app.launch_target.local_url.is_some() => {
            print_runtime_check(
                "botster_web_app",
                RuntimeCheckStatus::Pass,
                "botster-web web-client is running with structured local_url",
            );
        }
        Some(app) => {
            print_runtime_check(
                "botster_web_app",
                RuntimeCheckStatus::Warn,
                &format!(
                    "botster-web web-client lifecycle_state={} local_url={}",
                    app.lifecycle_state,
                    app.launch_target.local_url.as_deref().unwrap_or("none")
                ),
            );
            print_remediation(&format!(
                "botster-hub up --data-dir {}",
                options.data_directory.display()
            ));
        }
        None => {
            print_runtime_check(
                "botster_web_app",
                RuntimeCheckStatus::Warn,
                "botster-web web-client app is not installed",
            );
            print_remediation(&format!(
                "install and enable botster-web, then run botster-hub up --data-dir {}",
                options.data_directory.display()
            ));
        }
    }

    if status.core_initialized {
        Ok(())
    } else {
        Err(OperatorError::App("core is not initialized".to_string()))
    }
}

fn local_runtime_smoke(args: Vec<String>) -> Result<(), SmokeError> {
    let options = SmokeOptions::parse(args)?;
    println!("smoke=local_runtime");
    println!(
        "data_dir=resolved:{}",
        options.runtime.data_directory.display()
    );
    let outcome = prepare_local_runtime(options.runtime)?;
    let _cleanup = SmokeRuntimeCleanup::new(&outcome);
    print_runtime_check(
        "daemon",
        RuntimeCheckStatus::Pass,
        match outcome.daemon_ownership {
            LocalRuntimeDaemonOwnership::Started => "daemon started",
            LocalRuntimeDaemonOwnership::Reused => "daemon reused",
        },
    );

    let status = daemon_transport_request(&outcome.config, DaemonRequest::Status)?;
    let status = status
        .status
        .as_ref()
        .ok_or(SmokeError::UnexpectedResponse("status"))?;
    print_runtime_check(
        "core",
        if status.core_initialized {
            RuntimeCheckStatus::Pass
        } else {
            RuntimeCheckStatus::Fail
        },
        &format!("core_initialized={}", status.core_initialized),
    );

    let packages = daemon_transport_request(&outcome.config, DaemonRequest::ListPackages)?;
    require_smoke_package(&packages.packages, "botster-web")?;
    require_smoke_package(&packages.packages, "botster-tui")?;
    print_runtime_check(
        "packages",
        RuntimeCheckStatus::Pass,
        &format!("enabled_package_count={}", status.enabled_package_count),
    );

    let apps = daemon_transport_request(&outcome.config, DaemonRequest::ListApps)?;
    let web_app = apps
        .apps
        .iter()
        .find(|app| app.package_name == "botster-web" && app.entrypoint_id == "web-client")
        .ok_or(SmokeError::MissingPrerequisite(
            "botster-web web-client app",
        ))?;
    if web_app.lifecycle_state != "running" || web_app.launch_target.local_url.is_none() {
        return Err(SmokeError::MissingPrerequisite(
            "botster-web running local_url",
        ));
    }
    print_runtime_check(
        "apps",
        RuntimeCheckStatus::Pass,
        "botster-web web-client running",
    );

    smoke_session_round_trip(&outcome.config)?;
    print_runtime_check(
        "session_terminal",
        RuntimeCheckStatus::Pass,
        "spawn stream-attach marker shutdown succeeded",
    );

    let local_url =
        web_app
            .launch_target
            .local_url
            .as_deref()
            .ok_or(SmokeError::MissingPrerequisite(
                "botster-web running local_url",
            ))?;
    let origin = runtime_origin(local_url).ok_or(SmokeError::MissingPrerequisite(
        "botster-web local_url origin",
    ))?;
    let bootstrap = daemon_transport_request(
        &outcome.config,
        DaemonRequest::IssueLocalWebrtcBootstrap {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            origin,
        },
    )?;
    let Some(bootstrap) = bootstrap.local_webrtc_bootstrap.as_ref() else {
        return Err(SmokeError::MissingPrerequisite(
            "botster-web local WebRTC bootstrap",
        ));
    };
    if bootstrap.data_plane != "webrtc_data_channel" {
        return Err(SmokeError::MissingPrerequisite("webrtc_data_channel"));
    }
    println!("local_webrtc_grant_id={}", bootstrap.grant_id);
    smoke_local_webrtc_round_trip(&outcome.config, bootstrap)?;
    print_runtime_check(
        "webrtc",
        RuntimeCheckStatus::Pass,
        "encrypted local WebRTC data-channel terminal round trip succeeded",
    );
    println!("smoke_result=pass");
    Ok(())
}

fn smoke_session_round_trip(config: &botster_hub::HubConfig) -> Result<(), SmokeError> {
    let session_id = format!(
        "smoke-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SmokeError::Clock)?
            .as_nanos()
    );
    let subscription_id = format!("{session_id}-subscription");
    let marker = "botster-smoke-terminal-ok";
    let spawn = daemon_transport_request(
        config,
        DaemonRequest::Spawn {
            session_id: session_id.clone(),
            command: format!("printf 'smoke:{marker}\\n'"),
        },
    )?;
    if spawn.kind == DaemonResponseKind::OperatorError {
        return Err(SmokeError::OperatorResponse(operator_response_message(
            &spawn,
        )));
    }
    let mut observed = Vec::new();
    stream_attach(
        config,
        SessionId(session_id.clone()),
        SubscriptionId(subscription_id),
        &mut observed,
    )
    .map_err(SmokeError::Transport)?;
    let observed = String::from_utf8_lossy(&observed).to_string();
    if observed.contains(&format!("smoke:{marker}")) {
        let _ = daemon_transport_request(config, DaemonRequest::ShutdownSession { session_id });
        return Ok(());
    }

    let _ = daemon_transport_request(config, DaemonRequest::ShutdownSession { session_id });
    Err(SmokeError::SessionRoundTrip(observed))
}

fn operator_response_message(response: &DaemonResponse) -> String {
    response
        .error
        .as_ref()
        .map(|error| error.message.clone())
        .unwrap_or_else(|| "operator error".to_string())
}

fn smoke_local_webrtc_round_trip(
    config: &botster_hub::HubConfig,
    bootstrap: &DaemonLocalWebrtcBootstrap,
) -> Result<(), SmokeError> {
    let stream_key = local_webrtc_stream_key(&bootstrap.grant_secret)?;
    let result = block_on(async {
        let (mut offer_peer, offer) = LocalWebrtcOfferPeer::create_offer().await?;
        let signal = daemon_transport_request(
            config,
            DaemonRequest::LocalWebrtcSignal {
                grant_id: bootstrap.grant_id.clone(),
                grant_secret: bootstrap.grant_secret.clone(),
                origin: bootstrap.expected_origin.clone(),
                offer,
            },
        )?;
        let answer = signal
            .local_webrtc_answer
            .as_ref()
            .ok_or_else(|| SmokeError::Webrtc("missing local WebRTC answer".to_string()))?
            .answer
            .clone();
        offer_peer.accept_answer(answer).await?;
        offer_peer
            .encrypted_request(&stream_key, &DaemonRequest::Status)
            .await?;

        let session_id = "smoke-local-webrtc-session".to_string();
        let subscription_id = "smoke-local-webrtc-subscription".to_string();
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::Spawn {
                    session_id: session_id.clone(),
                    command: "printf 'webrtc-smoke-ready\\n'; while IFS= read -r line; do printf 'webrtc:%s\\n' \"$line\"; done".to_string(),
                },
            )
            .await?;
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::Attach {
                    session_id: session_id.clone(),
                    subscription_id,
                },
            )
            .await?;
        offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::SendInput {
                    session_id: session_id.clone(),
                    data: "from-smoke-webrtc\n".to_string(),
                },
            )
            .await?;
        let mut observed = String::new();
        for _ in 0..120 {
            let drain = offer_peer
                .encrypted_request(
                    &stream_key,
                    &DaemonRequest::Drain {
                        session_id: session_id.clone(),
                    },
                )
                .await?;
            for event in drain.events {
                if let DaemonEvent::TerminalOutput { data, .. } = event {
                    observed.push_str(&data);
                }
            }
            if observed.contains("webrtc:from-smoke-webrtc") {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
        let _ = offer_peer
            .encrypted_request(
                &stream_key,
                &DaemonRequest::ShutdownSession {
                    session_id: session_id.clone(),
                },
            )
            .await;
        let _ = offer_peer.peer.close().await;
        if observed.contains("webrtc:from-smoke-webrtc") {
            Ok(())
        } else {
            Err(SmokeError::Webrtc(format!(
                "local WebRTC terminal marker not observed; observed_bytes={}",
                observed.len()
            )))
        }
    });
    if result.is_err() {
        wait_for_local_webrtc_sender_terminal_record(config, &bootstrap.grant_id);
    }
    result
}

fn wait_for_local_webrtc_sender_terminal_record(
    config: &botster_hub::HubConfig,
    expected_grant_id: &str,
) {
    let path = config
        .data_directory
        .join(LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE);
    let deadline = Instant::now() + LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_WAIT;
    loop {
        if std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|record| {
                record
                    .get("grant_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .as_deref()
            == Some(expected_grant_id)
        {
            return;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

struct LocalWebrtcOffererHandler {
    gather_complete_tx: AsyncSender<()>,
    connected_tx: AsyncSender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for LocalWebrtcOffererHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state == RTCPeerConnectionState::Connected {
            let _ = self.connected_tx.try_send(());
        }
    }
}

struct LocalWebrtcOfferPeer {
    peer: Box<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
    connected_rx: AsyncReceiver<()>,
    data_channel_open_rx: AsyncReceiver<()>,
    data_channel_message_rx: AsyncReceiver<String>,
}

impl LocalWebrtcOfferPeer {
    async fn create_offer() -> Result<(Self, serde_json::Value), SmokeError> {
        let runtime = default_runtime()
            .ok_or_else(|| SmokeError::Webrtc("no async runtime found".to_string()))?;
        let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
        let (connected_tx, connected_rx) = channel::<()>(1);
        let (data_channel_open_tx, data_channel_open_rx) = channel::<()>(1);
        let (data_channel_message_tx, data_channel_message_rx) = channel::<String>(256);
        let handler = Arc::new(LocalWebrtcOffererHandler {
            gather_complete_tx,
            connected_tx,
        });
        let peer = PeerConnectionBuilder::new()
            .with_handler(handler)
            .with_runtime(runtime.clone())
            .with_udp_addrs(vec!["127.0.0.1:0".to_string()])
            .build()
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let data_channel = peer
            .create_data_channel(
                "botster-client",
                Some(RTCDataChannelInit {
                    ordered: true,
                    max_retransmits: None,
                    max_packet_life_time: None,
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;

        {
            let data_channel = data_channel.clone();
            let open_tx = data_channel_open_tx.clone();
            let message_tx = data_channel_message_tx.clone();
            runtime.spawn(Box::pin(async move {
                while let Some(event) = data_channel.poll().await {
                    match event {
                        DataChannelEvent::OnOpen => {
                            let _ = open_tx.try_send(());
                        }
                        DataChannelEvent::OnMessage(message) => {
                            if let Ok(text) = String::from_utf8(message.data.to_vec()) {
                                let _ = message_tx.try_send(text);
                            }
                        }
                        DataChannelEvent::OnClose => break,
                        _ => {}
                    }
                }
            }));
        }

        let offer = peer
            .create_offer(None)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        peer.set_local_description(offer)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let _ = timeout(Duration::from_secs(5), gather_complete_rx.recv()).await;
        let offer = peer
            .local_description()
            .await
            .ok_or_else(|| SmokeError::Webrtc("offer local description missing".to_string()))?;
        let offer =
            serde_json::to_value(offer).map_err(|error| SmokeError::Webrtc(error.to_string()))?;

        Ok((
            Self {
                peer: Box::new(peer),
                data_channel,
                connected_rx,
                data_channel_open_rx,
                data_channel_message_rx,
            },
            offer,
        ))
    }

    async fn accept_answer(&mut self, answer: serde_json::Value) -> Result<(), SmokeError> {
        let answer = serde_json::from_value::<RTCSessionDescription>(answer)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        self.peer
            .set_remote_description(answer)
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        timeout(Duration::from_secs(15), self.connected_rx.recv())
            .await
            .map_err(|_| {
                SmokeError::Webrtc("timed out waiting for WebRTC connection".to_string())
            })?;
        timeout(Duration::from_secs(10), self.data_channel_open_rx.recv())
            .await
            .map_err(|_| {
                SmokeError::Webrtc("timed out waiting for data channel open".to_string())
            })?;
        Ok(())
    }

    async fn encrypted_request(
        &mut self,
        key: &AesGcmKey,
        request: &DaemonRequest,
    ) -> Result<DaemonResponse, SmokeError> {
        let operation = smoke_local_webrtc_request_operation(request);
        let plaintext =
            serde_json::to_vec(request).map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let envelope = encrypt_aes_gcm(key, &plaintext, 1)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        self.data_channel
            .send_text(
                &serde_json::to_string(&envelope)
                    .map_err(|error| SmokeError::Webrtc(error.to_string()))?,
            )
            .await
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let mut encrypted = String::new();
        let mut message_id = None;
        let mut chunk_count = None;
        let mut next_chunk_index = 0;
        loop {
            let response = timeout(Duration::from_secs(10), self.data_channel_message_rx.recv())
                .await
                .map_err(|_| {
                    SmokeError::Webrtc(local_webrtc_response_progress_error(
                        operation,
                        "response_timeout",
                        message_id.as_deref(),
                        next_chunk_index,
                        chunk_count,
                    ))
                })?
                .ok_or_else(|| {
                    SmokeError::Webrtc(local_webrtc_response_progress_error(
                        operation,
                        "channel_closed",
                        message_id.as_deref(),
                        next_chunk_index,
                        chunk_count,
                    ))
                })?;
            if response.len() >= LOCAL_WEBRTC_MAX_FRAME_BYTES {
                return Err(SmokeError::Webrtc(
                    "local WebRTC response chunk exceeded frame bound".to_string(),
                ));
            }
            let chunk = serde_json::from_str::<DaemonLocalWebrtcDeliveryChunk>(&response)
                .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
            if chunk.version != LOCAL_WEBRTC_DELIVERY_CHUNK_VERSION
                || chunk.delivery_kind != DaemonLocalWebrtcDeliveryKind::DaemonResponse
                || chunk.chunk_index != next_chunk_index
                || chunk.total_bytes as usize > LOCAL_WEBRTC_MAX_DELIVERY_BYTES
                || message_id
                    .as_ref()
                    .is_some_and(|id| id != &chunk.message_id)
                || chunk_count.is_some_and(|count| count != chunk.chunk_count)
            {
                return Err(SmokeError::Webrtc(
                    "invalid local WebRTC response chunk sequence".to_string(),
                ));
            }
            message_id.get_or_insert(chunk.message_id);
            chunk_count.get_or_insert(chunk.chunk_count);
            encrypted.push_str(&chunk.payload);
            next_chunk_index += 1;
            if chunk.chunk_index + 1 == chunk.chunk_count {
                if encrypted.len() != chunk.total_bytes as usize {
                    return Err(SmokeError::Webrtc(
                        "local WebRTC response byte count mismatch".to_string(),
                    ));
                }
                break;
            }
        }
        let envelope = serde_json::from_str::<AesGcmEnvelope>(&encrypted)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        let plaintext = decrypt_aes_gcm(key, &envelope)
            .map_err(|error| SmokeError::Webrtc(error.to_string()))?;
        serde_json::from_slice(&plaintext).map_err(|error| SmokeError::Webrtc(error.to_string()))
    }
}

fn smoke_local_webrtc_request_operation(request: &DaemonRequest) -> &'static str {
    match request {
        DaemonRequest::Status => "status",
        DaemonRequest::Spawn { .. } => "spawn",
        DaemonRequest::Attach { .. } => "attach",
        DaemonRequest::SendInput { .. } => "send_input",
        DaemonRequest::Drain { .. } => "drain",
        DaemonRequest::ShutdownSession { .. } => "shutdown_session",
        _ => "other",
    }
}

fn local_webrtc_response_progress_error(
    operation: &str,
    cause: &str,
    message_id: Option<&str>,
    next_chunk_index: u32,
    expected_chunk_count: Option<u32>,
) -> String {
    format!(
        "local WebRTC response incomplete: operation={operation} cause={cause} message_id={} next_chunk={} expected_chunks={}",
        message_id.unwrap_or("pending"),
        next_chunk_index,
        expected_chunk_count.map_or_else(|| "pending".to_string(), |count| count.to_string()),
    )
}

fn local_webrtc_stream_key(secret: &str) -> Result<AesGcmKey, SmokeError> {
    let hex = secret
        .strip_prefix("secret-")
        .ok_or_else(|| SmokeError::Webrtc("local WebRTC secret prefix missing".to_string()))?;
    let bytes = decode_hex_bytes(hex)
        .ok_or_else(|| SmokeError::Webrtc("local WebRTC secret hex invalid".to_string()))?;
    AesGcmKey::from_slice(&bytes).map_err(|error| SmokeError::Webrtc(error.to_string()))
}

fn decode_hex_bytes(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum RuntimeCheckStatus {
    Pass,
    Warn,
    Fail,
}

impl RuntimeCheckStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

fn print_runtime_check(name: &str, status: RuntimeCheckStatus, message: &str) {
    println!(
        "check name={name} status={} message={}",
        status.as_str(),
        sanitize_runtime_message(message)
    );
}

fn print_remediation(command: &str) {
    println!("remediation={}", sanitize_runtime_message(command));
}

fn print_daemon_diagnostics(diagnostics: &[DaemonDiagnostic]) {
    for diagnostic in diagnostics {
        println!(
            "diagnostic kind={:?} operation={} feature={} message={}",
            diagnostic.kind,
            diagnostic.operation.as_deref().unwrap_or("none"),
            diagnostic.feature.as_deref().unwrap_or("none"),
            sanitize_runtime_message(diagnostic.message.as_deref().unwrap_or("none"))
        );
    }
}

fn print_first_party_package_check(packages: &[DaemonPackage], name: &'static str) {
    match packages.iter().find(|package| package.package_name == name) {
        Some(package) if package.state == "enabled" => {
            print_runtime_check(
                &format!("package_{name}"),
                RuntimeCheckStatus::Pass,
                "package is enabled",
            );
        }
        Some(package) => {
            print_runtime_check(
                &format!("package_{name}"),
                RuntimeCheckStatus::Warn,
                &format!("package state={}", package.state),
            );
        }
        None => {
            print_runtime_check(
                &format!("package_{name}"),
                RuntimeCheckStatus::Warn,
                "package is not installed",
            );
        }
    }
}

fn require_smoke_package(packages: &[DaemonPackage], name: &'static str) -> Result<(), SmokeError> {
    let package = packages
        .iter()
        .find(|package| package.package_name == name)
        .ok_or(SmokeError::MissingPrerequisite(name))?;
    if package.state != "enabled" {
        return Err(SmokeError::MissingPrerequisite(name));
    }
    Ok(())
}

fn sanitize_runtime_message(message: &str) -> String {
    message.replace(['\n', '\r'], " ")
}

struct LocalRuntimeOutcome {
    options: LocalRuntimeOptions,
    config: botster_hub::HubConfig,
    daemon_ownership: LocalRuntimeDaemonOwnership,
    web: RuntimeWebLaunch,
}

struct OperatorConsoleRuntime {
    config: botster_hub::HubConfig,
    daemon_ownership: LocalRuntimeDaemonOwnership,
    packages: Vec<ConsolePackageState>,
}

struct SmokeRuntimeCleanup<'a> {
    outcome: &'a LocalRuntimeOutcome,
}

struct StartedRuntimeCleanup<'a> {
    config: &'a botster_hub::HubConfig,
    armed: bool,
}

impl<'a> StartedRuntimeCleanup<'a> {
    fn new(
        config: &'a botster_hub::HubConfig,
        daemon_ownership: LocalRuntimeDaemonOwnership,
    ) -> Self {
        Self {
            config,
            armed: matches!(daemon_ownership, LocalRuntimeDaemonOwnership::Started),
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartedRuntimeCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let metadata = read_runtime_daemon_metadata(&self.config.data_directory)
                .ok()
                .flatten();
            let _ = daemon_transport_request(self.config, DaemonRequest::DaemonShutdown);
            if let Some(metadata) = metadata {
                let _ = wait_for_runtime_daemon_exit(metadata.pid);
            }
            let _ = remove_configured_local_socket(self.config);
            let _ = remove_runtime_daemon_metadata(&self.config.data_directory);
        }
    }
}

impl<'a> SmokeRuntimeCleanup<'a> {
    fn new(outcome: &'a LocalRuntimeOutcome) -> Self {
        Self { outcome }
    }
}

impl Drop for SmokeRuntimeCleanup<'_> {
    fn drop(&mut self) {
        if !matches!(
            self.outcome.daemon_ownership,
            LocalRuntimeDaemonOwnership::Started
        ) {
            return;
        }
        let _ = daemon_transport_request(
            &self.outcome.config,
            DaemonRequest::StopPackageEntrypoint {
                package_name: "botster-web".to_string(),
                entrypoint_id: "web-client".to_string(),
            },
        );
        let _ = daemon_transport_request(&self.outcome.config, DaemonRequest::DaemonShutdown);
    }
}

fn prepare_local_runtime(
    options: LocalRuntimeOptions,
) -> Result<LocalRuntimeOutcome, LocalRuntimeError> {
    std::fs::create_dir_all(&options.data_directory).map_err(|source| {
        LocalRuntimeError::CreateDataDir {
            path: options.data_directory.clone(),
            source,
        }
    })?;

    let hub_bin = env::current_exe().map_err(LocalRuntimeError::CurrentExe)?;
    let config = explicit_config(options.data_directory.clone())?;
    let daemon_ownership = ensure_local_runtime_daemon(&hub_bin, &options, &config)?;
    let mut started_runtime_cleanup = StartedRuntimeCleanup::new(&config, daemon_ownership);
    daemon_transport_request(&config, DaemonRequest::RefreshLocalPackages)?;
    let packages = daemon_transport_request(&config, DaemonRequest::ListPackages)?;
    require_runtime_package(&packages.packages, "botster-web")?;
    require_runtime_package(&packages.packages, "botster-tui")?;
    let web = start_installed_web_app(&config, &packages.packages)?;

    started_runtime_cleanup.disarm();
    drop(started_runtime_cleanup);
    Ok(LocalRuntimeOutcome {
        options,
        config,
        daemon_ownership,
        web,
    })
}

fn prepare_operator_console_runtime(
    data_directory: PathBuf,
) -> Result<OperatorConsoleRuntime, LocalRuntimeError> {
    std::fs::create_dir_all(&data_directory).map_err(|source| {
        LocalRuntimeError::CreateDataDir {
            path: data_directory.clone(),
            source,
        }
    })?;
    let hub_bin = env::current_exe().map_err(LocalRuntimeError::CurrentExe)?;
    let options = LocalRuntimeOptions {
        data_directory,
        session_worker_bin: None,
    };
    let config = explicit_config(options.data_directory.clone())?;
    let daemon_ownership = ensure_local_runtime_daemon(&hub_bin, &options, &config)?;
    let mut started_runtime_cleanup = StartedRuntimeCleanup::new(&config, daemon_ownership);
    let packages = daemon_transport_request(&config, DaemonRequest::ListPackages)?
        .packages
        .into_iter()
        .map(|package| ConsolePackageState {
            package_name: package.package_name,
            state: package.state,
        })
        .collect();
    started_runtime_cleanup.disarm();
    drop(started_runtime_cleanup);
    Ok(OperatorConsoleRuntime {
        config,
        daemon_ownership,
        packages,
    })
}

fn ensure_local_runtime_daemon(
    hub_bin: &Path,
    options: &LocalRuntimeOptions,
    config: &botster_hub::HubConfig,
) -> Result<LocalRuntimeDaemonOwnership, LocalRuntimeError> {
    match daemon_transport_request(config, DaemonRequest::Status) {
        Ok(_) => return Ok(LocalRuntimeDaemonOwnership::Reused),
        Err(
            botster_hub::DaemonTransportError::NotRunning
            | botster_hub::DaemonTransportError::ClientDisconnected,
        ) => {}
        Err(botster_hub::DaemonTransportError::Compatibility(error)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, config)? {
                return spawn_local_runtime_daemon(hub_bin, options, config);
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(error.to_string()));
        }
        Err(botster_hub::DaemonTransportError::Protocol(message)) => {
            if recover_owned_stale_runtime_daemon(&options.data_directory, config)? {
                return spawn_local_runtime_daemon(hub_bin, options, config);
            }
            return Err(LocalRuntimeError::IncompatibleDaemon(message.to_string()));
        }
        Err(error) => return Err(error.into()),
    }

    spawn_local_runtime_daemon(hub_bin, options, config)
}

fn spawn_local_runtime_daemon(
    hub_bin: &Path,
    options: &LocalRuntimeOptions,
    config: &botster_hub::HubConfig,
) -> Result<LocalRuntimeDaemonOwnership, LocalRuntimeError> {
    if !hub_bin.is_file() {
        return Err(LocalRuntimeError::MissingHubBinary(hub_bin.to_path_buf()));
    }
    let session_worker_bin = options.session_worker_bin(hub_bin)?;

    let mut command = Command::new(hub_bin);
    command
        .arg("start")
        .arg("--data-dir")
        .arg(&options.data_directory)
        .arg("--session-worker-bin")
        .arg(session_worker_bin)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    unsafe {
        // SAFETY: this hook runs in the daemon child after fork and only creates a new process
        // group, keeping terminal-generated signals scoped away from the operator console.
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|source| LocalRuntimeError::SpawnDaemon {
            path: hub_bin.to_path_buf(),
            source,
        })?;
    let (stderr_tx, stderr_rx) = mpsc::channel();
    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = stderr_tx.send(line);
            }
        });
    }

    if let Err(error) =
        write_runtime_daemon_metadata(&options.data_directory, config, hub_bin, child.id())
    {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }

    if let Err(error) = wait_for_local_runtime_ready(
        config,
        &mut child,
        local_runtime_daemon_readiness_budget(),
        &stderr_rx,
    ) {
        let _ = remove_runtime_daemon_metadata(&options.data_directory);
        let _ = remove_configured_local_socket(config);
        return Err(error);
    }
    Ok(LocalRuntimeDaemonOwnership::Started)
}

fn wait_for_local_runtime_ready(
    config: &botster_hub::HubConfig,
    child: &mut Child,
    readiness_budget: Duration,
    stderr_rx: &mpsc::Receiver<String>,
) -> Result<(), LocalRuntimeError> {
    let started_at = Instant::now();
    let deadline = started_at + readiness_budget;
    let mut last_probe = "status probe not attempted".to_string();
    let mut stderr_tail = String::new();
    while Instant::now() < deadline {
        drain_runtime_stderr(stderr_rx, &mut stderr_tail);
        if let Some(status) = child.try_wait().map_err(LocalRuntimeError::PollDaemon)? {
            thread::sleep(Duration::from_millis(20));
            drain_runtime_stderr(stderr_rx, &mut stderr_tail);
            return Err(LocalRuntimeError::DaemonExited {
                status: status.to_string(),
                elapsed: started_at.elapsed(),
                readiness_budget,
                last_probe,
                stderr_tail,
            });
        }
        match daemon_transport_request(config, DaemonRequest::Status) {
            Ok(_) => return Ok(()),
            Err(error) => last_probe = error.to_string(),
        }
        thread::sleep(Duration::from_millis(50));
    }

    let child_pid = child.id();
    let child_status = terminate_owned_runtime_child(child)?;
    Err(LocalRuntimeError::ReadinessTimeout {
        elapsed: started_at.elapsed(),
        readiness_budget,
        last_probe,
        child_pid,
        child_status,
    })
}

fn drain_runtime_stderr(stderr_rx: &mpsc::Receiver<String>, stderr_tail: &mut String) {
    for line in stderr_rx.try_iter() {
        if !stderr_tail.is_empty() {
            stderr_tail.push(' ');
        }
        stderr_tail.push_str(&sanitize_runtime_message(&line));
        if stderr_tail.len() > 8_192 {
            let keep_from = stderr_tail.len() - 8_192;
            stderr_tail.drain(..keep_from);
        }
    }
}

fn local_runtime_daemon_readiness_budget() -> Duration {
    if env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && let Some(milliseconds) = env::var_os(TEST_LOCAL_RUNTIME_READINESS_BUDGET_MS_ENV)
            .and_then(|value| value.to_str().and_then(|value| value.parse::<u64>().ok()))
    {
        return Duration::from_millis(milliseconds);
    }
    LOCAL_RUNTIME_DAEMON_READINESS_BUDGET
}

fn terminate_owned_runtime_child(child: &mut Child) -> Result<String, LocalRuntimeError> {
    if let Some(status) = child.try_wait().map_err(LocalRuntimeError::PollDaemon)? {
        return Ok(status.to_string());
    }
    child.kill().map_err(LocalRuntimeError::TerminateDaemon)?;
    child
        .wait()
        .map(|status| status.to_string())
        .map_err(LocalRuntimeError::TerminateDaemon)
}

#[derive(Debug, Deserialize, Serialize)]
struct LocalRuntimeDaemonMetadata {
    pid: u32,
    data_directory: String,
    #[serde(default)]
    data_directory_arg: Option<String>,
    socket_path: String,
    hub_bin: String,
}

fn recover_owned_stale_runtime_daemon(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<bool, LocalRuntimeError> {
    let Some(metadata) = read_runtime_daemon_metadata(data_directory)? else {
        return Ok(false);
    };
    if !runtime_daemon_metadata_matches(&metadata, data_directory, config)? {
        return Ok(false);
    }
    let Some(command) = process_command(metadata.pid)? else {
        return Ok(false);
    };
    if !runtime_daemon_command_matches(&metadata, &command) {
        return Ok(false);
    }

    terminate_process(metadata.pid)?;
    wait_for_runtime_daemon_exit(metadata.pid)?;
    remove_configured_local_socket(config)?;
    remove_runtime_daemon_metadata(data_directory)?;
    Ok(true)
}

fn owned_runtime_daemon_pid(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<Option<u32>, LocalRuntimeError> {
    let Some(metadata) = read_runtime_daemon_metadata(data_directory)? else {
        return Ok(None);
    };
    if !runtime_daemon_metadata_matches(&metadata, data_directory, config)? {
        return Ok(None);
    }
    let Some(command) = process_command(metadata.pid)? else {
        return Ok(None);
    };
    if !runtime_daemon_command_matches(&metadata, &command) {
        return Ok(None);
    }
    Ok(Some(metadata.pid))
}

fn write_runtime_daemon_metadata(
    data_directory: &Path,
    config: &botster_hub::HubConfig,
    hub_bin: &Path,
    pid: u32,
) -> Result<(), LocalRuntimeError> {
    let metadata = LocalRuntimeDaemonMetadata {
        pid,
        data_directory: stable_path_string(data_directory),
        data_directory_arg: Some(data_directory.display().to_string()),
        socket_path: configured_local_socket_path(config)?.display().to_string(),
        hub_bin: stable_path_string(hub_bin),
    };
    let bytes =
        serde_json::to_vec_pretty(&metadata).map_err(LocalRuntimeError::SerializeMetadata)?;
    std::fs::write(runtime_daemon_metadata_path(data_directory), bytes).map_err(|source| {
        LocalRuntimeError::WriteDaemonMetadata {
            path: runtime_daemon_metadata_path(data_directory),
            source,
        }
    })
}

fn read_runtime_daemon_metadata(
    data_directory: &Path,
) -> Result<Option<LocalRuntimeDaemonMetadata>, LocalRuntimeError> {
    let path = runtime_daemon_metadata_path(data_directory);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(LocalRuntimeError::ReadDaemonMetadata { path, source }),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalRuntimeError::ReadDaemonMetadataJson)
}

fn runtime_daemon_metadata_matches(
    metadata: &LocalRuntimeDaemonMetadata,
    data_directory: &Path,
    config: &botster_hub::HubConfig,
) -> Result<bool, LocalRuntimeError> {
    Ok(
        metadata.data_directory == stable_path_string(data_directory)
            && metadata.socket_path == configured_local_socket_path(config)?.display().to_string(),
    )
}

fn runtime_daemon_command_matches(metadata: &LocalRuntimeDaemonMetadata, command: &str) -> bool {
    let hub_bin_name = Path::new(&metadata.hub_bin)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("botster-hub");
    // PID reuse cannot be proven away with macOS process-table primitives alone.
    // Recovery therefore treats the live PID's command line as required ownership
    // evidence and refuses to signal when any recorded daemon token is missing.
    command.contains(hub_bin_name)
        && command.contains(" start ")
        && command.contains("--data-dir")
        && (command.contains(&metadata.data_directory)
            || metadata
                .data_directory_arg
                .as_ref()
                .is_some_and(|argument| command.contains(argument)))
}

fn configured_local_socket_path(
    config: &botster_hub::HubConfig,
) -> Result<PathBuf, LocalRuntimeError> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(LocalRuntimeError::MissingLocalSocket)
}

fn remove_configured_local_socket(
    config: &botster_hub::HubConfig,
) -> Result<(), LocalRuntimeError> {
    let socket_path = configured_local_socket_path(config)?;
    match std::fs::remove_file(&socket_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LocalRuntimeError::RemoveLocalSocket {
            path: socket_path,
            source,
        }),
    }
}

fn remove_runtime_daemon_metadata(data_directory: &Path) -> Result<(), LocalRuntimeError> {
    let path = runtime_daemon_metadata_path(data_directory);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LocalRuntimeError::RemoveDaemonMetadata { path, source }),
    }
}

fn runtime_daemon_metadata_path(data_directory: &Path) -> PathBuf {
    data_directory.join(LOCAL_RUNTIME_DAEMON_METADATA_FILE)
}

fn stable_path_string(path: &Path) -> String {
    std::fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn process_command(pid: u32) -> Result<Option<String>, LocalRuntimeError> {
    let output = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("command=")
        .output()
        .map_err(LocalRuntimeError::InspectProcess)?;
    if !output.status.success() {
        return Ok(None);
    }
    let command = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if command.is_empty() {
        Ok(None)
    } else {
        Ok(Some(command))
    }
}

fn terminate_process(pid: u32) -> Result<(), LocalRuntimeError> {
    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if result == 0 {
        Ok(())
    } else {
        Err(LocalRuntimeError::TerminateDaemon(
            io::Error::last_os_error(),
        ))
    }
}

fn wait_for_runtime_daemon_exit(pid: u32) -> Result<(), LocalRuntimeError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match process_state(pid)? {
            None => return Ok(()),
            Some(state) if state.starts_with('Z') => return Ok(()),
            Some(_) => {}
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(LocalRuntimeError::TerminateDaemonTimeout(pid))
}

fn process_state(pid: u32) -> Result<Option<String>, LocalRuntimeError> {
    let output = Command::new("ps")
        .arg("-p")
        .arg(pid.to_string())
        .arg("-o")
        .arg("stat=")
        .output()
        .map_err(LocalRuntimeError::InspectProcess)?;
    if !output.status.success() {
        return Ok(None);
    }
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        Ok(None)
    } else {
        Ok(Some(state))
    }
}

fn serve_test_incompatible_daemon(
    config: &botster_hub::HubConfig,
) -> Result<(), botster_hub::DaemonTransportError> {
    let socket_path = config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(botster_hub::DaemonTransportError::MissingSocketBinding)?;
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(botster_hub::DaemonTransportError::Io)?;
    }
    let _ = std::fs::remove_file(&socket_path);
    let listener =
        UnixListener::bind(&socket_path).map_err(botster_hub::DaemonTransportError::Io)?;
    loop {
        let (mut stream, _) = listener
            .accept()
            .map_err(botster_hub::DaemonTransportError::Io)?;
        let mut reader = BufReader::new(
            stream
                .try_clone()
                .map_err(botster_hub::DaemonTransportError::Io)?,
        );
        let mut hello = String::new();
        let _ = reader.read_line(&mut hello);
        stream
            .write_all(b"{\"protocol\":\"botster-hub-daemon-v1\"}\n")
            .map_err(botster_hub::DaemonTransportError::Io)?;
    }
}

struct RuntimeWebLaunch {
    local_url: String,
    package_state: String,
}

fn require_runtime_package(
    packages: &[DaemonPackage],
    package_name: &'static str,
) -> Result<(), LocalRuntimeError> {
    let package = packages
        .iter()
        .find(|package| package.package_name == package_name)
        .ok_or(LocalRuntimeError::MissingPackage {
            label: package_name,
        })?;
    if package.state != "enabled" {
        return Err(LocalRuntimeError::PackageDisabled {
            package_name,
            state: package.state.clone(),
        });
    }
    Ok(())
}

fn start_installed_web_app(
    config: &botster_hub::HubConfig,
    packages: &[DaemonPackage],
) -> Result<RuntimeWebLaunch, LocalRuntimeError> {
    let package = packages
        .iter()
        .find(|package| package.package_name == "botster-web")
        .ok_or(LocalRuntimeError::MissingPackage {
            label: "botster-web",
        })?;
    if !package
        .runnable_entrypoints
        .iter()
        .any(|entrypoint| entrypoint.id == "web-client")
    {
        return Err(LocalRuntimeError::MissingWebEntrypoint);
    }

    let response = daemon_transport_request(
        config,
        DaemonRequest::StartPackageEntrypoint {
            package_name: "botster-web".to_string(),
            entrypoint_id: "web-client".to_string(),
            environment_overrides: BTreeMap::new(),
        },
    )?;
    if response.kind == DaemonResponseKind::OperatorError {
        return Err(LocalRuntimeError::WebEntrypointStart(
            response
                .error
                .map(|error| error.message)
                .unwrap_or_else(|| "botster-web/web-client failed without diagnostics".to_string()),
        ));
    }

    let apps = daemon_transport_request(config, DaemonRequest::ListApps)?;
    let app = apps
        .apps
        .iter()
        .find(|app| app.package_name == "botster-web" && app.entrypoint_id == "web-client")
        .ok_or(LocalRuntimeError::MissingWebEntrypoint)?;
    let local_url = app
        .launch_target
        .local_url
        .as_ref()
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| {
            LocalRuntimeError::WebEntrypointStart(format!(
                "package botster-web entrypoint web-client returned lifecycle_state={} without structured local_url",
                app.lifecycle_state
            ))
        })?
        .clone();
    verify_web_health(&local_url)?;
    verify_web_ui(&local_url)?;

    Ok(RuntimeWebLaunch {
        local_url,
        package_state: package.state.clone(),
    })
}

fn verify_web_health(local_url: &str) -> Result<(), LocalRuntimeError> {
    let origin = runtime_origin(local_url).ok_or_else(|| {
        LocalRuntimeError::WebHealth(format!(
            "botster-web/web-client returned invalid local_url {local_url}"
        ))
    })?;
    let health_url = format!("{origin}/health");
    let (_, body) = read_web_http(&health_url, "health")?;
    let health: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|error| LocalRuntimeError::WebHealth(format!("invalid JSON: {error}")))?;
    if health.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(LocalRuntimeError::WebHealth(format!(
            "unexpected response {health}"
        )));
    }
    Ok(())
}

fn verify_web_ui(local_url: &str) -> Result<(), LocalRuntimeError> {
    let (content_type, body) = read_web_http(local_url, "UI")?;
    if !content_type
        .as_deref()
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/html"))
    {
        return Err(LocalRuntimeError::WebUi(
            "response did not use an HTML content type".to_string(),
        ));
    }
    let body = String::from_utf8_lossy(&body).to_ascii_lowercase();
    if !body.contains("<!doctype html") && !body.contains("<html") {
        return Err(LocalRuntimeError::WebUi(
            "response was not an HTML document".to_string(),
        ));
    }
    Ok(())
}

fn read_web_http(
    url: &str,
    label: &'static str,
) -> Result<(Option<String>, Vec<u8>), LocalRuntimeError> {
    let timeout = Duration::from_secs(2);
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(Some(timeout))
        .timeout_connect(Some(timeout))
        .timeout_recv_response(Some(timeout))
        .timeout_recv_body(Some(timeout))
        .build()
        .into();
    let request = ureq::http::Request::builder()
        .method(ureq::http::Method::GET)
        .uri(url)
        .body(Vec::new())
        .map_err(|_| LocalRuntimeError::WebRequest {
            label,
            message: "invalid URL".to_string(),
        })?;
    let mut response = agent
        .run(request)
        .map_err(|error| LocalRuntimeError::WebRequest {
            label,
            message: error.to_string(),
        })?;
    if !response.status().is_success() {
        return Err(LocalRuntimeError::WebRequest {
            label,
            message: format!("returned HTTP {}", response.status()),
        });
    }
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .body_mut()
        .with_config()
        .limit(1_048_576)
        .read_to_vec()
        .map_err(|error| LocalRuntimeError::WebRequest {
            label,
            message: error.to_string(),
        })?;
    Ok((content_type, body))
}

fn runtime_origin(local_url: &str) -> Option<String> {
    let scheme_end = local_url.find("://")?;
    let after_scheme = scheme_end + 3;
    let authority_end = local_url[after_scheme..]
        .find(['/', '?', '#'])
        .map(|index| after_scheme + index)
        .unwrap_or(local_url.len());
    (authority_end > after_scheme).then(|| local_url[..authority_end].to_string())
}

fn operator_status(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataArgs::parse(args, "status")?;
    if !options.arguments.is_empty() {
        return Err(OperatorError::Usage("status"));
    }
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

fn operator_session_templates(args: Vec<String>) -> Result<(), OperatorError> {
    let Some(action) = args.first().map(String::as_str) else {
        return Err(OperatorError::Usage("session-templates"));
    };
    match action {
        "list" => {
            let options = DataArgs::parse(args[1..].to_vec(), "session-templates list")?;
            if !options.arguments.is_empty() {
                return Err(OperatorError::Usage("session-templates list"));
            }
            let config = explicit_config(options.data_directory)?;
            let response = daemon_transport_request(&config, DaemonRequest::ListSessionTemplates)?;
            print_daemon_response(response)?;
        }
        "show" => {
            if args.len() != 4 {
                return Err(OperatorError::Usage("session-templates show"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "session-templates show")?;
            let config = explicit_config(options.data_directory)?;
            let response = daemon_transport_request(
                &config,
                DaemonRequest::ShowSessionTemplate {
                    template_id: args[3].clone(),
                },
            )?;
            print_daemon_response(response)?;
        }
        "resolve" => {
            if args.len() < 4 {
                return Err(OperatorError::Usage("session-templates resolve"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "session-templates resolve")?;
            let config = explicit_config(options.data_directory)?;
            let request = parse_session_template_request(&args[4..])?;
            let response = daemon_transport_request(
                &config,
                DaemonRequest::ResolveSessionTemplate {
                    template_id: args[3].clone(),
                    request,
                },
            )?;
            print_daemon_response(response)?;
        }
        "spawn" => {
            if args.len() < 6 || args.get(4).map(String::as_str) != Some("--session-id") {
                return Err(OperatorError::Usage("session-templates spawn"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "session-templates spawn")?;
            let config = explicit_config(options.data_directory)?;
            let request = parse_session_template_request(&args[6..])?;
            let response = daemon_transport_request(
                &config,
                DaemonRequest::SpawnSessionTemplate {
                    template_id: args[3].clone(),
                    session_id: args[5].clone(),
                    request,
                },
            )?;
            print_daemon_response(response)?;
        }
        _ => return Err(OperatorError::Usage("session-templates")),
    }
    Ok(())
}

fn operator_spawn_targets(args: Vec<String>) -> Result<(), OperatorError> {
    let Some(action) = args.first().map(String::as_str) else {
        return Err(OperatorError::Usage("spawn-targets"));
    };
    match action {
        "list" => {
            let options = DataArgs::parse(args[1..].to_vec(), "spawn-targets list")?;
            if !options.arguments.is_empty() {
                return Err(OperatorError::Usage("spawn-targets list"));
            }
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(
                &config,
                DaemonRequest::ListSpawnTargets,
            )?)?;
        }
        "show" => {
            if args.len() != 4 {
                return Err(OperatorError::Usage("spawn-targets show"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "spawn-targets show")?;
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(
                &config,
                DaemonRequest::ShowSpawnTarget {
                    target_id: args[3].clone(),
                },
            )?)?;
        }
        "create" => {
            if args.len() < 3 {
                return Err(OperatorError::Usage("spawn-targets create"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "spawn-targets create")?;
            let request = parse_spawn_target_create(&args[3..])?;
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(&config, request)?)?;
        }
        "update" => {
            if args.len() < 4 {
                return Err(OperatorError::Usage("spawn-targets update"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "spawn-targets update")?;
            let request = parse_spawn_target_update(&args[3], &args[4..])?;
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(&config, request)?)?;
        }
        "delete" => {
            if args.len() != 4 {
                return Err(OperatorError::Usage("spawn-targets delete"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "spawn-targets delete")?;
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(
                &config,
                DaemonRequest::DeleteSpawnTarget {
                    target_id: args[3].clone(),
                },
            )?)?;
        }
        "validate" => {
            if args.len() != 4 {
                return Err(OperatorError::Usage("spawn-targets validate"));
            }
            let options = DataArgs::parse(args[1..3].to_vec(), "spawn-targets validate")?;
            let config = explicit_config(options.data_directory)?;
            print_daemon_response(daemon_transport_request(
                &config,
                DaemonRequest::ValidateSpawnTarget {
                    target_id: args[3].clone(),
                },
            )?)?;
        }
        _ => return Err(OperatorError::Usage("spawn-targets")),
    }
    Ok(())
}

fn parse_spawn_target_create(args: &[String]) -> Result<DaemonRequest, OperatorError> {
    let mut target_id = None;
    let mut label = None;
    let mut root = None;
    let mut enabled = true;
    let mut kind = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--id" => {
                target_id = Some(required_arg(args, index + 1, "spawn-targets create")?);
                index += 2;
            }
            "--label" => {
                label = Some(required_arg(args, index + 1, "spawn-targets create")?);
                index += 2;
            }
            "--root" => {
                root = Some(PathBuf::from(required_arg(
                    args,
                    index + 1,
                    "spawn-targets create",
                )?));
                index += 2;
            }
            "--kind" => {
                kind = Some(required_arg(args, index + 1, "spawn-targets create")?);
                index += 2;
            }
            "--disabled" => {
                enabled = false;
                index += 1;
            }
            _ => return Err(OperatorError::Usage("spawn-targets create")),
        }
    }
    let root = root.ok_or(OperatorError::Usage("spawn-targets create"))?;
    Ok(DaemonRequest::CreateSpawnTarget {
        target_id,
        label,
        root,
        enabled,
        kind,
        metadata: BTreeMap::new(),
    })
}

fn parse_spawn_target_update(
    target_id: &str,
    args: &[String],
) -> Result<DaemonRequest, OperatorError> {
    let mut label = None;
    let mut root = None;
    let mut enabled = None;
    let mut kind = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--label" => {
                label = Some(required_arg(args, index + 1, "spawn-targets update")?);
                index += 2;
            }
            "--root" => {
                root = Some(PathBuf::from(required_arg(
                    args,
                    index + 1,
                    "spawn-targets update",
                )?));
                index += 2;
            }
            "--kind" => {
                kind = Some(required_arg(args, index + 1, "spawn-targets update")?);
                index += 2;
            }
            "--enable" => {
                enabled = Some(true);
                index += 1;
            }
            "--disable" => {
                enabled = Some(false);
                index += 1;
            }
            _ => return Err(OperatorError::Usage("spawn-targets update")),
        }
    }
    Ok(DaemonRequest::UpdateSpawnTarget {
        target_id: target_id.to_string(),
        label,
        root,
        enabled,
        kind,
        metadata: None,
    })
}

fn required_arg(
    args: &[String],
    index: usize,
    command: &'static str,
) -> Result<String, OperatorError> {
    args.get(index)
        .cloned()
        .ok_or(OperatorError::Usage(command))
}

fn operator_context(args: Vec<String>) -> Result<(), OperatorError> {
    let session_id = env::var("BOTSTER_SESSION_ID").ok();
    let context_id = env::var("BOTSTER_CONTEXT_ID").ok();
    let mut data_directory = None;
    let mut requested_session_id = session_id;
    let mut requested_context_id = context_id;
    let mut key = None;
    let mut cursor = 0;
    while cursor < args.len() {
        match args[cursor].as_str() {
            "--data-dir" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("context"));
                };
                data_directory = Some(PathBuf::from(value));
                cursor += 2;
            }
            "--session-id" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("context"));
                };
                requested_session_id = Some(value.clone());
                cursor += 2;
            }
            "--context-id" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("context"));
                };
                requested_context_id = Some(value.clone());
                cursor += 2;
            }
            "--key" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("context"));
                };
                key = Some(value.clone());
                cursor += 2;
            }
            _ => return Err(OperatorError::Usage("context")),
        }
    }
    let Some(data_directory) = data_directory else {
        return Err(OperatorError::Usage("context"));
    };
    let Some(session_id) = requested_session_id else {
        return Err(OperatorError::Usage("context"));
    };
    let config = explicit_config(data_directory)?;
    let response = daemon_transport_request(
        &config,
        DaemonRequest::ReadSessionContext {
            session_id,
            context_id: requested_context_id,
            key,
        },
    )?;
    if let Some(context) = response.session_context {
        println!(
            "{}",
            serde_json::to_string(&context.values).map_err(OperatorError::Serialize)?
        );
        Ok(())
    } else {
        Err(OperatorError::UnexpectedResponse("context"))
    }
}

fn parse_session_template_request(
    args: &[String],
) -> Result<botster_hub::DaemonSessionTemplateRequest, OperatorError> {
    let mut request = botster_hub::DaemonSessionTemplateRequest::default();
    let mut cursor = 0;
    while cursor < args.len() {
        match args[cursor].as_str() {
            "--target-id" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.target_id = Some(value.clone());
                cursor += 2;
            }
            "--cwd" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.cwd = Some(value.clone());
                cursor += 2;
            }
            "--env" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                let Some((name, value)) = value.split_once('=') else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request
                    .environment
                    .insert(name.to_string(), value.to_string());
                cursor += 2;
            }
            "--prompt" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.context.prompt = Some(value.clone());
                cursor += 2;
            }
            "--branch" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.context.branch_name = Some(value.clone());
                cursor += 2;
            }
            "--ticket-id" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.context.ticket_id = Some(value.clone());
                cursor += 2;
            }
            "--workspace-id" => {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage("session-templates"));
                };
                request.context.workspace_id = Some(value.clone());
                cursor += 2;
            }
            _ => return Err(OperatorError::Usage("session-templates")),
        }
    }
    Ok(request)
}

fn operator_shutdown(args: Vec<String>) -> Result<(), OperatorError> {
    let options = DataArgs::parse(args, "shutdown")?;
    if !options.arguments.is_empty() {
        return Err(OperatorError::Usage("shutdown"));
    }
    let config = explicit_config(options.data_directory)?;
    let response = daemon_transport_request(&config, DaemonRequest::DaemonShutdown)?;
    print_daemon_response(response)?;
    Ok(())
}

fn mcp_serve(args: Vec<String>) -> Result<(), McpCliError> {
    let options = DataArgs::parse(args, "mcp-serve")?;
    if !options.arguments.is_empty() {
        return Err(OperatorError::Usage("mcp-serve").into());
    }
    let config = explicit_config(options.data_directory)?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve_mcp_stdio(config, BufReader::new(stdin.lock()), stdout.lock())?;
    Ok(())
}

fn operator_open_alias(args: Vec<String>) -> Result<CommandOutcome, OperatorError> {
    let Some(alias) = args.first() else {
        return Err(OperatorError::Usage("open"));
    };
    let selector = match alias.as_str() {
        "web" => "botster-web/web-client",
        "tui" => "botster-tui",
        _ => return Err(OperatorError::Usage("open")),
    };
    let options = DataArgs::parse(args[1..].to_vec(), "open")?;
    if !options.arguments.is_empty() {
        return Err(OperatorError::Usage("open"));
    }
    open_app_by_selector(options.data_directory, selector)
}

fn operator_reload_alias(args: Vec<String>) -> Result<(), OperatorError> {
    if args.len() != 3 {
        return Err(OperatorError::Usage("reload"));
    }
    let options = DataArgs::parse(args[1..3].to_vec(), "reload")?;
    operator_packages(
        vec![
            "reload".to_string(),
            "--data-dir".to_string(),
            options.data_directory.to_string_lossy().into_owned(),
            args[0].clone(),
        ],
        false,
    )
}

fn operator_apps(args: Vec<String>) -> Result<CommandOutcome, OperatorError> {
    let command = AppCommand::parse(args)?;
    match command.action {
        AppActionCommand::List => {
            let config = explicit_config(command.data_directory)?;
            let response = daemon_transport_request(&config, DaemonRequest::ListApps)?;
            print_apps(&response.apps);
            Ok(CommandOutcome::Completed)
        }
        AppActionCommand::Show(selector) => {
            let config = explicit_config(command.data_directory)?;
            let response = daemon_transport_request(&config, DaemonRequest::ListApps)?;
            let app = resolve_app_selector(&response.apps, &selector)?;
            print_app_detail(app);
            Ok(CommandOutcome::Completed)
        }
        AppActionCommand::Open(selector) => open_app_by_selector(command.data_directory, &selector),
    }
}

fn open_app_by_selector(
    data_directory: PathBuf,
    selector: &str,
) -> Result<CommandOutcome, OperatorError> {
    let config = explicit_config(data_directory)?;
    let response = daemon_transport_request(&config, DaemonRequest::ListApps)?;
    let app = resolve_app_selector(&response.apps, selector)?.clone();
    match app.kind.as_str() {
        "web_app" => open_web_app(&config, app).map(|()| CommandOutcome::Completed),
        "terminal_app" => open_terminal_app(&config, app),
        _ => Err(OperatorError::App(format!(
            "unsupported app kind {} for {}",
            app.kind, app.entrypoint_id
        ))),
    }
}

fn open_web_app(config: &botster_hub::HubConfig, app: DaemonApp) -> Result<(), OperatorError> {
    if app.launch_mode != "background" {
        return Err(OperatorError::App(format!(
            "web_app {} must use background launch mode",
            app.entrypoint_id
        )));
    }
    if !app.blocked_reasons.is_empty() {
        return Err(OperatorError::App(format!(
            "app {} is blocked: {}",
            app.entrypoint_id,
            app.blocked_reasons.join(",")
        )));
    }
    if app.lifecycle_state != "running" {
        let response = daemon_transport_request(
            config,
            DaemonRequest::StartPackageEntrypoint {
                package_name: app.package_name.clone(),
                entrypoint_id: app.entrypoint_id.clone(),
                environment_overrides: BTreeMap::new(),
            },
        )?;
        if response.kind == DaemonResponseKind::OperatorError {
            return print_daemon_response(response);
        }
    }
    let app = wait_for_app_url(config, &app.package_name, &app.entrypoint_id)?;
    let Some(url) = app.launch_target.local_url else {
        return Err(OperatorError::App(format!(
            "app {} did not report a structured local_url",
            app.entrypoint_id
        )));
    };
    println!("app_url={url}");
    Ok(())
}

fn open_terminal_app(
    config: &botster_hub::HubConfig,
    app: DaemonApp,
) -> Result<CommandOutcome, OperatorError> {
    if app.launch_mode != "foreground_stdio" {
        return Err(OperatorError::App(format!(
            "terminal_app {} must use foreground_stdio launch mode",
            app.entrypoint_id
        )));
    }
    if !app.blocked_reasons.is_empty() {
        return Err(OperatorError::App(format!(
            "app {} is blocked: {}",
            app.entrypoint_id,
            app.blocked_reasons.join(",")
        )));
    }
    let response = daemon_transport_request(
        config,
        DaemonRequest::ResolveAppLaunch {
            package_name: app.package_name,
            entrypoint_id: app.entrypoint_id,
        },
    )?;
    if response.kind == DaemonResponseKind::OperatorError {
        print_daemon_response(response)?;
        return Ok(CommandOutcome::Completed);
    }
    let launch = response
        .resolved_app_launch
        .ok_or(OperatorError::UnexpectedResponse("resolve_app_launch"))?;
    let mut command = Command::new(&launch.command);
    command.args(&launch.args);
    command.current_dir(&launch.working_directory);
    command.envs(&launch.environment);
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    unsafe {
        // SAFETY: this hook runs only in the foreground app child. Restoring SIGINT's default
        // disposition lets Ctrl-C target the handed-off app while the console parent stays alive.
        command.pre_exec(|| {
            if libc::signal(libc::SIGINT, libc::SIG_DFL) == libc::SIG_ERR {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let status = command.status().map_err(OperatorError::SpawnApp)?;
    if let Some(code) = status.code() {
        if code == 0 {
            Ok(CommandOutcome::Completed)
        } else {
            Ok(CommandOutcome::ForegroundAppExited {
                code,
                description: format!("exited with code {code}"),
            })
        }
    } else {
        let signal = status.signal();
        Ok(CommandOutcome::ForegroundAppExited {
            code: 1,
            description: signal.map_or_else(
                || "terminated without an exit code".to_string(),
                |signal| format!("terminated by signal {signal}"),
            ),
        })
    }
}

fn wait_for_app_url(
    config: &botster_hub::HubConfig,
    package_name: &str,
    entrypoint_id: &str,
) -> Result<DaemonApp, OperatorError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_app = None;
    while Instant::now() < deadline {
        let response = daemon_transport_request(config, DaemonRequest::ListApps)?;
        let app = response
            .apps
            .into_iter()
            .find(|app| app.package_name == package_name && app.entrypoint_id == entrypoint_id)
            .ok_or_else(|| OperatorError::App("app disappeared from registry".to_string()))?;
        if app.lifecycle_state == "running" && app.launch_target.local_url.is_some() {
            return Ok(app);
        }
        last_app = Some(app);
        thread::sleep(Duration::from_millis(50));
    }
    let state = last_app
        .map(|app| app.lifecycle_state)
        .unwrap_or_else(|| "missing".to_string());
    Err(OperatorError::App(format!(
        "timed out waiting for structured app URL; lifecycle_state={state}"
    )))
}

fn resolve_app_selector<'a>(
    apps: &'a [DaemonApp],
    selector: &str,
) -> Result<&'a DaemonApp, OperatorError> {
    let matches = if let Some((package, app_id)) = selector.split_once('/') {
        apps.iter()
            .filter(|app| app.package_name == package && app.app_id == app_id)
            .collect::<Vec<_>>()
    } else {
        apps.iter()
            .filter(|app| {
                app.app_id == selector
                    || app.entrypoint_id == selector
                    || app.package_name == selector && app.kind == "terminal_app"
            })
            .collect::<Vec<_>>()
    };
    match matches.as_slice() {
        [app] => Ok(*app),
        [] => Err(OperatorError::App(format!(
            "app {selector} is not installed or enabled"
        ))),
        _ => Err(OperatorError::App(format!(
            "app selector {selector} is ambiguous; use package/app"
        ))),
    }
}

fn print_app_detail(app: &DaemonApp) {
    println!("response=app");
    println!("package={}", app.package_name);
    println!("app_id={}", app.app_id);
    println!("entrypoint_id={}", app.entrypoint_id);
    println!("kind={}", app.kind);
    println!("launch_mode={}", app.launch_mode);
    println!("lifecycle_state={}", app.lifecycle_state);
    println!("launch_target={}", app.launch_target.kind);
    println!(
        "local_url={}",
        app.launch_target.local_url.as_deref().unwrap_or("")
    );
    println!("blocked_reasons={}", app.blocked_reasons.len());
    for reason in &app.blocked_reasons {
        println!("blocked_reason={reason}");
    }
    println!("diagnostics={}", app.diagnostics.len());
    for diagnostic in &app.diagnostics {
        println!(
            "app_diagnostic kind={} message={}",
            diagnostic.kind, diagnostic.message
        );
    }
    println!("actions={}", app.actions.len());
    for action in &app.actions {
        println!(
            "app_action id={} status={}",
            action.action_id,
            package_action_status_label(action.status)
        );
    }
}

fn package_action_status_label(status: DaemonPackageActionStatus) -> &'static str {
    match status {
        DaemonPackageActionStatus::Available => "available",
        DaemonPackageActionStatus::Blocked => "blocked",
        DaemonPackageActionStatus::Unavailable => "unavailable",
    }
}

fn operator_packages(args: Vec<String>, providers_only: bool) -> Result<(), OperatorError> {
    let command = PackageCommand::parse(args, providers_only)?;
    let config = explicit_config(command.data_directory)?;

    match command.action {
        PackageActionCommand::List => {
            let response = daemon_transport_request(&config, DaemonRequest::ListPackages)?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::Available(registry_path) => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::ListAvailablePackages { registry_path },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::InspectAvailable {
            registry_path,
            entry_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::InspectAvailablePackage {
                    registry_path,
                    entry_id,
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::PreviewInstall {
            registry_path,
            entry_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::PreviewPackageInstall {
                    registry_path,
                    entry_id,
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::InstallRegistryEntry {
            registry_path,
            entry_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::InstallPackageRegistryEntry {
                    registry_path,
                    entry_id,
                },
            )?;
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
        PackageActionCommand::Config(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::ShowPackage { package_name })?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::SetConfig {
            package_name,
            values,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::SetPackageConfiguration {
                    package_name,
                    values,
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::Reload(package_name) => {
            let response =
                daemon_transport_request(&config, DaemonRequest::ReloadPackage { package_name })?;
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
        PackageActionCommand::CheckUpdate(package_name) => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::CheckPackageUpdate { package_name },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::PreviewUpdate { package_name, pin } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::PreviewPackageUpdate { package_name, pin },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::ApplyUpdate { package_name, pin } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::ApplyPackageUpdate { package_name, pin },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::StartEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::StartPackageEntrypoint {
                    package_name,
                    entrypoint_id,
                    environment_overrides: BTreeMap::new(),
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::StopEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::StopPackageEntrypoint {
                    package_name,
                    entrypoint_id,
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::RestartEntrypoint {
            package_name,
            entrypoint_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::RestartPackageEntrypoint {
                    package_name,
                    entrypoint_id,
                },
            )?;
            print_packages_response(response, providers_only)?;
        }
        PackageActionCommand::EntrypointStatus {
            package_name,
            entrypoint_id,
        } => {
            let response = daemon_transport_request(
                &config,
                DaemonRequest::PackageEntrypointStatus {
                    package_name,
                    entrypoint_id,
                },
            )?;
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
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
        DaemonResponseKind::EntitySubscribed => println!("response=entity_subscribed"),
        DaemonResponseKind::EntityUnsubscribed => println!("response=entity_unsubscribed"),
        DaemonResponseKind::SessionRemoved => println!("response=session_removed"),
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
        DaemonResponseKind::SessionTemplates => {
            println!("response=session_templates");
            println!("template_count={}", response.session_templates.len());
            for template in response.session_templates {
                println!(
                    "template id={} package={} available={} target={}",
                    template.id, template.package_name, template.available, template.target_id
                );
            }
        }
        DaemonResponseKind::ResolvedSessionTemplate => {
            println!("response=resolved_session_template");
            if let Some(resolved) = response.resolved_session_template {
                println!("template_id={}", resolved.template.template_id);
                println!("session_id={}", resolved.session_id);
                println!("command_present={}", !resolved.executable.is_empty());
                println!("args={}", resolved.arguments.len());
                println!("environment={}", resolved.environment.len());
                println!("context_id={}", resolved.context_id);
                println!("context_keys={}", resolved.context_keys.len());
            }
        }
        DaemonResponseKind::SessionContext => {
            println!("response=session_context");
            if let Some(context) = response.session_context {
                println!(
                    "{}",
                    serde_json::to_string(&context.values).map_err(OperatorError::Serialize)?
                );
            }
        }
        DaemonResponseKind::ReadScreen => {
            println!("response=read_screen");
            if let Some(screen) = response.read_screen {
                println!("session_id={}", screen.session_id);
                println!("text={}", screen.text);
            }
        }
        DaemonResponseKind::ReadModeFlags => {
            println!("response=read_mode_flags");
            if let Some(mode_flags) = response.mode_flags {
                println!("session_id={}", mode_flags.session_id);
                println!("mouse_mode={}", mode_flags.mouse_mode);
            }
        }
        DaemonResponseKind::CaptureSnapshot => {
            println!("response=capture_snapshot");
            if let Some(snapshot) = response.capture_snapshot {
                println!("session_id={}", snapshot.session_id);
                println!("rows={}", snapshot.rows);
                println!("cols={}", snapshot.cols);
                println!("payload_format={:?}", snapshot.payload_format);
                println!("payload_bytes={}", snapshot.payload_bytes);
            }
        }
        DaemonResponseKind::SpawnTargets => {
            println!("response=spawn_targets");
            println!("target_count={}", response.spawn_targets.len());
            for target in response.spawn_targets {
                print_spawn_target(&target);
            }
        }
        DaemonResponseKind::SpawnTargetValidation => {
            println!("response=spawn_target_validation");
            if let Some(validation) = response.spawn_target_validation {
                println!("target_id={}", validation.target_id);
                println!("ok={}", validation.ok);
                println!("status={}", validation.status);
            }
        }
        DaemonResponseKind::Worktrees => {
            println!("response=worktrees");
            println!("worktree_count={}", response.worktrees.len());
            for worktree in response.worktrees {
                print_worktree(&worktree);
            }
        }
        DaemonResponseKind::Apps => {
            print_apps(&response.apps);
        }
        DaemonResponseKind::ResolvedAppLaunch => {
            println!("response=resolved_app_launch");
            if let Some(launch) = response.resolved_app_launch {
                println!("package={}", launch.package_name);
                println!("app_id={}", launch.app_id);
                println!("entrypoint_id={}", launch.entrypoint_id);
                println!("kind={}", launch.kind);
                println!("launch_mode={}", launch.launch_mode);
                println!("command_present={}", !launch.command.is_empty());
                println!("args={}", launch.args.len());
                println!("environment={}", launch.environment.len());
            }
        }
        DaemonResponseKind::ResolvedPackageRoute => {
            println!("response=resolved_package_route");
            if let Some(route) = response.resolved_package_route {
                println!("package={}", route.package_name);
                println!("route_id={}", route.route_id);
                println!("route_path={}", route.route_path);
                println!("target={}", route.target.kind);
                println!("layout_mode={}", route.layout_mode);
                println!("enabled={}", route.enabled);
                println!("blocked={}", route.blocked);
                println!("diagnostics={}", route.diagnostics.len());
            }
        }
        DaemonResponseKind::PackageNavigation => {
            println!("response=package_navigation");
            println!("navigation_count={}", response.package_navigation.len());
            for entry in response.package_navigation {
                println!(
                    "navigation package={} item={} label={} route={} enabled={} blocked={} diagnostics={}",
                    entry.package_name,
                    entry.item_id,
                    entry.label,
                    entry.route_path,
                    entry.enabled,
                    entry.blocked,
                    entry.diagnostics.len()
                );
            }
        }
        DaemonResponseKind::Packages => {
            print_packages(&response.packages, false);
        }
        DaemonResponseKind::AvailablePackages => {
            print_available_packages(&response.available_packages);
        }
        DaemonResponseKind::PackageInstallPlan => {
            if let Some(plan) = response.install_plan.as_ref() {
                print_package_install_plan(plan);
            }
        }
        DaemonResponseKind::PackageUpdateStatus => {
            if let Some(status) = response.update_status.as_ref() {
                print_package_update_status(status);
            }
        }
        DaemonResponseKind::PackageDecision => {
            if let Some(decision) = response.package_decision {
                print_package_decision(&decision);
            }
            if let Some(status) = response.update_status.as_ref() {
                print_package_update_status(status);
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
        DaemonResponseKind::LocalWebrtcBootstrap => {
            println!("response=local_webrtc_bootstrap");
            if let Some(bootstrap) = response.local_webrtc_bootstrap {
                println!("package={}", bootstrap.package_name);
                println!("entrypoint_id={}", bootstrap.entrypoint_id);
                println!("grant_id={}", bootstrap.grant_id);
                println!("expected_origin={}", bootstrap.expected_origin);
                println!("expires_at={}", bootstrap.expires_at);
                println!("signaling_transport={}", bootstrap.signaling_transport);
                println!("data_plane={}", bootstrap.data_plane);
                println!("ordered={}", bootstrap.ordered);
                println!("max_retransmits={:?}", bootstrap.max_retransmits);
                println!(
                    "max_packet_lifetime_ms={:?}",
                    bootstrap.max_packet_lifetime_ms
                );
            }
        }
        DaemonResponseKind::LocalWebrtcAnswer => {
            println!("response=local_webrtc_answer");
            if let Some(answer) = response.local_webrtc_answer {
                println!("grant_id={}", answer.grant_id);
                println!("answer_present={}", !answer.answer.is_null());
                println!("diagnostic_count={}", answer.diagnostics.len());
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

fn print_spawn_target(target: &DaemonSpawnTarget) {
    println!(
        "target id={} label={} enabled={} kind={} root={}",
        target.target_id,
        target.label,
        target.enabled,
        target.kind,
        target.root.display()
    );
}

fn print_worktree(worktree: &DaemonWorktree) {
    println!(
        "worktree id={} target={} label={} status={} path={} git={}",
        worktree.worktree_id,
        worktree.target_id,
        worktree.label,
        worktree.status,
        worktree.path.display(),
        worktree.git.is_some()
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
                history,
            } => {
                println!(
                    "event=snapshot session_id={session_id} subscription_id={subscription_id} bytes={}",
                    history.bytes
                );
            }
            DaemonEvent::Scrollback {
                session_id,
                subscription_id,
                history,
            } => {
                println!(
                    "event=scrollback session_id={session_id} subscription_id={subscription_id} bytes={}",
                    history.bytes
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
            DaemonEvent::WorktreeLifecycle { event } => {
                println!(
                    "event=worktree_lifecycle name={} worktree_id={} target_id={} status={} failure_kind={}",
                    event.event,
                    event.worktree_id.as_deref().unwrap_or("none"),
                    event.target_id.as_deref().unwrap_or("none"),
                    event.status.as_deref().unwrap_or("none"),
                    event.failure_kind.as_deref().unwrap_or("none")
                );
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
    if let Some(plan) = response.install_plan.as_ref() {
        print_package_install_plan(plan);
    }
    if let Some(status) = response.update_status.as_ref() {
        print_package_update_status(status);
    }
    if !response.available_packages.is_empty() {
        print_available_packages(&response.available_packages);
    }
    print_packages(&response.packages, providers_only);
    Ok(())
}

fn print_available_packages(packages: &[botster_hub::DaemonAvailablePackage]) {
    println!("response=available_packages");
    println!("available_package_count={}", packages.len());
    for package in packages {
        println!(
            "available_package entry={} name={} version={} classification={} source_kind={} source={} first_party={} state={} capabilities={} compatibility={}",
            package.entry_id,
            package.package_name,
            package.version,
            package.classification,
            package.source_kind,
            package.source_label,
            package.first_party,
            package.state,
            package.requested_capabilities.len(),
            package.compatibility.result
        );
        if let Some(pin) = &package.pin {
            println!(
                "available_package_pin entry={} revision={} branch={} tag={} rev={} update_policy={}",
                package.entry_id,
                pin.revision,
                pin.branch.as_deref().unwrap_or("none"),
                pin.tag.as_deref().unwrap_or("none"),
                pin.rev.as_deref().unwrap_or("none"),
                pin.update_policy
            );
        }
    }
}

fn print_package_install_plan(plan: &botster_hub::DaemonPackageInstallPlan) {
    println!("response=package_install_plan");
    println!(
        "install_plan entry={} package={} state={} mutates_registry={} starts_entrypoints={}",
        plan.entry.entry_id,
        plan.entry.package_name,
        plan.entry.state,
        plan.mutates_registry,
        plan.starts_entrypoints
    );
    for effect in &plan.effects {
        println!(
            "install_plan_effect kind={} message={}",
            effect.kind, effect.message
        );
    }
    for diagnostic in &plan.diagnostics {
        println!(
            "install_plan_diagnostic kind={} message={}",
            diagnostic.kind, diagnostic.message
        );
    }
}

fn print_package_update_status(status: &DaemonPackageUpdateStatus) {
    println!(
        "package_update package={} update_available={} reload_required={} restart_required={}",
        status.package_name,
        status.update_available,
        status.reload_required,
        status.restart_required
    );
    if let Some(pin) = &status.pin {
        println!(
            "package_update_pin package={} revision={} branch={} tag={} rev={} checksum={} update_policy={}",
            status.package_name,
            pin.revision,
            pin.branch.as_deref().unwrap_or("none"),
            pin.tag.as_deref().unwrap_or("none"),
            pin.rev.as_deref().unwrap_or("none"),
            pin.checksum.as_deref().unwrap_or("none"),
            pin.update_policy
        );
    }
    for diagnostic in &status.diagnostics {
        println!(
            "package_update_diagnostic package={} kind={} message={}",
            status.package_name, diagnostic.kind, diagnostic.message
        );
    }
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
            "package name={} version={} classification={} state={} capabilities={} runnable_entrypoints={} provider_profile_admitted={}",
            package.package_name,
            package.version,
            package.classification,
            package.state,
            package.requested_capabilities.len(),
            package.runnable_entrypoints.len(),
            package.provider_profile_admitted
        );
        println!(
            "package_config package={} schema_present={} effective_values={} missing_required={} diagnostics={}",
            package.package_name,
            package.configuration.schema.is_some(),
            package.configuration.effective_values.len(),
            package.configuration.missing_required.len(),
            package.configuration.diagnostics.len()
        );
        for key in &package.configuration.missing_required {
            println!(
                "package_config_missing package={} field={}",
                package.package_name, key
            );
        }
        for (key, value) in &package.configuration.effective_values {
            println!(
                "package_config_value package={} field={} value={}",
                package.package_name, key, value
            );
        }
        for diagnostic in &package.configuration.diagnostics {
            println!(
                "package_config_diagnostic package={} kind={} message={}",
                package.package_name, diagnostic.kind, diagnostic.message
            );
        }
        for entrypoint in &package.runnable_entrypoints {
            println!(
                "package_entrypoint package={} id={} kind={} launch_mode={} command={} args={} working_directory={} environment={} capabilities={} may_supervise={} process_state={}",
                package.package_name,
                entrypoint.id,
                entrypoint.kind,
                entrypoint.launch_mode,
                entrypoint.command,
                entrypoint.args.len(),
                entrypoint.working_directory.policy,
                entrypoint.environment.len(),
                entrypoint.capabilities.len(),
                entrypoint.may_supervise,
                entrypoint.process.state
            );
            if entrypoint.process.pid.is_some()
                || entrypoint.process.started_at.is_some()
                || entrypoint.process.exited_at.is_some()
                || entrypoint.process.exit_status.is_some()
            {
                println!(
                    "package_entrypoint_process package={} id={} pid={} started_at={} exited_at={} exit_status={}",
                    package.package_name,
                    entrypoint.id,
                    entrypoint
                        .process
                        .pid
                        .map_or_else(|| "none".to_string(), |value| value.to_string()),
                    entrypoint
                        .process
                        .started_at
                        .map_or_else(|| "none".to_string(), |value| value.to_string()),
                    entrypoint
                        .process
                        .exited_at
                        .map_or_else(|| "none".to_string(), |value| value.to_string()),
                    entrypoint
                        .process
                        .exit_status
                        .clone()
                        .unwrap_or_else(|| "none".to_string())
                );
            }
            for diagnostic in &entrypoint.process.diagnostics {
                println!(
                    "package_entrypoint_diagnostic package={} id={} kind={} message={}",
                    package.package_name, entrypoint.id, diagnostic.kind, diagnostic.message
                );
            }
        }
    }
}

fn print_apps(apps: &[DaemonApp]) {
    println!("response=apps");
    println!("app_count={}", apps.len());
    for app in apps {
        println!(
            "app package={} app_id={} entrypoint_id={} kind={} launch_mode={} lifecycle_state={} diagnostics={} actions={} blocked_reasons={} launch_target={} local_url={}",
            app.package_name,
            app.app_id,
            app.entrypoint_id,
            app.kind,
            app.launch_mode,
            app.lifecycle_state,
            app.diagnostics.len(),
            app.actions.len(),
            app.blocked_reasons.len(),
            app.launch_target.kind,
            app.launch_target.local_url.as_deref().unwrap_or("")
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

struct DataArgs {
    data_directory: PathBuf,
    arguments: Vec<String>,
}

impl DataArgs {
    fn parse(args: Vec<String>, command: &'static str) -> Result<Self, OperatorError> {
        let mut data_directory = None;
        let mut arguments = Vec::new();
        let mut cursor = 0;
        while cursor < args.len() {
            if args[cursor] == "--" {
                arguments.extend(args[cursor..].iter().cloned());
                break;
            } else if args[cursor] == "--data-dir" {
                if data_directory.is_some() {
                    return Err(OperatorError::Usage(command));
                }
                let Some(value) = args.get(cursor + 1) else {
                    return Err(OperatorError::Usage(command));
                };
                data_directory = Some(PathBuf::from(value));
                cursor += 2;
            } else {
                arguments.push(args[cursor].clone());
                cursor += 1;
            }
        }

        let data_directory =
            DataDirectoryOption::Explicit(data_directory.ok_or(OperatorError::Usage(command))?)
                .resolve(&RuntimeEnvironment::from_current_process())?;

        Ok(Self {
            data_directory,
            arguments,
        })
    }

    fn print_source(&self) {
        println!("data_dir=resolved:{}", self.data_directory.display());
    }
}

struct StartOptions {
    data_directory: PathBuf,
    session_worker_bin: Option<PathBuf>,
}

impl StartOptions {
    fn parse(args: Vec<String>) -> Result<Self, OperatorError> {
        let options = DataArgs::parse(args, "start")?;
        let session_worker_bin = match options.arguments.first().map(String::as_str) {
            None => None,
            Some("--session-worker-bin") if options.arguments.len() == 2 => {
                options.arguments.get(1).map(PathBuf::from)
            }
            Some(_) => return Err(OperatorError::Usage("start")),
        };

        Ok(Self {
            data_directory: options.data_directory,
            session_worker_bin,
        })
    }
}

struct LocalRuntimeOptions {
    data_directory: PathBuf,
    session_worker_bin: Option<PathBuf>,
}

impl LocalRuntimeOptions {
    fn parse(args: Vec<String>) -> Result<Self, LocalRuntimeError> {
        let options = DataArgs::parse(args, "up")?;
        let mut session_worker_bin = None;
        let mut cursor = 0;
        while cursor < options.arguments.len() {
            match options.arguments[cursor].as_str() {
                "--session-worker-bin" => {
                    let Some(value) = options.arguments.get(cursor + 1) else {
                        return Err(LocalRuntimeError::Usage);
                    };
                    if session_worker_bin.is_some() {
                        return Err(LocalRuntimeError::Usage);
                    }
                    session_worker_bin = Some(PathBuf::from(value));
                    cursor += 2;
                }
                _ => return Err(LocalRuntimeError::Usage),
            }
        }
        Ok(Self {
            data_directory: options.data_directory,
            session_worker_bin,
        })
    }

    fn session_worker_bin(&self, hub_bin: &Path) -> Result<PathBuf, LocalRuntimeError> {
        if let Some(path) = &self.session_worker_bin {
            if path.is_file() {
                return Ok(path.clone());
            }
            return Err(LocalRuntimeError::MissingSessionWorkerBinary(path.clone()));
        }
        let Some(bin_dir) = hub_bin.parent() else {
            return Err(LocalRuntimeError::MissingSessionWorkerBinary(
                PathBuf::from("botster-session-worker"),
            ));
        };
        let path = bin_dir.join("botster-session-worker");
        if path.is_file() {
            return Ok(path);
        }
        if bin_dir.file_name().and_then(|name| name.to_str()) == Some("deps")
            && let Some(debug_dir) = bin_dir.parent()
        {
            let debug_path = debug_dir.join("botster-session-worker");
            if debug_path.is_file() {
                return Ok(debug_path);
            }
            return Err(LocalRuntimeError::MissingSessionWorkerBinary(debug_path));
        }
        Err(LocalRuntimeError::MissingSessionWorkerBinary(path))
    }
}

struct SmokeOptions {
    runtime: LocalRuntimeOptions,
}

impl SmokeOptions {
    fn parse(args: Vec<String>) -> Result<Self, SmokeError> {
        Ok(Self {
            runtime: LocalRuntimeOptions::parse(args)?,
        })
    }
}

#[derive(Clone, Copy)]
enum LocalRuntimeDaemonOwnership {
    Started,
    Reused,
}

fn print_local_runtime_ready(
    outcome: &LocalRuntimeOutcome,
    status: &DaemonStatus,
    apps: &[DaemonApp],
) {
    let dir = outcome.options.data_directory.display();
    println!("runtime=ready");
    println!("data_dir=resolved:{dir}");
    println!(
        "daemon={}",
        match outcome.daemon_ownership {
            LocalRuntimeDaemonOwnership::Started => "started",
            LocalRuntimeDaemonOwnership::Reused => "reused",
        }
    );
    println!("protocol={}", status.compatibility.protocol);
    println!("protocol_version={}", status.compatibility.protocol_version);
    println!(
        "conformance_fixture_revision={}",
        status.compatibility.conformance_fixture_revision
    );
    println!("package_count={}", status.package_count);
    println!("enabled_package_count={}", status.enabled_package_count);
    println!("app_count={}", apps.len());
    for app in apps {
        println!(
            "app package={} app_id={} kind={} lifecycle_state={} local_url={}",
            app.package_name,
            app.entrypoint_id,
            app.kind,
            app.lifecycle_state,
            app.launch_target.local_url.as_deref().unwrap_or("none")
        );
    }
    println!(
        "package name=botster-web state={}",
        outcome.web.package_state
    );
    println!("web={}", outcome.web.local_url);
    println!("tui=botster-hub apps open --data-dir {dir} botster-tui");
    println!("mcp=botster-hub mcp-serve --data-dir {dir}");
    println!("status=botster-hub status --data-dir {dir}");
    println!("apps=botster-hub apps list --data-dir {dir}");
    println!("down=botster-hub down --data-dir {dir}");
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
                let options = DataArgs::parse(args[1..].to_vec(), "sessions list")?;
                if !options.arguments.is_empty() {
                    return Err(OperatorError::Usage("sessions list"));
                }
                Ok(Self {
                    data_directory: options.data_directory,
                    action: SessionAction::List,
                })
            }
            "spawn" => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("sessions spawn"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions spawn")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions attach")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions send-input")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions resize")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions detach")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "sessions shutdown")?;
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

struct AppCommand {
    data_directory: PathBuf,
    action: AppActionCommand,
}

enum AppActionCommand {
    List,
    Show(String),
    Open(String),
}

impl AppCommand {
    fn parse(args: Vec<String>) -> Result<Self, OperatorError> {
        let Some(action) = args.first().map(String::as_str) else {
            return Err(OperatorError::Usage("apps"));
        };
        match action {
            "list" => {
                let options = DataArgs::parse(args[1..].to_vec(), "apps list")?;
                if !options.arguments.is_empty() {
                    return Err(OperatorError::Usage("apps list"));
                }
                Ok(Self {
                    data_directory: options.data_directory,
                    action: AppActionCommand::List,
                })
            }
            "show" => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("apps show"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "apps show")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: AppActionCommand::Show(args[3].clone()),
                })
            }
            "open" => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("apps open"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "apps open")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: AppActionCommand::Open(args[3].clone()),
                })
            }
            _ => Err(OperatorError::Usage("apps")),
        }
    }
}

enum PackageActionCommand {
    List,
    Available(PathBuf),
    InspectAvailable {
        registry_path: PathBuf,
        entry_id: String,
    },
    PreviewInstall {
        registry_path: PathBuf,
        entry_id: String,
    },
    InstallRegistryEntry {
        registry_path: PathBuf,
        entry_id: String,
    },
    InstallLocalPath(PathBuf),
    Show(String),
    Config(String),
    SetConfig {
        package_name: String,
        values: BTreeMap<String, serde_json::Value>,
    },
    Reload(String),
    EnableLocalPath(PathBuf),
    EnableName(String),
    Disable(String),
    Remove(String),
    CheckUpdate(String),
    PreviewUpdate {
        package_name: String,
        pin: DaemonPackagePin,
    },
    ApplyUpdate {
        package_name: String,
        pin: DaemonPackagePin,
    },
    StartEntrypoint {
        package_name: String,
        entrypoint_id: String,
    },
    StopEntrypoint {
        package_name: String,
        entrypoint_id: String,
    },
    RestartEntrypoint {
        package_name: String,
        entrypoint_id: String,
    },
    EntrypointStatus {
        package_name: String,
        entrypoint_id: String,
    },
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
                let command = if providers_only {
                    "providers list"
                } else {
                    "packages list"
                };
                let options = DataArgs::parse(args[1..].to_vec(), command)?;
                if !options.arguments.is_empty() {
                    return Err(OperatorError::Usage(command));
                }
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::List,
                })
            }
            "available" if !providers_only => {
                if args.len() != 5 || args.get(3).map(String::as_str) != Some("--registry") {
                    return Err(OperatorError::Usage("packages available"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages available")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Available(PathBuf::from(&args[4])),
                })
            }
            "inspect" if !providers_only => {
                if args.len() != 6 || args.get(3).map(String::as_str) != Some("--registry") {
                    return Err(OperatorError::Usage("packages inspect"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages inspect")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::InspectAvailable {
                        registry_path: PathBuf::from(&args[4]),
                        entry_id: args[5].clone(),
                    },
                })
            }
            "preview-install" if !providers_only => {
                if args.len() != 6 || args.get(3).map(String::as_str) != Some("--registry") {
                    return Err(OperatorError::Usage("packages preview-install"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages preview-install")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::PreviewInstall {
                        registry_path: PathBuf::from(&args[4]),
                        entry_id: args[5].clone(),
                    },
                })
            }
            "install" if !providers_only => {
                if args.len() != 5 && args.len() != 6 {
                    return Err(OperatorError::Usage("packages install"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages install")?;
                match args.get(3).map(String::as_str) {
                    Some("--path") if args.len() == 5 => Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::InstallLocalPath(PathBuf::from(&args[4])),
                    }),
                    Some("--registry") if args.len() == 6 => Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::InstallRegistryEntry {
                            registry_path: PathBuf::from(&args[4]),
                            entry_id: args[5].clone(),
                        },
                    }),
                    _ => Err(OperatorError::Usage("packages install")),
                }
            }
            "show" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages show"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages show")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Show(args[3].clone()),
                })
            }
            "config" if !providers_only => {
                if args.get(1).map(String::as_str) == Some("set") {
                    if args.len() != 6 {
                        return Err(OperatorError::Usage("packages config set"));
                    }
                    let options = DataArgs::parse(args[2..4].to_vec(), "packages config set")?;
                    let values =
                        serde_json::from_str::<BTreeMap<String, serde_json::Value>>(&args[5])
                            .map_err(|_| OperatorError::Usage("packages config set"))?;
                    Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::SetConfig {
                            package_name: args[4].clone(),
                            values,
                        },
                    })
                } else {
                    if args.len() != 4 {
                        return Err(OperatorError::Usage("packages config"));
                    }
                    let options = DataArgs::parse(args[1..3].to_vec(), "packages config")?;
                    Ok(Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::Config(args[3].clone()),
                    })
                }
            }
            "enable" if !providers_only => {
                if args.len() < 4 {
                    return Err(OperatorError::Usage("packages enable"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages enable")?;
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
                let options = DataArgs::parse(args[1..3].to_vec(), "packages disable")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Disable(args[3].clone()),
                })
            }
            "remove" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages remove"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages remove")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Remove(args[3].clone()),
                })
            }
            "check-update" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages check-update"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages check-update")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::CheckUpdate(args[3].clone()),
                })
            }
            "reload" if !providers_only => {
                if args.len() != 4 {
                    return Err(OperatorError::Usage("packages reload"));
                }
                let options = DataArgs::parse(args[1..3].to_vec(), "packages reload")?;
                Ok(Self {
                    data_directory: options.data_directory,
                    action: PackageActionCommand::Reload(args[3].clone()),
                })
            }
            "preview-update" if !providers_only => parse_package_update_command(
                args,
                "packages preview-update",
                PackageUpdateCommandKind::Preview,
            ),
            "apply-update" if !providers_only => parse_package_update_command(
                args,
                "packages apply-update",
                PackageUpdateCommandKind::Apply,
            ),
            "start-entrypoint" if !providers_only => {
                parse_package_entrypoint_command(args, "packages start-entrypoint").map(
                    |(options, package_name, entrypoint_id)| Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::StartEntrypoint {
                            package_name,
                            entrypoint_id,
                        },
                    },
                )
            }
            "stop-entrypoint" if !providers_only => {
                parse_package_entrypoint_command(args, "packages stop-entrypoint").map(
                    |(options, package_name, entrypoint_id)| Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::StopEntrypoint {
                            package_name,
                            entrypoint_id,
                        },
                    },
                )
            }
            "restart-entrypoint" if !providers_only => {
                parse_package_entrypoint_command(args, "packages restart-entrypoint").map(
                    |(options, package_name, entrypoint_id)| Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::RestartEntrypoint {
                            package_name,
                            entrypoint_id,
                        },
                    },
                )
            }
            "entrypoint-status" if !providers_only => {
                parse_package_entrypoint_command(args, "packages entrypoint-status").map(
                    |(options, package_name, entrypoint_id)| Self {
                        data_directory: options.data_directory,
                        action: PackageActionCommand::EntrypointStatus {
                            package_name,
                            entrypoint_id,
                        },
                    },
                )
            }
            _ => Err(OperatorError::Usage(if providers_only {
                "providers list"
            } else {
                "packages"
            })),
        }
    }
}

enum PackageUpdateCommandKind {
    Preview,
    Apply,
}

fn parse_package_update_command(
    args: Vec<String>,
    usage: &'static str,
    kind: PackageUpdateCommandKind,
) -> Result<PackageCommand, OperatorError> {
    if args.len() < 6 {
        return Err(OperatorError::Usage(usage));
    }
    let options = DataArgs::parse(args[1..3].to_vec(), usage)?;
    let package_name = args[3].clone();
    let pin = parse_daemon_package_pin(&args[4..], usage)?;
    let action = match kind {
        PackageUpdateCommandKind::Preview => {
            PackageActionCommand::PreviewUpdate { package_name, pin }
        }
        PackageUpdateCommandKind::Apply => PackageActionCommand::ApplyUpdate { package_name, pin },
    };
    Ok(PackageCommand {
        data_directory: options.data_directory,
        action,
    })
}

fn parse_daemon_package_pin(
    args: &[String],
    usage: &'static str,
) -> Result<DaemonPackagePin, OperatorError> {
    let mut revision = None;
    let mut branch = None;
    let mut tag = None;
    let mut rev = None;
    let mut checksum = None;
    let mut update_policy = "manual".to_string();
    let mut cursor = 0;
    while cursor < args.len() {
        let Some(value) = args.get(cursor + 1) else {
            return Err(OperatorError::Usage(usage));
        };
        match args[cursor].as_str() {
            "--revision" => revision = Some(value.clone()),
            "--branch" => branch = Some(value.clone()),
            "--tag" => tag = Some(value.clone()),
            "--rev" => rev = Some(value.clone()),
            "--checksum" => checksum = Some(value.clone()),
            "--policy" if matches!(value.as_str(), "manual" | "track_source") => {
                update_policy = value.clone();
            }
            _ => return Err(OperatorError::Usage(usage)),
        }
        cursor += 2;
    }
    Ok(DaemonPackagePin {
        revision: revision.ok_or(OperatorError::Usage(usage))?,
        branch,
        tag,
        rev,
        checksum,
        update_policy,
    })
}

fn parse_package_entrypoint_command(
    args: Vec<String>,
    usage: &'static str,
) -> Result<(DataArgs, String, String), OperatorError> {
    if args.len() != 5 {
        return Err(OperatorError::Usage(usage));
    }
    let options = DataArgs::parse(args[1..3].to_vec(), usage)?;
    Ok((options, args[3].clone(), args[4].clone()))
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
        let options = DataArgs::parse(args[0..2].to_vec(), "inspect")?;
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
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))?;

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
enum LocalRuntimeError {
    Usage,
    CurrentExe(io::Error),
    CreateDataDir {
        path: PathBuf,
        source: io::Error,
    },
    MissingHubBinary(PathBuf),
    MissingSessionWorkerBinary(PathBuf),
    SpawnDaemon {
        path: PathBuf,
        source: io::Error,
    },
    PollDaemon(io::Error),
    ReadinessTimeout {
        elapsed: Duration,
        readiness_budget: Duration,
        last_probe: String,
        child_pid: u32,
        child_status: String,
    },
    MissingLocalSocket,
    WriteDaemonMetadata {
        path: PathBuf,
        source: io::Error,
    },
    ReadDaemonMetadata {
        path: PathBuf,
        source: io::Error,
    },
    ReadDaemonMetadataJson(serde_json::Error),
    RemoveDaemonMetadata {
        path: PathBuf,
        source: io::Error,
    },
    RemoveLocalSocket {
        path: PathBuf,
        source: io::Error,
    },
    SerializeMetadata(serde_json::Error),
    InspectProcess(io::Error),
    TerminateDaemon(io::Error),
    TerminateDaemonTimeout(u32),
    Config(botster_hub::HubConfigError),
    Transport(botster_hub::DaemonTransportError),
    MissingPackage {
        label: &'static str,
    },
    PackageDisabled {
        package_name: &'static str,
        state: String,
    },
    MissingWebEntrypoint,
    WebEntrypointStart(String),
    WebHealth(String),
    WebUi(String),
    WebRequest {
        label: &'static str,
        message: String,
    },
    Operator(Box<OperatorError>),
    IncompatibleDaemon(String),
    DaemonExited {
        status: String,
        elapsed: Duration,
        readiness_budget: Duration,
        last_probe: String,
        stderr_tail: String,
    },
}

#[derive(Debug)]
enum SmokeError {
    Usage,
    Clock,
    Runtime(LocalRuntimeError),
    Transport(botster_hub::DaemonTransportError),
    UnexpectedResponse(&'static str),
    MissingPrerequisite(&'static str),
    OperatorResponse(String),
    SessionRoundTrip(String),
    Webrtc(String),
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
    Package(botster_hub::PackageRegistryError),
    State(botster_hub::HubStateStoreError),
    App(String),
    SpawnApp(io::Error),
    Serialize(serde_json::Error),
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

impl fmt::Display for LocalRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "{}", usage_for("up")),
            Self::CurrentExe(error) => {
                write!(formatter, "resolve current botster-hub binary: {error}")
            }
            Self::CreateDataDir { path, source } => {
                write!(
                    formatter,
                    "create local runtime data dir {}: {source}",
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
                    "spawn local runtime daemon {}: {source}",
                    path.display()
                )
            }
            Self::PollDaemon(error) => write!(formatter, "poll local runtime daemon: {error}"),
            Self::ReadinessTimeout {
                elapsed,
                readiness_budget,
                last_probe,
                child_pid,
                child_status,
            } => {
                write!(
                    formatter,
                    "timed out waiting for local runtime daemon readiness after {elapsed:?} (budget {readiness_budget:?}); last status probe: {last_probe}; terminated owned child_pid={child_pid} child_status={child_status}"
                )
            }
            Self::MissingLocalSocket => write!(formatter, "local socket transport is disabled"),
            Self::WriteDaemonMetadata { path, source } => {
                write!(
                    formatter,
                    "write local runtime daemon metadata {}: {source}",
                    path.display()
                )
            }
            Self::ReadDaemonMetadata { path, source } => {
                write!(
                    formatter,
                    "read local runtime daemon metadata {}: {source}",
                    path.display()
                )
            }
            Self::ReadDaemonMetadataJson(error) => {
                write!(formatter, "parse local runtime daemon metadata: {error}")
            }
            Self::RemoveDaemonMetadata { path, source } => {
                write!(
                    formatter,
                    "remove local runtime daemon metadata {}: {source}",
                    path.display()
                )
            }
            Self::RemoveLocalSocket { path, source } => {
                write!(
                    formatter,
                    "remove stale local runtime local socket {}: {source}",
                    path.display()
                )
            }
            Self::SerializeMetadata(error) => {
                write!(
                    formatter,
                    "serialize local runtime daemon metadata: {error}"
                )
            }
            Self::InspectProcess(error) => {
                write!(formatter, "inspect local runtime daemon process: {error}")
            }
            Self::TerminateDaemon(error) => {
                write!(formatter, "terminate stale local runtime daemon: {error}")
            }
            Self::TerminateDaemonTimeout(pid) => {
                write!(
                    formatter,
                    "timed out waiting for stale local runtime daemon process {pid} to exit"
                )
            }
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::MissingPackage { label } => {
                write!(
                    formatter,
                    "package {label} is not installed; install it with `botster-hub packages install [--data-dir <path>] --path <package-path>` and enable it before running `botster-hub up`"
                )
            }
            Self::PackageDisabled {
                package_name,
                state,
            } => {
                write!(
                    formatter,
                    "package {package_name} must be enabled before `botster-hub up`; current state={state}"
                )
            }
            Self::MissingWebEntrypoint => write!(
                formatter,
                "package botster-web must declare runnable entrypoint web-client"
            ),
            Self::WebEntrypointStart(message) => write!(
                formatter,
                "start package botster-web entrypoint web-client: {message}"
            ),
            Self::WebHealth(message) => {
                write!(formatter, "verify botster-web/web-client health: {message}")
            }
            Self::WebUi(message) => {
                write!(formatter, "verify botster-web/web-client UI: {message}")
            }
            Self::WebRequest { label, message } => {
                write!(
                    formatter,
                    "request botster-web/web-client {label}: {message}"
                )
            }
            Self::Operator(error) => write!(formatter, "{error}"),
            Self::IncompatibleDaemon(message) => write!(
                formatter,
                "running daemon is incompatible or stale: {message}; `botster-hub down` may fail against this daemon because shutdown uses the same protocol handshake. Stop the running botster-hub process directly, remove the stale local socket for this data dir if one remains, then retry `botster-hub up [--data-dir <path>]`"
            ),
            Self::DaemonExited {
                status,
                elapsed,
                readiness_budget,
                last_probe,
                stderr_tail,
            } => {
                write!(
                    formatter,
                    "local runtime daemon exited with {status} after {elapsed:?} (readiness budget {readiness_budget:?}); last status probe: {last_probe}; daemon error: {stderr_tail}"
                )
            }
        }
    }
}

impl fmt::Display for SmokeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(formatter, "{}", usage_for("smoke")),
            Self::Clock => write!(formatter, "system clock is before unix epoch"),
            Self::Runtime(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::UnexpectedResponse(response) => {
                write!(formatter, "unexpected client API response for {response}")
            }
            Self::MissingPrerequisite(name) => {
                write!(formatter, "missing_prerequisite={name}")
            }
            Self::OperatorResponse(message) => write!(formatter, "operator response: {message}"),
            Self::SessionRoundTrip(observed) => write!(
                formatter,
                "session terminal round trip did not observe marker; observed_bytes={}",
                observed.len()
            ),
            Self::Webrtc(message) => write!(formatter, "local_webrtc={message}"),
        }
    }
}

impl From<botster_hub::HubConfigError> for LocalRuntimeError {
    fn from(error: botster_hub::HubConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<botster_hub::DaemonTransportError> for LocalRuntimeError {
    fn from(error: botster_hub::DaemonTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<OperatorError> for LocalRuntimeError {
    fn from(error: OperatorError) -> Self {
        Self::Operator(Box::new(error))
    }
}

impl From<LocalRuntimeError> for SmokeError {
    fn from(error: LocalRuntimeError) -> Self {
        match error {
            LocalRuntimeError::Usage => Self::Usage,
            LocalRuntimeError::MissingPackage { label } => Self::MissingPrerequisite(label),
            other => Self::Runtime(other),
        }
    }
}

impl From<botster_hub::DaemonTransportError> for SmokeError {
    fn from(error: botster_hub::DaemonTransportError) -> Self {
        Self::Transport(error)
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
            Self::Package(error) => write!(formatter, "package policy error: {error:?}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::App(message) => write!(formatter, "{message}"),
            Self::SpawnApp(error) => write!(formatter, "spawn app command: {error}"),
            Self::Serialize(error) => write!(formatter, "serialize response: {error}"),
        }
    }
}

fn print_global_help() {
    println!("{}", usage_for("global"));
}

fn usage_for(command: &str) -> &'static str {
    match command {
        "global" => {
            "usage: botster-hub [<command> [args...]]

Interactive operator console:
  botster-hub
    Starts or reuses the resolved local daemon when stdin/stdout are terminals.
    Use exit or Ctrl-D to detach; use down to stop the daemon.
    Noninteractive callers must use an explicit command.

Daily runtime commands:
  botster-hub up [--data-dir <path>] [...]
  botster-hub down [--data-dir <path>]
  botster-hub status [--data-dir <path>]
  botster-hub doctor [--data-dir <path>]
  botster-hub smoke [--data-dir <path>] [...]
  botster-hub open web [--data-dir <path>]
  botster-hub open tui [--data-dir <path>]
  botster-hub mcp-serve [--data-dir <path>]

Apps:
  botster-hub apps list [--data-dir <path>]
  botster-hub apps show [--data-dir <path>] <app|package/app>
  botster-hub apps open [--data-dir <path>] <app|package/app>

Spawn targets:
  botster-hub spawn-targets list [--data-dir <path>]
  botster-hub spawn-targets show [--data-dir <path>] <target-id>
  botster-hub spawn-targets create [--data-dir <path>] --root <dir> [--id <id>] [--label <label>] [--kind directory] [--disabled]
  botster-hub spawn-targets update [--data-dir <path>] <target-id> [--label <label>] [--root <dir>] [--enable|--disable]
  botster-hub spawn-targets delete [--data-dir <path>] <target-id>
  botster-hub spawn-targets validate [--data-dir <path>] <target-id>

Packages:
  botster-hub packages list [--data-dir <path>]
  botster-hub packages available [--data-dir <path>] --registry <registry-dir-or-file>
  botster-hub packages inspect [--data-dir <path>] --registry <registry-dir-or-file> <entry-id>
  botster-hub packages preview-install [--data-dir <path>] --registry <registry-dir-or-file> <entry-id>
  botster-hub packages install [--data-dir <path>] (--path <package-dir-or-manifest>|--registry <registry-dir-or-file> <entry-id>)
  botster-hub packages show [--data-dir <path>] <name>
  botster-hub packages config [--data-dir <path>] <name>
  botster-hub packages config set [--data-dir <path>] <name> '<json-object>'
  botster-hub packages enable [--data-dir <path>] (--path <package-dir-or-manifest>|<name>)
  botster-hub packages disable [--data-dir <path>] <name>
  botster-hub packages remove [--data-dir <path>] <name>
  botster-hub packages reload [--data-dir <path>] <name>
  botster-hub reload <name> [--data-dir <path>]
  botster-hub packages check-update [--data-dir <path>] <name>
  botster-hub packages preview-update [--data-dir <path>] <name> --revision <revision> [...]
  botster-hub packages apply-update [--data-dir <path>] <name> --revision <revision> [...]
  botster-hub packages start-entrypoint [--data-dir <path>] <package> <entrypoint>
  botster-hub packages stop-entrypoint [--data-dir <path>] <package> <entrypoint>
  botster-hub packages restart-entrypoint [--data-dir <path>] <package> <entrypoint>
  botster-hub packages entrypoint-status [--data-dir <path>] <package> <entrypoint>"
        }
        "start" => "usage: botster-hub start [--data-dir <path>] [--session-worker-bin <path>]",
        "up" => {
            "usage: botster-hub up [--data-dir <path>] [--session-worker-bin <path>]"
        }
        "down" => "usage: botster-hub down [--data-dir <path>]",
        "doctor" => "usage: botster-hub doctor [--data-dir <path>]",
        "smoke" => {
            "usage: botster-hub smoke [--data-dir <path>] [--session-worker-bin <path>]"
        }
        "status" => "usage: botster-hub status [--data-dir <path>]",
        "sessions" => {
            "usage: botster-hub sessions <list|spawn|attach|send-input|resize|detach|shutdown> ..."
        }
        "session-templates" => "usage: botster-hub session-templates <list|show|resolve|spawn> ...",
        "session-templates list" => "usage: botster-hub session-templates list [--data-dir <path>]",
        "session-templates show" => {
            "usage: botster-hub session-templates show [--data-dir <path>] <template-id>"
        }
        "session-templates resolve" => {
            "usage: botster-hub session-templates resolve [--data-dir <path>] <template-id> [--target-id <id>] [--cwd <path>] [--env NAME=value] [--prompt <text>] [--branch <name>] [--ticket-id <id>] [--workspace-id <id>]"
        }
        "session-templates spawn" => {
            "usage: botster-hub session-templates spawn [--data-dir <path>] <template-id> --session-id <id> [--target-id <id>] [--cwd <path>] [--env NAME=value] [--prompt <text>] [--branch <name>] [--ticket-id <id>] [--workspace-id <id>]"
        }
        "spawn-targets" | "spawn-targets list" => {
            "usage: botster-hub spawn-targets list [--data-dir <path>]"
        }
        "spawn-targets show" => {
            "usage: botster-hub spawn-targets show [--data-dir <path>] <target-id>"
        }
        "spawn-targets create" => {
            "usage: botster-hub spawn-targets create [--data-dir <path>] --root <dir> [--id <id>] [--label <label>] [--kind directory] [--disabled]"
        }
        "spawn-targets update" => {
            "usage: botster-hub spawn-targets update [--data-dir <path>] <target-id> [--label <label>] [--root <dir>] [--kind directory] [--enable|--disable]"
        }
        "spawn-targets delete" => {
            "usage: botster-hub spawn-targets delete [--data-dir <path>] <target-id>"
        }
        "spawn-targets validate" => {
            "usage: botster-hub spawn-targets validate [--data-dir <path>] <target-id>"
        }
        "context" => {
            "usage: botster-hub context [--data-dir <path>] [--session-id <id>] [--context-id <id>] [--key <name>]"
        }
        "sessions list" => "usage: botster-hub sessions list [--data-dir <path>]",
        "sessions spawn" => {
            "usage: botster-hub sessions spawn [--data-dir <path>] [--session-id <id>] -- <command>"
        }
        "sessions attach" => {
            "usage: botster-hub sessions attach [--data-dir <path>] <session-id> [--subscription-id <id>]"
        }
        "sessions resize" => {
            "usage: botster-hub sessions resize [--data-dir <path>] <session-id> <rows> <cols>"
        }
        "sessions detach" => {
            "usage: botster-hub sessions detach [--data-dir <path>] <session-id> [--subscription-id <id>]"
        }
        "sessions send-input" => {
            "usage: botster-hub sessions send-input [--data-dir <path>] <session-id> -- <bytes>"
        }
        "sessions shutdown" => {
            "usage: botster-hub sessions shutdown [--data-dir <path>] <session-id>"
        }
        "shutdown" => "usage: botster-hub shutdown [--data-dir <path>]",
        "mcp-serve" => "usage: botster-hub mcp-serve [--data-dir <path>]",
        "open" => "usage: botster-hub open <web|tui> [--data-dir <path>]",
        "apps" => "usage: botster-hub apps <list|show|open> ...",
        "apps list" => "usage: botster-hub apps list [--data-dir <path>]",
        "apps show" => "usage: botster-hub apps show [--data-dir <path>] <app|package/app>",
        "apps open" => "usage: botster-hub apps open [--data-dir <path>] <app|package/app>",
        "packages" => {
            "usage: botster-hub packages <available|inspect|preview-install|install|list|show|config|enable|disable|remove|reload|check-update|preview-update|apply-update|start-entrypoint|stop-entrypoint|restart-entrypoint|entrypoint-status> ..."
        }
        "packages install" => {
            "usage: botster-hub packages install [--data-dir <path>] (--path <package-dir-or-manifest>|--registry <registry-dir-or-file> <entry-id>)"
        }
        "packages available" => {
            "usage: botster-hub packages available [--data-dir <path>] --registry <registry-dir-or-file>"
        }
        "packages inspect" => {
            "usage: botster-hub packages inspect [--data-dir <path>] --registry <registry-dir-or-file> <entry-id>"
        }
        "packages preview-install" => {
            "usage: botster-hub packages preview-install [--data-dir <path>] --registry <registry-dir-or-file> <entry-id>"
        }
        "packages list" => "usage: botster-hub packages list [--data-dir <path>]",
        "packages show" => "usage: botster-hub packages show [--data-dir <path>] <name>",
        "packages config" => "usage: botster-hub packages config [--data-dir <path>] <name>",
        "packages config set" => {
            "usage: botster-hub packages config set [--data-dir <path>] <name> '<json-object>'"
        }
        "packages enable" => {
            "usage: botster-hub packages enable [--data-dir <path>] (--path <package-dir-or-manifest>|<name>)"
        }
        "packages disable" => "usage: botster-hub packages disable [--data-dir <path>] <name>",
        "packages remove" => "usage: botster-hub packages remove [--data-dir <path>] <name>",
        "reload" => "usage: botster-hub reload <name> [--data-dir <path>]",
        "packages check-update" => {
            "usage: botster-hub packages check-update [--data-dir <path>] <name>"
        }
        "packages reload" => "usage: botster-hub packages reload [--data-dir <path>] <name>",
        "packages preview-update" => {
            "usage: botster-hub packages preview-update [--data-dir <path>] <name> --revision <revision> [--branch <branch>] [--tag <tag>] [--rev <rev>] [--checksum <checksum>] [--policy manual|track_source]"
        }
        "packages apply-update" => {
            "usage: botster-hub packages apply-update [--data-dir <path>] <name> --revision <revision> [--branch <branch>] [--tag <tag>] [--rev <rev>] [--checksum <checksum>] [--policy manual|track_source]"
        }
        "packages start-entrypoint" => {
            "usage: botster-hub packages start-entrypoint [--data-dir <path>] <package> <entrypoint>"
        }
        "packages stop-entrypoint" => {
            "usage: botster-hub packages stop-entrypoint [--data-dir <path>] <package> <entrypoint>"
        }
        "packages restart-entrypoint" => {
            "usage: botster-hub packages restart-entrypoint [--data-dir <path>] <package> <entrypoint>"
        }
        "packages entrypoint-status" => {
            "usage: botster-hub packages entrypoint-status [--data-dir <path>] <package> <entrypoint>"
        }
        "providers" | "providers list" => "usage: botster-hub providers list [--data-dir <path>]",
        "inspect" => "usage: botster-hub inspect [--data-dir <path>] <session-id>",
        _ => {
            "usage: botster-hub <help|up|down|doctor|smoke|open|reload|start|status|sessions|shutdown|mcp-serve|apps|packages|providers|inspect|run-one>"
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

#[cfg(test)]
mod cli_data_dir_tests {
    use super::*;

    fn environment() -> RuntimeEnvironment {
        RuntimeEnvironment::from_values(None, Some(PathBuf::from("/tmp/botster-cli-home")))
    }

    #[test]
    fn operator_console_requires_terminal_stdin_and_stdout() {
        assert!(should_open_operator_console(true, true));
        assert!(!should_open_operator_console(true, false));
        assert!(!should_open_operator_console(false, true));
        assert!(!should_open_operator_console(false, false));
    }

    #[test]
    fn every_stateful_command_family_receives_the_home_runtime_root() {
        let expected = "/tmp/botster-cli-home/.botster/hub";
        let cases = [
            ("start", vec![], 0),
            ("up", vec![], 0),
            ("down", vec![], 0),
            ("doctor", vec![], 0),
            ("smoke", vec![], 0),
            ("status", vec![], 0),
            ("shutdown", vec![], 0),
            ("mcp-serve", vec![], 0),
            ("inspect", vec!["session"], 0),
            ("sessions", vec!["list"], 1),
            ("session-templates", vec!["list"], 1),
            ("spawn-targets", vec!["list"], 1),
            ("context", vec!["--session-id", "session"], 0),
            ("open", vec!["web"], 1),
            ("reload", vec!["botster-web"], 1),
            ("apps", vec!["list"], 1),
            ("packages", vec!["list"], 1),
            ("providers", vec!["list"], 1),
            ("packages", vec!["config", "set", "demo", "{}"], 2),
        ];

        for (command, args, insert_at) in cases {
            let resolved = canonicalize_data_dir_args_for_environment(
                command,
                args.into_iter().map(str::to_string).collect(),
                &environment(),
            )
            .expect("resolve command data directory");
            assert_eq!(resolved[insert_at], "--data-dir", "{command}");
            assert_eq!(resolved[insert_at + 1], expected, "{command}");
        }
    }

    #[test]
    fn explicit_data_dir_wins_and_can_appear_after_operands() {
        let resolved = canonicalize_data_dir_args_for_environment(
            "packages",
            vec![
                "show".to_string(),
                "demo".to_string(),
                "--data-dir".to_string(),
                "/tmp/explicit".to_string(),
            ],
            &RuntimeEnvironment::from_values(
                Some(PathBuf::from("/tmp/environment")),
                Some(PathBuf::from("/tmp/botster-fixture-home")),
            ),
        )
        .expect("resolve explicit data directory");

        assert_eq!(resolved, ["show", "--data-dir", "/tmp/explicit", "demo"]);
    }

    #[test]
    fn environment_data_dir_wins_over_home() {
        let resolved = canonicalize_data_dir_args_for_environment(
            "status",
            Vec::new(),
            &RuntimeEnvironment::from_values(
                Some(PathBuf::from("/tmp/environment")),
                Some(PathBuf::from("/tmp/botster-fixture-home")),
            ),
        )
        .expect("resolve environment data directory");

        assert_eq!(resolved, ["--data-dir", "/tmp/environment"]);
    }

    #[test]
    fn duplicate_or_missing_data_dir_values_are_usage_errors() {
        for args in [
            vec!["--data-dir".to_string()],
            vec![
                "--data-dir".to_string(),
                "/tmp/one".to_string(),
                "--data-dir".to_string(),
                "/tmp/two".to_string(),
            ],
        ] {
            assert!(matches!(
                canonicalize_data_dir_args_for_environment("start", args, &environment()),
                Err(OperatorError::Usage("start"))
            ));
        }
    }

    #[test]
    fn data_dir_tokens_after_operand_separator_are_not_cli_options() {
        let resolved = canonicalize_data_dir_args_for_environment(
            "sessions",
            vec![
                "spawn".to_string(),
                "--".to_string(),
                "printf".to_string(),
                "--data-dir".to_string(),
            ],
            &environment(),
        )
        .expect("resolve session spawn data directory");

        assert_eq!(
            resolved,
            [
                "spawn",
                "--data-dir",
                "/tmp/botster-cli-home/.botster/hub",
                "--",
                "printf",
                "--data-dir"
            ]
        );
    }

    #[test]
    fn thin_data_args_parser_also_stops_at_operand_separator() {
        let parsed = DataArgs::parse(
            vec![
                "--data-dir".to_string(),
                "/tmp/explicit".to_string(),
                "--".to_string(),
                "--data-dir".to_string(),
                "/tmp/operand".to_string(),
            ],
            "test",
        )
        .expect("parse explicit data directory and operand tail");

        assert_eq!(parsed.data_directory, PathBuf::from("/tmp/explicit"));
        assert_eq!(
            parsed.arguments,
            ["--", "--data-dir", "/tmp/operand"],
            "operand-tail tokens must not be consumed as CLI options"
        );
    }

    #[test]
    fn help_marks_data_dir_optional_for_start_smoke_and_command_families() {
        for command in [
            "start",
            "smoke",
            "sessions list",
            "session-templates list",
            "apps list",
            "packages list",
            "mcp-serve",
        ] {
            assert!(
                usage_for(command).contains("[--data-dir <path>]"),
                "{command} usage must mark --data-dir optional"
            );
        }
    }
}
