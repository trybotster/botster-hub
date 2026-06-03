use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    ClientId, CoreSessionMetadata, RequestId, ResizePayload, SessionId, SessionSpawnRequest,
    SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId, TransportEgress,
};
use botster_hub::{
    DataDirectoryOption, HubRuntime, HubStartupOptions, RuntimeEnvironment, SessionDefaults,
    TransportBindings, build_default_config_for_runtime, host_profile,
};

const SMOKE_MARKER: &str = "botster-hub-smoke-ok";
const SMOKE_TIMEOUT: Duration = Duration::from_secs(5);

fn main() {
    if env::args().nth(1).as_deref() == Some("run-one") {
        if let Err(error) = run_one(env::args().skip(2).collect()) {
            eprintln!("botster-hub run-one error: {error}");
            process::exit(1);
        }
        return;
    }

    match boot_runtime() {
        Ok(runtime) => {
            let profile = host_profile();
            println!(
                "{} first-party host profile ready for {}: {} roles, {} core capability surfaces",
                profile.id,
                runtime.config().host.id,
                profile.responsibilities().len(),
                profile.capability_surfaces().len()
            );
        }
        Err(error) => {
            eprintln!("botster-hub boot error: {error}");
            process::exit(1);
        }
    }
}

fn boot_runtime() -> Result<HubRuntime, BootError> {
    let environment = RuntimeEnvironment::from_current_process();
    let config = build_default_config_for_runtime(&environment).map_err(BootError::Config)?;
    HubRuntime::load(config).map_err(BootError::State)
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
        client_id,
        session_id.clone(),
        subscription_id,
        logical_clock,
    )?;
    logical_clock += 1;

    let observed = drain_until_marker(&mut runtime, &session_id, &mut logical_clock)?;
    let shutdown =
        runtime.shutdown_session(session_id.clone(), "run-one complete", logical_clock)?;

    println!(
        "{} first-party host profile booted for {} through DefaultBotsterEngine",
        profile.id, host_id
    );
    println!("spawned_session={}", spawn.handle.session_id.0);
    println!("observed_marker={SMOKE_MARKER}");
    println!("observed_bytes={}", observed.len());
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
enum RunOneError {
    Usage,
    Config(botster_hub::HubConfigError),
    Runtime(botster_hub::HubRuntimeError),
    State(botster_hub::HubStateStoreError),
    TimedOut,
}

#[derive(Debug)]
enum BootError {
    Config(botster_hub::HubConfigError),
    State(botster_hub::HubStateStoreError),
}

impl fmt::Display for BootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
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
