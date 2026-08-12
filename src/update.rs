//! Source-checkout update orchestration for one resolved Botster stack.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
#[cfg(test)]
use std::os::unix::process::ExitStatusExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::PackageSource;
use botster_hub::{
    DaemonPackage, DaemonRequest, FileHubStateStore, HubState, PackageRecord,
    daemon_transport_request,
};
use serde::Deserialize;

use super::{
    DataArgs, LocalRuntimeOptions, complete_owned_runtime_daemon_shutdown, explicit_config,
    owned_runtime_daemon_pid, read_runtime_daemon_metadata, spawn_local_runtime_daemon,
};

const SOURCE_LOCK_FILE: &str = ".botster-update.lock";
const REPLACE_LOCK_FILE: &str = ".botster-hub-update.lock";
const PACKAGE_CONTRACT_FILE: &str = "botster-update.json";
const CORE_SIDECAR_SUFFIX: &str = "core-revision";
const GIT_TIMEOUT: Duration = Duration::from_secs(120);
const CARGO_TIMEOUT: Duration = Duration::from_secs(1_800);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateScope {
    Core,
    All,
}

impl UpdateScope {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "core" => Ok(Self::Core),
            "all" => Ok(Self::All),
            _ => Err(usage()),
        }
    }
}

#[derive(Debug)]
struct UpdateOptions {
    scope: UpdateScope,
    data_directory: PathBuf,
}

impl UpdateOptions {
    fn parse(args: Vec<String>) -> Result<Self, String> {
        let args = DataArgs::parse(args, "update").map_err(|_| usage())?;
        if args.arguments.len() != 1 {
            return Err(usage());
        }
        Ok(Self {
            scope: UpdateScope::parse(&args.arguments[0])?,
            data_directory: args.data_directory,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageBuildContract {
    steps: Vec<PackageBuildStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageBuildStep {
    argv: Vec<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    timeout_seconds: u64,
}

#[derive(Debug, Clone)]
struct LocalPackage {
    name: String,
    root: PathBuf,
}

struct PackageSelection {
    build: Vec<LocalPackage>,
    enabled: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectLocalPackageCandidate {
    name: String,
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EntrypointIdentity {
    package_name: String,
    entrypoint_id: String,
}

struct UpdateVerification<'a> {
    data_directory: &'a Path,
    hub_revision: &'a str,
    core_revision: &'a str,
    worker_bin: &'a Path,
    sidecar: &'a Path,
    enabled_packages: &'a BTreeSet<String>,
    restored_entrypoints: &'a [EntrypointIdentity],
}

struct UpdateLock {
    _file: File,
}

impl UpdateLock {
    fn acquire(path: &Path, label: &str) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create {label} lock directory: {error}"))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| format!("open {label} lock {}: {error}", path.display()))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Err(format!("{label} lock is held: {}", path.display()));
            }
            return Err(format!("acquire {label} lock {}: {error}", path.display()));
        }
        Ok(Self { _file: file })
    }
}

#[derive(Debug)]
struct CommandResult {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

trait CommandRunner {
    fn capture(
        &mut self,
        program: &str,
        args: &[String],
        cwd: &Path,
        timeout: Duration,
    ) -> Result<CommandResult, String>;

    fn stream(
        &mut self,
        program: &str,
        args: &[String],
        cwd: &Path,
        timeout: Duration,
        environment: &[(&str, &str)],
    ) -> Result<ExitStatus, String>;
}

struct ProcessCommandRunner;

impl CommandRunner for ProcessCommandRunner {
    fn capture(
        &mut self,
        program: &str,
        args: &[String],
        cwd: &Path,
        timeout: Duration,
    ) -> Result<CommandResult, String> {
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("start {program}: {error}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("capture {program} stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| format!("capture {program} stderr"))?;
        let stdout_reader = thread::spawn(move || read_command_output(stdout));
        let stderr_reader = thread::spawn(move || read_command_output(stderr));
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("wait for {program}: {error}"))?
            {
                break status;
            }
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                }
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!("{program} timed out after {}s", timeout.as_secs()));
            }
            thread::sleep(Duration::from_millis(25));
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| format!("{program} stdout reader panicked"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| format!("{program} stderr reader panicked"))??;
        Ok(CommandResult {
            status,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }

    fn stream(
        &mut self,
        program: &str,
        args: &[String],
        cwd: &Path,
        timeout: Duration,
        environment: &[(&str, &str)],
    ) -> Result<ExitStatus, String> {
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        for (name, value) in environment {
            command.env(name, value);
        }
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("start {program}: {error}"))?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("wait for {program}: {error}"))?
            {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGTERM);
                }
                let grace = Instant::now() + Duration::from_secs(2);
                while Instant::now() < grace {
                    if let Some(status) = child
                        .try_wait()
                        .map_err(|error| format!("wait for timed out {program}: {error}"))?
                    {
                        return Err(format!("{program} timed out with status {status}"));
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                }
                let _ = child.wait();
                return Err(format!("{program} timed out after {}s", timeout.as_secs()));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

fn read_command_output(mut reader: impl Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read command output: {error}"))?;
    Ok(bytes)
}

pub(super) fn run(args: Vec<String>) -> Result<(), String> {
    let options = UpdateOptions::parse(args)?;
    let source_root = if std::env::var("BOTSTER_ENV").as_deref() == Ok("test") {
        std::env::var_os("BOTSTER_HUB_TEST_UPDATE_SOURCE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    };
    execute(options, &source_root, &mut ProcessCommandRunner)
}

fn execute(
    options: UpdateOptions,
    source_root: &Path,
    runner: &mut dyn CommandRunner,
) -> Result<(), String> {
    let source_lock = source_root.join(".git").join(SOURCE_LOCK_FILE);
    let _source_lock = UpdateLock::acquire(&source_lock, "source build")?;
    let replace_lock = options.data_directory.join(REPLACE_LOCK_FILE);
    let _replace_lock = UpdateLock::acquire(&replace_lock, "daemon replace")?;

    let config =
        explicit_config(options.data_directory.clone()).map_err(|error| error.to_string())?;
    let daemon_running = match daemon_transport_request(&config, DaemonRequest::Status) {
        Ok(_) => true,
        Err(
            botster_hub::DaemonTransportError::NotRunning
            | botster_hub::DaemonTransportError::ClientDisconnected,
        ) => false,
        Err(error) => return Err(format!("probe running daemon: {error}")),
    };
    let old_pid = if daemon_running {
        owned_runtime_daemon_pid(&options.data_directory, &config)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "running daemon has no matching owned runtime metadata".to_string())?
            .into()
    } else {
        None
    };

    let selection = selected_packages(
        options.scope,
        &options.data_directory,
        &config,
        daemon_running,
    )?;

    ensure_clean_git_repository(source_root, "botster-hub", runner)?;
    for package in &selection.build {
        ensure_clean_git_repository(&package.root, &package.name, runner)?;
    }
    for package in &selection.build {
        load_package_contract(package)?;
    }

    fast_forward_repository(source_root, "botster-hub", runner)?;
    for package in &selection.build {
        fast_forward_repository(&package.root, &package.name, runner)?;
    }
    let package_contracts = selection
        .build
        .iter()
        .map(load_package_contract)
        .collect::<Result<Vec<_>, _>>()?;

    run_required(
        runner,
        "cargo",
        &["update", "-p", "botster-core", "-p", "botster-core-daemon"],
        source_root,
        CARGO_TIMEOUT,
        &[],
        "update locked Core dependencies",
    )?;
    let core_revision = resolved_core_revision(&source_root.join("Cargo.lock"))?;
    persist_core_lock_pin(source_root, runner)?;
    let hub_revision = git_output(source_root, &["rev-parse", "HEAD"], runner, GIT_TIMEOUT)?;

    run_required(
        runner,
        "cargo",
        &["build", "--locked", "-p", "botster-hub"],
        source_root,
        CARGO_TIMEOUT,
        &[("BOTSTER_BUILD_REVISION", hub_revision.as_str())],
        "build Hub",
    )?;
    run_required(
        runner,
        "cargo",
        &[
            "build",
            "--locked",
            "-p",
            "botster-core-daemon",
            "--bin",
            "botster-session-worker",
        ],
        source_root,
        CARGO_TIMEOUT,
        &[],
        "build session worker",
    )?;

    for (package, contract) in selection.build.iter().zip(&package_contracts) {
        run_package_contract(package, contract, runner)?;
    }

    let target_dir = cargo_target_directory(source_root);
    let hub_bin = target_dir.join("debug").join("botster-hub");
    let worker_bin = target_dir.join("debug").join("botster-session-worker");
    require_file(&hub_bin, "built Hub")?;
    require_file(&worker_bin, "built session worker")?;
    let sidecar = worker_sidecar(&worker_bin);
    fs::write(&sidecar, format!("{core_revision}\n"))
        .map_err(|error| format!("write worker Core revision sidecar: {error}"))?;

    let running_entrypoints = if daemon_running {
        snapshot_running_entrypoints(&config)?
    } else {
        Vec::new()
    };

    if daemon_running {
        let response = daemon_transport_request(&config, DaemonRequest::DaemonShutdown)
            .map_err(|error| format!("stop old daemon: {error}"))?;
        if response.kind != botster_hub::DaemonResponseKind::Shutdown {
            return Err("old daemon returned an unexpected shutdown response".to_string());
        }
        complete_owned_runtime_daemon_shutdown(&options.data_directory, &config, old_pid)
            .map_err(|error| format!("complete old daemon shutdown: {error}"))?;
    }

    let runtime_options = LocalRuntimeOptions {
        data_directory: options.data_directory.clone(),
        session_worker_bin: Some(worker_bin.clone()),
    };
    spawn_local_runtime_daemon(&hub_bin, &runtime_options, &config).map_err(|error| {
        format!(
            "start updated daemon: {error}; recovery: {} start --data-dir {} --session-worker-bin {}",
            hub_bin.display(),
            options.data_directory.display(),
            worker_bin.display()
        )
    })?;
    let new_pid = read_runtime_daemon_metadata(&options.data_directory)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "updated daemon did not write runtime metadata".to_string())?
        .pid;
    verify_daemon_replacement(old_pid, new_pid)?;

    daemon_transport_request(&config, DaemonRequest::RefreshLocalPackages)
        .map_err(|error| format!("refresh local package registrations: {error}"))?;
    for entrypoint in &running_entrypoints {
        daemon_transport_request(
            &config,
            DaemonRequest::StartPackageEntrypoint {
                package_name: entrypoint.package_name.clone(),
                entrypoint_id: entrypoint.entrypoint_id.clone(),
                environment_overrides: Default::default(),
            },
        )
        .map_err(|error| {
            format!(
                "restore package entrypoint {}/{}: {error}; updated daemon remains running; recovery: {} packages start-entrypoint --data-dir {} {} {}",
                entrypoint.package_name,
                entrypoint.entrypoint_id,
                hub_bin.display(),
                options.data_directory.display(),
                entrypoint.package_name,
                entrypoint.entrypoint_id
            )
        })?;
    }
    verify_update(
        &config,
        UpdateVerification {
            data_directory: &options.data_directory,
            hub_revision: &hub_revision,
            core_revision: &core_revision,
            worker_bin: &worker_bin,
            sidecar: &sidecar,
            enabled_packages: &selection.enabled,
            restored_entrypoints: &running_entrypoints,
        },
    )?;

    println!("update=complete");
    println!(
        "scope={}",
        if options.scope == UpdateScope::Core {
            "core"
        } else {
            "all"
        }
    );
    println!("hub_revision={hub_revision}");
    println!("core_revision={core_revision}");
    println!("daemon_pid={new_pid}");
    println!("package_count={}", selection.build.len());
    Ok(())
}

fn selected_packages(
    scope: UpdateScope,
    data_directory: &Path,
    config: &botster_hub::HubConfig,
    daemon_running: bool,
) -> Result<PackageSelection, String> {
    let state = read_hub_state(data_directory)?;
    let live_packages = if daemon_running {
        daemon_transport_request(config, DaemonRequest::ListPackages)
            .map_err(|error| format!("list live packages: {error}"))?
            .packages
    } else {
        Vec::new()
    };
    let enabled_names: BTreeSet<String> = if daemon_running {
        live_packages
            .iter()
            .filter(|package| package.state == "enabled")
            .map(|package| package.package_name.clone())
            .collect()
    } else {
        state
            .as_ref()
            .into_iter()
            .flat_map(|state| &state.package_registry.records)
            .filter(|record| record.is_enabled())
            .map(|record| record.manifest.name.clone())
            .collect()
    };
    let mut selected = Vec::new();
    if scope == UpdateScope::All {
        let candidates = state
            .as_ref()
            .map(direct_local_package_candidates)
            .unwrap_or_default();
        for candidate in candidates {
            if !enabled_names.contains(&candidate.name) {
                continue;
            }
            selected.push(LocalPackage {
                name: candidate.name,
                root: candidate.root,
            });
        }
        selected.sort_by(|left, right| left.name.cmp(&right.name));
    }
    Ok(PackageSelection {
        build: selected,
        enabled: enabled_names,
    })
}

fn direct_local_package_candidates(state: &HubState) -> Vec<DirectLocalPackageCandidate> {
    state
        .package_registry
        .records
        .iter()
        .filter(|record| is_direct_local_path_package(record))
        .filter_map(|record| {
            let Some(PackageSource::Path { path }) = &record.manifest.source else {
                return None;
            };
            Some(DirectLocalPackageCandidate {
                name: record.manifest.name.clone(),
                root: PathBuf::from(path),
            })
        })
        .collect()
}

fn is_direct_local_path_package(record: &PackageRecord) -> bool {
    matches!(record.manifest.source, Some(PackageSource::Path { .. }))
        && record.source_metadata.is_none()
        && record.provenance.source.starts_with("local:")
}

fn snapshot_running_entrypoints(
    config: &botster_hub::HubConfig,
) -> Result<Vec<EntrypointIdentity>, String> {
    Ok(
        daemon_transport_request(config, DaemonRequest::ListPackages)
            .map_err(|error| format!("snapshot running package entrypoints: {error}"))?
            .packages
            .iter()
            .flat_map(running_entrypoints)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    )
}

fn read_hub_state(data_directory: &Path) -> Result<Option<HubState>, String> {
    let path = FileHubStateStore::for_data_directory(data_directory)
        .path()
        .to_path_buf();
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("read durable package state {}: {error}", path.display())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!(
            "read durable package state {}: {error}",
            path.display()
        )),
    }
}

fn running_entrypoints(package: &DaemonPackage) -> impl Iterator<Item = EntrypointIdentity> + '_ {
    package
        .runnable_entrypoints
        .iter()
        .filter(|entrypoint| entrypoint.process.state == "running")
        .map(|entrypoint| EntrypointIdentity {
            package_name: package.package_name.clone(),
            entrypoint_id: entrypoint.id.clone(),
        })
}

fn ensure_clean_git_repository(
    root: &Path,
    label: &str,
    runner: &mut dyn CommandRunner,
) -> Result<(), String> {
    let inside = git_output(
        root,
        &["rev-parse", "--is-inside-work-tree"],
        runner,
        GIT_TIMEOUT,
    )?;
    if inside != "true" {
        return Err(format!(
            "{label} is not a Git repository: {}",
            root.display()
        ));
    }
    let status = git_output(
        root,
        &["status", "--porcelain", "--untracked-files=all"],
        runner,
        GIT_TIMEOUT,
    )?;
    if !status.is_empty() {
        return Err(format!(
            "{label} repository is dirty; preserve and commit or remove changes before update"
        ));
    }
    git_output(
        root,
        &["rev-parse", "--abbrev-ref", "@{upstream}"],
        runner,
        GIT_TIMEOUT,
    )
    .map_err(|_| format!("{label} repository has no upstream branch"))?;
    Ok(())
}

fn fast_forward_repository(
    root: &Path,
    label: &str,
    runner: &mut dyn CommandRunner,
) -> Result<(), String> {
    run_required(
        runner,
        "git",
        &["fetch", "--prune"],
        root,
        GIT_TIMEOUT,
        &[],
        &format!("fetch {label}"),
    )?;
    run_required(
        runner,
        "git",
        &["merge", "--ff-only", "@{upstream}"],
        root,
        GIT_TIMEOUT,
        &[],
        &format!("fast-forward {label}"),
    )
}

fn persist_core_lock_pin(source_root: &Path, runner: &mut dyn CommandRunner) -> Result<(), String> {
    let lock_status = git_output(
        source_root,
        &["status", "--porcelain", "--", "Cargo.lock"],
        runner,
        GIT_TIMEOUT,
    )?;
    if lock_status.is_empty() {
        return Ok(());
    }
    run_required(
        runner,
        "git",
        &["add", "--", "Cargo.lock"],
        source_root,
        GIT_TIMEOUT,
        &[],
        "stage updated Core lock pin",
    )?;
    run_required(
        runner,
        "git",
        &[
            "commit",
            "-m",
            "Update locked Botster Core revision",
            "--",
            "Cargo.lock",
        ],
        source_root,
        GIT_TIMEOUT,
        &[],
        "commit updated Core lock pin",
    )?;
    let remaining = git_output(
        source_root,
        &["status", "--porcelain", "--untracked-files=all"],
        runner,
        GIT_TIMEOUT,
    )?;
    if remaining.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Core lock pin commit left unexpected repository changes: {remaining}"
        ))
    }
}

fn git_output(
    root: &Path,
    args: &[&str],
    runner: &mut dyn CommandRunner,
    timeout: Duration,
) -> Result<String, String> {
    let args: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();
    let result = runner.capture("git", &args, root, timeout)?;
    if !result.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            result.stderr.trim()
        ));
    }
    Ok(result.stdout.trim().to_string())
}

fn run_required(
    runner: &mut dyn CommandRunner,
    program: &str,
    args: &[&str],
    cwd: &Path,
    timeout: Duration,
    environment: &[(&str, &str)],
    label: &str,
) -> Result<(), String> {
    let args: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();
    let status = runner.stream(program, &args, cwd, timeout, environment)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with status {status}"))
    }
}

fn load_package_contract(package: &LocalPackage) -> Result<PackageBuildContract, String> {
    let path = package.root.join(PACKAGE_CONTRACT_FILE);
    let bytes = fs::read(&path).map_err(|error| {
        format!(
            "package {} requires {}: {error}",
            package.name,
            path.display()
        )
    })?;
    let contract: PackageBuildContract = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse package update contract {}: {error}", path.display()))?;
    for (index, step) in contract.steps.iter().enumerate() {
        if step.argv.is_empty() {
            return Err(format!(
                "package {} step {} has empty argv",
                package.name,
                index + 1
            ));
        }
        if step.timeout_seconds == 0 {
            return Err(format!(
                "package {} step {} has zero timeout",
                package.name,
                index + 1
            ));
        }
        resolve_contract_cwd(&package.root, step.cwd.as_deref())?;
    }
    Ok(contract)
}

fn run_package_contract(
    package: &LocalPackage,
    contract: &PackageBuildContract,
    runner: &mut dyn CommandRunner,
) -> Result<(), String> {
    for (index, step) in contract.steps.iter().enumerate() {
        let program = &step.argv[0];
        let cwd = resolve_contract_cwd(&package.root, step.cwd.as_deref())?;
        let status = runner.stream(
            program,
            &step.argv[1..],
            &cwd,
            Duration::from_secs(step.timeout_seconds),
            &[],
        )?;
        if !status.success() {
            return Err(format!(
                "package {} step {} failed with status {status}",
                package.name,
                index + 1
            ));
        }
    }
    Ok(())
}

fn resolve_contract_cwd(root: &Path, cwd: Option<&Path>) -> Result<PathBuf, String> {
    let Some(cwd) = cwd else {
        return Ok(root.to_path_buf());
    };
    if cwd.is_absolute()
        || cwd
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(format!(
            "package contract cwd must stay within package root: {}",
            cwd.display()
        ));
    }
    Ok(root.join(cwd))
}

fn resolved_core_revision(lockfile: &Path) -> Result<String, String> {
    let contents = fs::read_to_string(lockfile)
        .map_err(|error| format!("read {}: {error}", lockfile.display()))?;
    let core = locked_git_revision(&contents, "botster-core")?;
    let daemon = locked_git_revision(&contents, "botster-core-daemon")?;
    if core != daemon {
        return Err(format!(
            "Cargo.lock resolves different Core revisions: botster-core={core} botster-core-daemon={daemon}"
        ));
    }
    Ok(core)
}

fn locked_git_revision(contents: &str, package: &str) -> Result<String, String> {
    let marker = format!("name = \"{package}\"");
    let mut lines = contents.lines();
    while let Some(line) = lines.next() {
        if line.trim() != marker {
            continue;
        }
        for line in lines.by_ref() {
            let line = line.trim();
            if line == "[[package]]" {
                break;
            }
            if let Some(source) = line
                .strip_prefix("source = \"")
                .and_then(|value| value.strip_suffix('"'))
                && let Some((_, revision)) = source.rsplit_once('#')
            {
                return Ok(revision.to_string());
            }
        }
    }
    Err(format!("Cargo.lock has no Git revision for {package}"))
}

fn cargo_target_directory(source_root: &Path) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from) {
        Some(path) if path.is_absolute() => path,
        Some(path) => source_root.join(path),
        None => source_root.join("target"),
    }
}

fn worker_sidecar(worker: &Path) -> PathBuf {
    let name = worker
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("botster-session-worker");
    worker.with_file_name(format!("{name}.{CORE_SIDECAR_SUFFIX}"))
}

fn require_file(path: &Path, label: &str) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("{label} does not exist: {}", path.display()))
    }
}

fn verify_update(
    config: &botster_hub::HubConfig,
    expected: UpdateVerification<'_>,
) -> Result<(), String> {
    let status = daemon_transport_request(config, DaemonRequest::Status)
        .map_err(|error| format!("verify Hub status: {error}"))?
        .status
        .ok_or_else(|| "verify Hub status returned no status".to_string())?;
    if status.compatibility != botster_hub::DaemonCompatibility::current() {
        return Err("updated Hub compatibility does not match this command".to_string());
    }
    if status.software.build_revision.as_deref() != Some(expected.hub_revision) {
        return Err(format!(
            "updated Hub revision mismatch: expected {}, got {:?}",
            expected.hub_revision, status.software.build_revision
        ));
    }
    let metadata = read_runtime_daemon_metadata(expected.data_directory)
        .map_err(|error| format!("read updated daemon metadata: {error}"))?
        .ok_or_else(|| "updated daemon metadata is missing".to_string())?;
    verify_worker_identity(
        expected.worker_bin,
        metadata.session_worker_bin.as_deref(),
        expected.sidecar,
        expected.core_revision,
    )?;
    let packages = daemon_transport_request(config, DaemonRequest::ListPackages)
        .map_err(|error| format!("verify enabled packages: {error}"))?
        .packages;
    for package_name in expected.enabled_packages {
        let package = packages
            .iter()
            .find(|package| &package.package_name == package_name)
            .ok_or_else(|| format!("updated package {package_name} is not registered"))?;
        if package.state != "enabled" {
            return Err(format!(
                "updated package {} state is {}",
                package_name, package.state
            ));
        }
    }
    let apps = daemon_transport_request(config, DaemonRequest::ListApps)
        .map_err(|error| format!("verify app readiness: {error}"))?
        .apps;
    for restored in expected.restored_entrypoints {
        let app = apps
            .iter()
            .find(|app| {
                app.package_name == restored.package_name
                    && app.entrypoint_id == restored.entrypoint_id
            })
            .ok_or_else(|| {
                format!(
                    "restored app {}/{} is missing",
                    restored.package_name, restored.entrypoint_id
                )
            })?;
        if app.lifecycle_state != "running" || !app.blocked_reasons.is_empty() {
            return Err(format!(
                "restored app {}/{} is not ready: state={} blocked={:?}",
                restored.package_name,
                restored.entrypoint_id,
                app.lifecycle_state,
                app.blocked_reasons
            ));
        }
        if app.kind == "web_app" && app.launch_target.local_url.is_none() {
            return Err(format!(
                "restored web app {}/{} has no local_url",
                restored.package_name, restored.entrypoint_id
            ));
        }
    }
    Ok(())
}

fn verify_daemon_replacement(old_pid: Option<u32>, new_pid: u32) -> Result<(), String> {
    if old_pid == Some(new_pid) {
        Err(format!("daemon replacement reused pid {new_pid}"))
    } else {
        Ok(())
    }
}

fn verify_worker_identity(
    worker_bin: &Path,
    configured_worker: Option<&str>,
    sidecar: &Path,
    core_revision: &str,
) -> Result<(), String> {
    let worker_realpath = worker_bin
        .canonicalize()
        .map_err(|error| format!("resolve built worker realpath: {error}"))?;
    if !worker_realpath.is_file() {
        return Err("configured worker realpath is not a file".to_string());
    }
    let configured_worker = configured_worker
        .ok_or_else(|| "updated daemon metadata has no session worker path".to_string())?;
    let configured_realpath = Path::new(configured_worker)
        .canonicalize()
        .map_err(|error| format!("resolve started worker realpath: {error}"))?;
    if configured_realpath != worker_realpath {
        return Err(format!(
            "started worker realpath mismatch: expected {}, got {}",
            worker_realpath.display(),
            configured_realpath.display()
        ));
    }
    let recorded_core = fs::read_to_string(sidecar)
        .map_err(|error| format!("read worker Core revision sidecar: {error}"))?;
    if recorded_core.trim() != core_revision {
        return Err(format!(
            "worker Core revision mismatch: expected {core_revision}, got {}",
            recorded_core.trim()
        ));
    }
    Ok(())
}

fn usage() -> String {
    "usage: botster-hub update <core|all> [--data-dir <path>]".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scopes_are_explicit() {
        assert_eq!(UpdateScope::parse("core").unwrap(), UpdateScope::Core);
        assert_eq!(UpdateScope::parse("all").unwrap(), UpdateScope::All);
        assert!(UpdateScope::parse("").is_err());
        assert!(UpdateScope::parse("everything").is_err());
    }

    #[test]
    fn lockfile_requires_one_core_revision() {
        let lock = r#"
[[package]]
name = "botster-core"
source = "git+https://example.test/core#abc123"
[[package]]
name = "botster-core-daemon"
source = "git+https://example.test/core#abc123"
"#;
        let path = std::env::temp_dir().join(format!("botster-update-lock-{}", std::process::id()));
        fs::write(&path, lock).unwrap();
        assert_eq!(resolved_core_revision(&path).unwrap(), "abc123");
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn package_contract_cwd_cannot_escape_package_root() {
        let root = Path::new("/tmp/package");
        assert_eq!(
            resolve_contract_cwd(root, Some(Path::new("web"))).unwrap(),
            root.join("web")
        );
        assert!(resolve_contract_cwd(root, Some(Path::new("../other"))).is_err());
        assert!(resolve_contract_cwd(root, Some(Path::new("/tmp/other"))).is_err());
    }

    #[test]
    fn dirty_porcelain_is_a_failure() {
        struct DirtyRunner;
        impl CommandRunner for DirtyRunner {
            fn capture(
                &mut self,
                _program: &str,
                args: &[String],
                _cwd: &Path,
                _timeout: Duration,
            ) -> Result<CommandResult, String> {
                let stdout = if args.contains(&"--is-inside-work-tree".to_string()) {
                    "true\n"
                } else if args.contains(&"--porcelain".to_string()) {
                    "?? operator-file\n"
                } else {
                    "origin/main\n"
                };
                Ok(CommandResult {
                    status: ExitStatus::from_raw(0),
                    stdout: stdout.to_string(),
                    stderr: String::new(),
                })
            }
            fn stream(
                &mut self,
                _program: &str,
                _args: &[String],
                _cwd: &Path,
                _timeout: Duration,
                _environment: &[(&str, &str)],
            ) -> Result<ExitStatus, String> {
                unreachable!()
            }
        }
        let error =
            ensure_clean_git_repository(Path::new("/tmp/repo"), "fixture", &mut DirtyRunner)
                .unwrap_err();
        assert!(error.contains("repository is dirty"));
    }

    #[test]
    fn package_build_failure_stops_the_update() {
        struct FailedBuildRunner;
        impl CommandRunner for FailedBuildRunner {
            fn capture(
                &mut self,
                _program: &str,
                _args: &[String],
                _cwd: &Path,
                _timeout: Duration,
            ) -> Result<CommandResult, String> {
                unreachable!()
            }

            fn stream(
                &mut self,
                _program: &str,
                _args: &[String],
                _cwd: &Path,
                _timeout: Duration,
                _environment: &[(&str, &str)],
            ) -> Result<ExitStatus, String> {
                Ok(ExitStatus::from_raw(256))
            }
        }

        let root = unique_test_dir("failed-package-build");
        fs::write(
            root.join(PACKAGE_CONTRACT_FILE),
            r#"{"steps":[{"argv":["package-build"],"timeout_seconds":10}]}"#,
        )
        .unwrap();
        let package = LocalPackage {
            name: "fixture".to_string(),
            root,
        };
        let contract = load_package_contract(&package).unwrap();
        let error = run_package_contract(&package, &contract, &mut FailedBuildRunner).unwrap_err();
        assert!(error.contains("step 1 failed"));
    }

    #[test]
    fn package_contract_validation_rejects_invalid_steps_before_execution() {
        let root = unique_test_dir("invalid-package-contract");
        fs::write(
            root.join(PACKAGE_CONTRACT_FILE),
            r#"{"steps":[{"argv":[],"timeout_seconds":10}]}"#,
        )
        .unwrap();
        let package = LocalPackage {
            name: "fixture".to_string(),
            root,
        };

        let error = load_package_contract(&package).unwrap_err();

        assert!(error.contains("step 1 has empty argv"));
    }

    #[test]
    fn daemon_replacement_rejects_the_old_pid() {
        assert!(verify_daemon_replacement(Some(42), 42).is_err());
        assert!(verify_daemon_replacement(Some(42), 43).is_ok());
        assert!(verify_daemon_replacement(None, 42).is_ok());
    }

    #[test]
    fn worker_revision_verification_fails_closed() {
        let root = unique_test_dir("worker-verification");
        let worker = root.join("botster-session-worker");
        let sidecar = worker_sidecar(&worker);
        fs::write(&worker, "fixture").unwrap();
        fs::write(&sidecar, "old-core\n").unwrap();
        let error = verify_worker_identity(
            &worker,
            Some(worker.to_string_lossy().as_ref()),
            &sidecar,
            "new-core",
        )
        .unwrap_err();
        assert!(error.contains("worker Core revision mismatch"));
    }

    #[test]
    fn update_locks_fail_fast_on_contention() {
        let root = unique_test_dir("lock-contention");
        let path = root.join("update.lock");
        let _first = UpdateLock::acquire(&path, "test").unwrap();
        let error = UpdateLock::acquire(&path, "test").err().unwrap();
        assert!(error.contains("lock is held"));
    }

    #[test]
    fn registry_installed_path_package_is_not_a_direct_local_update_target() {
        let record: PackageRecord = serde_json::from_value(serde_json::json!({
            "manifest": {
                "name": "registry-path",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": "/tmp/registry-path" },
                "capabilities": [],
                "entrypoints": []
            },
            "state": "enabled",
            "classification": "plugin",
            "provenance": { "source": "local:/tmp/registry-path", "checksum": null },
            "source_metadata": {
                "registry_id": "fixture-registry",
                "registry_kind": "local_path",
                "entry_id": "registry-path",
                "source_kind": "local_path",
                "source_label": "registry-path",
                "git_repo": null
            },
            "update_policy": "manual",
            "last_audit_reason": "registry install"
        }))
        .unwrap();

        assert!(!is_direct_local_path_package(&record));
    }

    #[test]
    fn core_lock_pin_commit_persists_only_cargo_lock() {
        let root = unique_test_dir("core-lock-pin");
        for args in [
            ["init"].as_slice(),
            ["config", "user.email", "update-test@example.invalid"].as_slice(),
            ["config", "user.name", "Update Test"].as_slice(),
        ] {
            let status = Command::new("git")
                .args(args)
                .current_dir(&root)
                .status()
                .unwrap();
            assert!(status.success());
        }
        fs::write(root.join("Cargo.lock"), "old\n").unwrap();
        assert!(
            Command::new("git")
                .args(["add", "Cargo.lock"])
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .args(["commit", "-m", "fixture"])
                .current_dir(&root)
                .status()
                .unwrap()
                .success()
        );
        fs::write(root.join("Cargo.lock"), "new\n").unwrap();

        persist_core_lock_pin(&root, &mut ProcessCommandRunner).unwrap();

        let status = Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&root)
            .output()
            .unwrap();
        assert!(status.status.success());
        assert!(status.stdout.is_empty());
        let subject = Command::new("git")
            .args(["log", "-1", "--format=%s"])
            .current_dir(&root)
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&subject.stdout).trim(),
            "Update locked Botster Core revision"
        );
    }

    fn unique_test_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "botster-update-unit-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
