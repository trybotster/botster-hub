//! Durable handoff state for source-checkout stack updates.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};

use botster_hub_client::{
    DaemonHubUpdateExecution, DaemonHubUpdateExecutionState, DaemonHubUpdateScope,
};

const EXECUTION_FILE: &str = ".botster-hub-update-execution.json";
const ERROR_LIMIT: usize = 4_096;

pub(crate) struct UpdateHandoff {
    child: Child,
    gate: ChildStdin,
}

impl UpdateHandoff {
    pub(crate) fn release(mut self) -> Result<(), String> {
        if let Err(error) = self.gate.write_all(b"1") {
            self.stop();
            return Err(format!("release update handoff: {error}"));
        }
        std::thread::spawn(move || {
            let _ = self.child.wait();
        });
        Ok(())
    }

    pub(crate) fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(crate) fn start_update_handoff(
    data_directory: &Path,
    scope: DaemonHubUpdateScope,
) -> Result<(DaemonHubUpdateExecution, UpdateHandoff), String> {
    if let Some(active) = current_update_execution(data_directory)?
        && matches!(
            active.state,
            DaemonHubUpdateExecutionState::Started | DaemonHubUpdateExecutionState::Running
        )
        && process_exists(active.updater_pid)
    {
        return Err(format!(
            "hub update {} is already active with updater pid {}",
            active.update_id, active.updater_pid
        ));
    }

    let update_id = new_update_id()?;
    let executable = std::env::current_exe()
        .map_err(|error| format!("resolve current Hub executable: {error}"))?;
    let log_path = data_directory.join(format!(".botster-hub-update-{update_id}.log"));
    let stdout = update_log(&log_path)?;
    let stderr = stdout
        .try_clone()
        .map_err(|error| format!("clone update log handle: {error}"))?;
    let mut command = Command::new(executable);
    command
        .arg("__update-handoff")
        .arg(scope.as_str())
        .arg("--data-dir")
        .arg(data_directory)
        .arg("--update-id")
        .arg(&update_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
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
        .map_err(|error| format!("start detached Hub updater: {error}"))?;
    let Some(gate) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err("detached Hub updater has no handoff gate".to_string());
    };
    let execution = DaemonHubUpdateExecution {
        update_id,
        scope,
        state: DaemonHubUpdateExecutionState::Started,
        updater_pid: child.id(),
        error: None,
    };
    if let Err(error) = write_update_execution(data_directory, &execution) {
        UpdateHandoff { child, gate }.stop();
        return Err(error);
    }
    Ok((execution, UpdateHandoff { child, gate }))
}

pub(crate) fn current_update_execution(
    data_directory: &Path,
) -> Result<Option<DaemonHubUpdateExecution>, String> {
    let Some(mut execution) = read_update_execution(data_directory)? else {
        return Ok(None);
    };
    if matches!(
        execution.state,
        DaemonHubUpdateExecutionState::Started | DaemonHubUpdateExecutionState::Running
    ) && !process_exists(execution.updater_pid)
    {
        execution.state = DaemonHubUpdateExecutionState::Failed;
        execution.error = Some("updater process exited before it recorded a result".to_string());
        write_update_execution(data_directory, &execution)?;
    }
    Ok(Some(execution))
}

#[doc(hidden)]
pub fn mark_update_running(
    data_directory: &Path,
    update_id: &str,
) -> Result<DaemonHubUpdateExecution, String> {
    update_execution_transition(
        data_directory,
        update_id,
        DaemonHubUpdateExecutionState::Running,
        None,
    )
}

#[doc(hidden)]
pub fn mark_update_complete(
    data_directory: &Path,
    update_id: &str,
) -> Result<DaemonHubUpdateExecution, String> {
    update_execution_transition(
        data_directory,
        update_id,
        DaemonHubUpdateExecutionState::Complete,
        None,
    )
}

#[doc(hidden)]
pub fn mark_update_failed(
    data_directory: &Path,
    update_id: &str,
    error: &str,
) -> Result<DaemonHubUpdateExecution, String> {
    update_execution_transition(
        data_directory,
        update_id,
        DaemonHubUpdateExecutionState::Failed,
        Some(error.chars().take(ERROR_LIMIT).collect()),
    )
}

fn update_execution_transition(
    data_directory: &Path,
    update_id: &str,
    state: DaemonHubUpdateExecutionState,
    error: Option<String>,
) -> Result<DaemonHubUpdateExecution, String> {
    let mut execution = read_update_execution(data_directory)?
        .ok_or_else(|| "Hub update execution record is missing".to_string())?;
    if execution.update_id != update_id {
        return Err(format!(
            "Hub update execution record belongs to {} instead of {update_id}",
            execution.update_id
        ));
    }
    execution.state = state;
    execution.error = error;
    write_update_execution(data_directory, &execution)?;
    Ok(execution)
}

fn update_log(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("create Hub update log {}: {error}", path.display()))
}

fn new_update_id() -> Result<String, String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| format!("create Hub update id: {error}"))?;
    Ok(format!(
        "update-{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

fn read_update_execution(
    data_directory: &Path,
) -> Result<Option<DaemonHubUpdateExecution>, String> {
    let path = execution_path(data_directory);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "read Hub update execution {}: {error}",
                path.display()
            ));
        }
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("parse Hub update execution {}: {error}", path.display()))
}

fn write_update_execution(
    data_directory: &Path,
    execution: &DaemonHubUpdateExecution,
) -> Result<(), String> {
    fs::create_dir_all(data_directory).map_err(|error| {
        format!(
            "create Hub data directory {}: {error}",
            data_directory.display()
        )
    })?;
    let path = execution_path(data_directory);
    let temporary = data_directory.join(format!(".{EXECUTION_FILE}.{}.tmp", execution.update_id));
    let bytes = serde_json::to_vec_pretty(execution)
        .map_err(|error| format!("serialize Hub update execution: {error}"))?;
    fs::write(&temporary, bytes).map_err(|error| {
        format!(
            "write Hub update execution {}: {error}",
            temporary.display()
        )
    })?;
    fs::rename(&temporary, &path)
        .map_err(|error| format!("publish Hub update execution {}: {error}", path.display()))
}

fn execution_path(data_directory: &Path) -> PathBuf {
    data_directory.join(EXECUTION_FILE)
}

fn process_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "botster-source-update-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn execution_transitions_are_durable() {
        let root = unique_test_dir("transitions");
        let execution = DaemonHubUpdateExecution {
            update_id: "update-test".to_string(),
            scope: DaemonHubUpdateScope::All,
            state: DaemonHubUpdateExecutionState::Started,
            updater_pid: std::process::id(),
            error: None,
        };
        write_update_execution(&root, &execution).unwrap();
        mark_update_running(&root, "update-test").unwrap();
        assert_eq!(
            current_update_execution(&root).unwrap().unwrap().state,
            DaemonHubUpdateExecutionState::Running
        );
        mark_update_complete(&root, "update-test").unwrap();
        assert_eq!(
            current_update_execution(&root).unwrap().unwrap().state,
            DaemonHubUpdateExecutionState::Complete
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_dead_updater_becomes_failed() {
        let root = unique_test_dir("dead");
        let mut exited = Command::new("/usr/bin/true").spawn().unwrap();
        let exited_pid = exited.id();
        assert!(exited.wait().unwrap().success());
        let execution = DaemonHubUpdateExecution {
            update_id: "update-dead".to_string(),
            scope: DaemonHubUpdateScope::Core,
            state: DaemonHubUpdateExecutionState::Running,
            updater_pid: exited_pid,
            error: None,
        };
        write_update_execution(&root, &execution).unwrap();
        let execution = current_update_execution(&root).unwrap().unwrap();
        assert_eq!(execution.state, DaemonHubUpdateExecutionState::Failed);
        assert!(execution.error.is_some());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_live_updater_blocks_a_second_start() {
        let root = unique_test_dir("busy");
        let execution = DaemonHubUpdateExecution {
            update_id: "update-live".to_string(),
            scope: DaemonHubUpdateScope::All,
            state: DaemonHubUpdateExecutionState::Running,
            updater_pid: std::process::id(),
            error: None,
        };
        write_update_execution(&root, &execution).unwrap();
        let error = start_update_handoff(&root, DaemonHubUpdateScope::Core)
            .err()
            .expect("a live updater must block a second start");
        assert!(error.contains("already active"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }
}
