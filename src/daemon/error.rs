use std::error::Error;
use std::fmt;

use botster_hub_client::DaemonDiagnostic;
use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{DaemonOperatorError, DaemonResponse, DaemonResponseKind};

use crate::client_api_dto::response::daemon_response_base;
use crate::daemon_projection::{
    daemon_operator_error_from_client, daemon_operator_error_from_package,
};
use crate::entrypoint_supervisor::EntrypointSupervisorError;
use crate::{SpawnTargetError, WorktreeError};

const WEBRTC_SIGNAL_OPERATION: &str = "local_webrtc_signal";

pub(crate) fn hub_update_execution_error(
    code: &str,
    operation: &str,
    message: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("daemon-{operation}"),
        operation: operation.to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(operation, message)],
    });
    response
}

pub(crate) fn daemon_operator_error(error: crate::HubClientError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_client(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn daemon_package_error(error: crate::PackageRegistryError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_package(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn daemon_spawn_target_error(error: SpawnTargetError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.kind.to_string(),
        request_id: "daemon-spawn-targets".to_string(),
        operation: "spawn_targets".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

pub(crate) fn daemon_worktree_error(error: WorktreeError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.kind.to_string(),
        request_id: "daemon-worktrees".to_string(),
        operation: "worktrees".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

pub(crate) fn daemon_state_error(error: crate::HubStateStoreError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_state(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn daemon_snapshot_stream_forbidden_error(
    error: DaemonTransportError,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "snapshot_stream_forbidden".to_string(),
        request_id: "daemon-sessions-drain".to_string(),
        operation: "drain".to_string(),
        message: error.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "drain",
            "snapshot stream is owned by another connection",
        )],
    });
    response
}

pub(crate) fn daemon_package_compensation_error(error: DaemonTransportError) -> DaemonResponse {
    const MESSAGE_BOUND: usize = 512;
    let DaemonTransportError::PackageCompensation {
        original,
        rollbacks,
    } = error
    else {
        let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
        response.error = Some(DaemonOperatorError {
            code: "package_compensation_failed".to_string(),
            request_id: "daemon-package-mutation".to_string(),
            operation: "package_mutation_compensation".to_string(),
            message: error.to_string(),
            diagnostics: Vec::new(),
        });
        return response;
    };

    let mut diagnostics = vec![DaemonDiagnostic {
        kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
        operation: Some("original".to_string()),
        feature: None,
        message: Some(bound_compensation_message(
            original.to_string(),
            MESSAGE_BOUND,
        )),
    }];
    for rollback in &rollbacks {
        diagnostics.push(DaemonDiagnostic {
            kind: botster_hub_client::DaemonDiagnosticKind::ActionFailure,
            operation: Some(rollback.step.to_string()),
            feature: rollback.package_name.clone(),
            message: Some(bound_compensation_message(
                rollback.error.to_string(),
                MESSAGE_BOUND,
            )),
        });
    }

    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "package_compensation_failed".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: "package_mutation_compensation".to_string(),
        message: format!(
            "package mutation failed ({original}); rollback failures: {}",
            rollbacks.len()
        ),
        diagnostics: diagnostics.clone(),
    });
    response.diagnostics = diagnostics;
    response
}

pub(crate) fn bound_compensation_message(message: String, bound: usize) -> String {
    if message.chars().count() <= bound {
        return message;
    }
    message.chars().take(bound).collect()
}

pub(crate) fn daemon_entrypoint_error(error: EntrypointSupervisorError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_entrypoint(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn daemon_local_webrtc_error(error: crate::LocalWebrtcError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(daemon_operator_error_from_local_webrtc(error));
    if let Some(error) = &response.error {
        response.diagnostics = error.diagnostics.clone();
    }
    response
}

pub(crate) fn local_webrtc_bootstrap_issue_error(
    code: &str,
    message: impl Into<String>,
) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("issue_local_webrtc_bootstrap", message.clone());
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "issue-local-webrtc-bootstrap".to_string(),
        operation: "issue_local_webrtc_bootstrap".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

pub(crate) fn daemon_app_launch_error(
    package_name: &str,
    entrypoint_id: &str,
    code: &str,
    message: impl Into<String>,
) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("resolve_app_launch", format!("{code}: {message}"));
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("resolve-app-launch-{package_name}-{entrypoint_id}"),
        operation: "resolve_app_launch".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

pub(crate) fn daemon_package_route_error(
    package_name: &str,
    route_id: &str,
    code: &str,
    message: impl Into<String>,
) -> DaemonResponse {
    let message = message.into();
    let diagnostic =
        DaemonDiagnostic::action_failure("resolve_package_route", format!("{code}: {message}"));
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: format!("resolve-package-route-{package_name}-{route_id}"),
        operation: "resolve_package_route".to_string(),
        message,
        diagnostics: vec![diagnostic.clone()],
    });
    response.diagnostics = vec![diagnostic];
    response
}

pub(crate) fn daemon_plugin_tool_error(error: crate::McpToolError) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.code,
        request_id: "daemon-plugin-mcp-call".to_string(),
        operation: "plugin_mcp_call".to_string(),
        message: error.message,
        diagnostics: Vec::new(),
    });
    response
}

pub(crate) fn daemon_operator_error_from_state(
    error: crate::HubStateStoreError,
) -> DaemonOperatorError {
    DaemonOperatorError {
        code: "hub_state_error".to_string(),
        request_id: "daemon-package-mutation".to_string(),
        operation: "persist_package_registry".to_string(),
        message: format!("failed to persist package registry: {error}"),
        diagnostics: Vec::new(),
    }
}

pub(crate) fn daemon_operator_error_from_entrypoint(
    error: EntrypointSupervisorError,
) -> DaemonOperatorError {
    let (code, message) = match error {
        EntrypointSupervisorError::PackageNotInstalled(package_name) => (
            "package_not_installed",
            format!("package {package_name} is not installed"),
        ),
        EntrypointSupervisorError::PackageDisabled(package_name) => (
            "package_disabled",
            format!("package {package_name} is not enabled"),
        ),
        EntrypointSupervisorError::PackageNotLocal(package_name) => (
            "package_not_local",
            format!("package {package_name} is not a local package"),
        ),
        EntrypointSupervisorError::EntrypointNotFound {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_found",
            format!("package {package_name} has no runnable entrypoint {entrypoint_id}"),
        ),
        EntrypointSupervisorError::EntrypointNotSupervisable {
            package_name,
            entrypoint_id,
        } => (
            "entrypoint_not_supervisable",
            format!("package {package_name} entrypoint {entrypoint_id} is not marked supervisable"),
        ),
        EntrypointSupervisorError::ReadinessFailed {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_readiness_failed",
            format!(
                "package {package_name} entrypoint {entrypoint_id} exited before publishing structured readiness: {details}"
            ),
        ),
        EntrypointSupervisorError::ReadinessTimeout {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_readiness_timeout",
            format!(
                "package {package_name} entrypoint {entrypoint_id} did not publish structured readiness before the liveness deadline: {details}"
            ),
        ),
        EntrypointSupervisorError::LaunchContract {
            package_name,
            entrypoint_id,
            details,
        } => (
            "entrypoint_launch_contract_error",
            format!(
                "package {package_name} entrypoint {entrypoint_id} launch contract could not be resolved: {details}"
            ),
        ),
        EntrypointSupervisorError::Watch(message) => (
            "entrypoint_readiness_watch_error",
            format!("entrypoint launch-result watch failed: {message}"),
        ),
        EntrypointSupervisorError::Io(error) => (
            "entrypoint_io_error",
            format!("entrypoint process error: {error}"),
        ),
    };
    DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-package-entrypoint".to_string(),
        operation: "package_entrypoint".to_string(),
        message,
        diagnostics: Vec::new(),
    }
}

pub(crate) fn daemon_operator_error_from_local_webrtc(
    error: crate::LocalWebrtcError,
) -> DaemonOperatorError {
    let (code, message) = match error {
        crate::LocalWebrtcError::MissingGrant => (
            "local_webrtc_missing_grant",
            "local WebRTC bootstrap grant was not found".to_string(),
        ),
        crate::LocalWebrtcError::ExpiredGrant => (
            "local_webrtc_expired_grant",
            "local WebRTC bootstrap grant expired".to_string(),
        ),
        crate::LocalWebrtcError::RedeemedGrant => (
            "local_webrtc_redeemed_grant",
            "local WebRTC bootstrap grant was already redeemed".to_string(),
        ),
        crate::LocalWebrtcError::SecretMismatch => (
            "local_webrtc_secret_mismatch",
            "local WebRTC bootstrap grant secret mismatch".to_string(),
        ),
        crate::LocalWebrtcError::OriginMismatch => (
            "local_webrtc_origin_mismatch",
            "local WebRTC bootstrap origin mismatch".to_string(),
        ),
        crate::LocalWebrtcError::InvalidOffer(message) => (
            "local_webrtc_invalid_offer",
            format!("invalid local WebRTC offer: {message}"),
        ),
        crate::LocalWebrtcError::Random(message) => (
            "local_webrtc_random_failed",
            format!("local WebRTC random token failed: {message}"),
        ),
        crate::LocalWebrtcError::Webrtc(message) => (
            "local_webrtc_signaling_failed",
            format!("local WebRTC signaling failed: {message}"),
        ),
    };
    let diagnostic = DaemonDiagnostic::action_failure(WEBRTC_SIGNAL_OPERATION, message.clone());
    DaemonOperatorError {
        code: code.to_string(),
        request_id: WEBRTC_SIGNAL_OPERATION.to_string(),
        operation: WEBRTC_SIGNAL_OPERATION.to_string(),
        message,
        diagnostics: vec![diagnostic],
    }
}

/// Daemon socket transport error.
#[derive(Debug)]
pub enum DaemonTransportError {
    MissingSocketBinding,
    NotRunning,
    AlreadyRunning,
    ClientDisconnected,
    Protocol(&'static str),
    Compatibility(botster_hub_client::DaemonCompatibilityError),
    UnexpectedResponse,
    DaemonNotRunning,
    ControlThreadStopped,
    Io(std::io::Error),
    Json(serde_json::Error),
    Daemon(crate::HubDaemonError),
    Client(crate::HubClientError),
    Package(crate::PackageRegistryError),
    SpawnTarget(SpawnTargetError),
    Worktree(WorktreeError),
    State(crate::HubStateStoreError),
    Entrypoint(EntrypointSupervisorError),
    LocalWebrtc(crate::LocalWebrtcError),
    Runtime(crate::HubRuntimeError),
    Lifecycle(crate::HubLifecycleError),
    /// A package mutation side effect failed, and one or more rollback steps also failed.
    PackageCompensation {
        original: Box<DaemonTransportError>,
        rollbacks: Vec<PackageRollbackFailure>,
    },
    SnapshotStreamForbidden {
        session_id: String,
        subscription_id: String,
    },
}

/// One failed compensation step after a package mutation side-effect failure.
#[derive(Debug)]
pub struct PackageRollbackFailure {
    pub step: &'static str,
    pub package_name: Option<String>,
    pub error: Box<DaemonTransportError>,
}

impl fmt::Display for DaemonTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSocketBinding => write!(formatter, "local socket transport is disabled"),
            Self::NotRunning => write!(formatter, "daemon not running"),
            Self::AlreadyRunning => write!(formatter, "daemon already running"),
            Self::ClientDisconnected => write!(formatter, "client disconnected"),
            Self::Protocol(message) => write!(formatter, "daemon protocol error: {message}"),
            Self::Compatibility(error) => write!(formatter, "{error}"),
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::DaemonNotRunning => write!(formatter, "daemon runtime is not running"),
            Self::ControlThreadStopped => write!(formatter, "daemon control thread stopped"),
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Json(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::Client(error) => write!(formatter, "{error:?}"),
            Self::Package(error) => write!(formatter, "{error:?}"),
            Self::SpawnTarget(error) => write!(formatter, "{error}"),
            Self::Worktree(error) => write!(formatter, "{error}"),
            Self::State(error) => write!(formatter, "{error}"),
            Self::Entrypoint(error) => write!(formatter, "{error:?}"),
            Self::LocalWebrtc(error) => write!(formatter, "{error}"),
            Self::Runtime(error) => write!(formatter, "{error:?}"),
            Self::Lifecycle(error) => write!(formatter, "{error:?}"),
            Self::PackageCompensation {
                original,
                rollbacks,
            } => {
                write!(
                    formatter,
                    "package mutation failed ({original}); rollback failures: {}",
                    rollbacks.len()
                )
            }
            Self::SnapshotStreamForbidden {
                session_id,
                subscription_id,
            } => write!(
                formatter,
                "snapshot stream forbidden session={session_id} subscription={subscription_id}"
            ),
        }
    }
}

impl Error for DaemonTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Compatibility(error) => Some(error),
            Self::Daemon(error) => Some(error),
            Self::LocalWebrtc(error) => Some(error),
            Self::SpawnTarget(error) => Some(error),
            Self::Worktree(error) => Some(error),
            Self::State(error) => Some(error),
            _ => None,
        }
    }
}

impl From<crate::HubDaemonError> for DaemonTransportError {
    fn from(error: crate::HubDaemonError) -> Self {
        Self::Daemon(error)
    }
}

impl From<ClientDaemonTransportError> for DaemonTransportError {
    fn from(error: ClientDaemonTransportError) -> Self {
        match error {
            ClientDaemonTransportError::Io(error) => Self::Io(error),
            ClientDaemonTransportError::Json(error) => Self::Json(error),
            ClientDaemonTransportError::MissingSocketBinding => Self::MissingSocketBinding,
            ClientDaemonTransportError::AlreadyRunning => Self::AlreadyRunning,
            ClientDaemonTransportError::NotRunning => Self::NotRunning,
            ClientDaemonTransportError::ClientDisconnected => Self::ClientDisconnected,
            ClientDaemonTransportError::Protocol(message) => Self::Protocol(message),
            ClientDaemonTransportError::Compatibility(error) => Self::Compatibility(error),
            ClientDaemonTransportError::ControlThreadStopped => Self::ControlThreadStopped,
        }
    }
}

impl From<crate::HubClientError> for DaemonTransportError {
    fn from(error: crate::HubClientError) -> Self {
        Self::Client(error)
    }
}

impl From<crate::PackageRegistryError> for DaemonTransportError {
    fn from(error: crate::PackageRegistryError) -> Self {
        Self::Package(error)
    }
}

impl From<SpawnTargetError> for DaemonTransportError {
    fn from(error: SpawnTargetError) -> Self {
        Self::SpawnTarget(error)
    }
}

impl From<WorktreeError> for DaemonTransportError {
    fn from(error: WorktreeError) -> Self {
        Self::Worktree(error)
    }
}

impl From<crate::HubStateStoreError> for DaemonTransportError {
    fn from(error: crate::HubStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<EntrypointSupervisorError> for DaemonTransportError {
    fn from(error: EntrypointSupervisorError) -> Self {
        Self::Entrypoint(error)
    }
}

impl From<crate::LocalWebrtcError> for DaemonTransportError {
    fn from(error: crate::LocalWebrtcError) -> Self {
        Self::LocalWebrtc(error)
    }
}

impl From<crate::HubRuntimeError> for DaemonTransportError {
    fn from(error: crate::HubRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<crate::HubLifecycleError> for DaemonTransportError {
    fn from(error: crate::HubLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Result alias for daemon socket transport operations.
pub type DaemonTransportResult<T> = Result<T, DaemonTransportError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entrypoint_supervisor::EntrypointSupervisorError;
    use botster_hub_client::DaemonResponseKind;

    #[test]
    fn package_compensation_projects_every_rollback_to_socket_diagnostics() {
        let error = DaemonTransportError::PackageCompensation {
            original: Box::new(DaemonTransportError::Entrypoint(
                EntrypointSupervisorError::ReadinessFailed {
                    package_name: "reload.plugin".to_string(),
                    entrypoint_id: "sleeper".to_string(),
                    details: "entrypoint state after restart is failed".to_string(),
                },
            )),
            rollbacks: vec![
                PackageRollbackFailure {
                    step: "persist",
                    package_name: None,
                    error: Box::new(DaemonTransportError::State(
                        crate::HubStateStoreError::InjectedWriteFailure,
                    )),
                },
                PackageRollbackFailure {
                    step: "entrypoint",
                    package_name: Some("reload.plugin".to_string()),
                    error: Box::new(DaemonTransportError::Entrypoint(
                        EntrypointSupervisorError::ReadinessFailed {
                            package_name: "reload.plugin".to_string(),
                            entrypoint_id: "sleeper".to_string(),
                            details: "restore spawn failed".to_string(),
                        },
                    )),
                },
            ],
        };

        let response = daemon_package_compensation_error(error);
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error");
        assert_eq!(operator.code, "package_compensation_failed");
        assert_eq!(response.diagnostics, operator.diagnostics);
        assert_eq!(operator.diagnostics.len(), 3);

        let original = &operator.diagnostics[0];
        assert_eq!(
            original.kind,
            botster_hub_client::DaemonDiagnosticKind::ActionFailure
        );
        assert_eq!(original.operation.as_deref(), Some("original"));
        assert!(
            original.message.as_deref().is_some_and(
                |message| message.contains("reload.plugin") && message.contains("failed")
            )
        );

        let persist = operator
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.operation.as_deref() == Some("persist"))
            .expect("persist rollback diagnostic");
        assert_eq!(persist.feature, None);
        assert!(
            persist
                .message
                .as_deref()
                .is_some_and(|message| message.contains("injected"))
        );

        let entrypoint = operator
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.operation.as_deref() == Some("entrypoint"))
            .expect("entrypoint rollback diagnostic");
        assert_eq!(entrypoint.feature.as_deref(), Some("reload.plugin"));
        assert!(
            entrypoint
                .message
                .as_deref()
                .is_some_and(|message| message.contains("restore spawn failed"))
        );
    }
}
