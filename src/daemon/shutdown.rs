use std::env;

use botster_core::{RequestId, SessionId, SessionLifecycleState};
use botster_core_daemon::{RegistrySessionState, SessionLifecycleLookup};
use botster_hub_client::{DaemonResponse, DaemonSessionCleanup};

use crate::client_api_dto::response::{daemon_session_cleanup, daemon_unknown_session_cleanup};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::tick;

#[derive(Debug, Clone)]
pub(crate) enum ShutdownSessionClassification {
    Active,
    Cleanup(DaemonSessionCleanup),
    Missing,
    Stopping,
}

pub(crate) fn response_after_core_shutdown_error(
    classification: ShutdownSessionClassification,
    error: crate::HubClientError,
    session_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    shutdown_error_response(classification, error, session_id)
}

pub(crate) fn recover_after_core_shutdown_error(
    runtime: &mut crate::HubRuntime,
    session_id: &str,
    error: crate::HubClientError,
    logical_clock: &mut u64,
) -> DaemonTransportResult<DaemonResponse> {
    recover_from_exact_classify(
        classify_shutdown_session(runtime, session_id, tick(logical_clock)),
        error,
        session_id,
    )
}

pub(crate) fn recover_from_exact_classify(
    classification: DaemonTransportResult<ShutdownSessionClassification>,
    error: crate::HubClientError,
    session_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    match classification {
        Ok(classification) => response_after_core_shutdown_error(classification, error, session_id),
        Err(_) => Err(DaemonTransportError::Client(error)),
    }
}

pub(crate) fn shutdown_error_is_already_gone(error: &crate::HubClientError) -> bool {
    matches!(
        error,
        crate::HubClientError::Runtime {
            operation: crate::HubClientOperation::Shutdown,
            kind: crate::HubClientRuntimeErrorKind::UnknownSession,
            ..
        }
    )
}

pub(crate) fn shutdown_error_response(
    classification: ShutdownSessionClassification,
    error: crate::HubClientError,
    session_id: &str,
) -> DaemonTransportResult<DaemonResponse> {
    match classification {
        ShutdownSessionClassification::Cleanup(cleanup) => Ok(daemon_session_cleanup(cleanup)),
        ShutdownSessionClassification::Missing => Ok(daemon_unknown_session_cleanup(session_id)),
        ShutdownSessionClassification::Stopping => {
            Ok(daemon_session_cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            }))
        }
        ShutdownSessionClassification::Active if shutdown_error_is_already_gone(&error) => {
            Ok(daemon_session_cleanup(DaemonSessionCleanup {
                session_id: session_id.to_string(),
                outcome: "already_exited".to_string(),
            }))
        }
        ShutdownSessionClassification::Active => Err(DaemonTransportError::Client(error)),
    }
}

pub(crate) fn forced_shutdown_classify_stopping(session_id: &str) -> bool {
    let botster_env = env::var("BOTSTER_ENV").ok();
    let forced_for = env::var("BOTSTER_HUB_TEST_FORCE_SHUTDOWN_CLASSIFY_STOPPING_FOR").ok();
    forced_shutdown_classify_stopping_from(
        session_id,
        botster_env.as_deref(),
        forced_for.as_deref(),
    )
}

pub(crate) fn forced_shutdown_classify_stopping_from(
    session_id: &str,
    botster_env: Option<&str>,
    forced_for: Option<&str>,
) -> bool {
    botster_env == Some("test") && forced_for == Some(session_id)
}

pub(crate) fn classify_shutdown_session(
    runtime: &mut crate::HubRuntime,
    session_id: &str,
    now_seconds: u64,
) -> DaemonTransportResult<ShutdownSessionClassification> {
    if forced_shutdown_classify_stopping(session_id) {
        return Ok(ShutdownSessionClassification::Stopping);
    }
    match runtime.observe_session_lifecycle(&SessionId(session_id.to_string()), now_seconds) {
        Ok(SessionLifecycleLookup::Found(record)) => {
            Ok(classify_found_session_lifecycle(session_id, &record))
        }
        Ok(SessionLifecycleLookup::Absent) => Ok(ShutdownSessionClassification::Missing),
        Ok(_) => Err(DaemonTransportError::Client(shutdown_lookup_error(
            botster_core_daemon::CoreDaemonError::Shutdown,
        ))),
        Err(botster_core_daemon::CoreDaemonError::UnknownSession(_)) => {
            Ok(ShutdownSessionClassification::Missing)
        }
        Err(error) => Err(DaemonTransportError::Client(shutdown_lookup_error(error))),
    }
}

pub(crate) fn classify_found_session_lifecycle(
    session_id: &str,
    record: &botster_core_daemon::SessionLifecycleRecord,
) -> ShutdownSessionClassification {
    let complete_lifecycle = matches!(
        record.lifecycle,
        Some(SessionLifecycleState::Exited { .. }) | Some(SessionLifecycleState::Failed { .. })
    );
    let complete_registry = matches!(
        record.session.registry_state,
        RegistrySessionState::Exited | RegistrySessionState::Stale
    );
    let stopping = matches!(record.lifecycle, Some(SessionLifecycleState::Stopping))
        || matches!(
            record.session.registry_state,
            RegistrySessionState::Stopping
        );
    if complete_lifecycle || complete_registry {
        ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
            session_id: session_id.to_string(),
            outcome: if matches!(record.session.registry_state, RegistrySessionState::Stale)
                || matches!(record.lifecycle, Some(SessionLifecycleState::Failed { .. }))
            {
                "stale_session".to_string()
            } else {
                "already_exited".to_string()
            },
        })
    } else if stopping {
        ShutdownSessionClassification::Stopping
    } else {
        ShutdownSessionClassification::Active
    }
}

pub(crate) fn shutdown_lookup_error(
    error: botster_core_daemon::CoreDaemonError,
) -> crate::HubClientError {
    crate::HubClientError::Runtime {
        request_id: RequestId("daemon-sessions-shutdown".to_string()),
        operation: crate::HubClientOperation::Shutdown,
        kind: match error {
            botster_core_daemon::CoreDaemonError::UnknownSession(_) => {
                crate::HubClientRuntimeErrorKind::UnknownSession
            }
            _ => crate::HubClientRuntimeErrorKind::Runtime,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::error::daemon_operator_error;
    use botster_core::{SessionId, SessionLifecycleState};
    use botster_core_daemon::RegistrySessionState;
    use botster_hub_client::DaemonResponseKind;

    #[test]
    fn forced_stopping_classify_inject_requires_test_mode() {
        assert!(forced_shutdown_classify_stopping_from(
            "sess",
            Some("test"),
            Some("sess")
        ));
        assert!(
            !forced_shutdown_classify_stopping_from("sess", Some("production"), Some("sess")),
            "non-test BOTSTER_ENV must ignore the Stopping inject"
        );
        assert!(
            !forced_shutdown_classify_stopping_from("sess", None, Some("sess")),
            "unset BOTSTER_ENV must ignore the Stopping inject"
        );
        assert!(!forced_shutdown_classify_stopping_from(
            "sess",
            Some("test"),
            Some("other")
        ));
        assert!(!forced_shutdown_classify_stopping_from(
            "sess",
            Some("test"),
            None
        ));

        const TRANSPORT: &str = include_str!("shutdown.rs");
        let classify = TRANSPORT
            .split("fn classify_shutdown_session(")
            .nth(1)
            .expect("classify_shutdown_session")
            .split("fn classify_found_session_lifecycle(")
            .next()
            .expect("classify body");
        assert!(
            classify.contains("forced_shutdown_classify_stopping("),
            "classify must use the test-gated inject helper"
        );
        let helper = TRANSPORT
            .split("fn forced_shutdown_classify_stopping_from(")
            .nth(1)
            .expect("inject helper")
            .split("fn classify_shutdown_session(")
            .next()
            .expect("inject helper body");
        assert!(
            helper.contains("botster_env == Some(\"test\")"),
            "Stopping inject must require BOTSTER_ENV=test"
        );
    }

    fn shutdown_runtime_error(kind: crate::HubClientRuntimeErrorKind) -> crate::HubClientError {
        crate::HubClientError::Runtime {
            request_id: RequestId("daemon-sessions-shutdown".to_string()),
            operation: crate::HubClientOperation::Shutdown,
            kind,
        }
    }

    #[test]
    fn production_core_shutdown_error_keeps_active_runtime_as_operator_error() {
        let error = response_after_core_shutdown_error(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "live-session",
        )
        .expect_err("Active plus Runtime stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::Runtime,
                ..
            })
        ));
    }

    #[test]
    fn production_core_shutdown_error_keeps_active_state_as_operator_error() {
        let error = response_after_core_shutdown_error(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::State),
            "live-session",
        )
        .expect_err("Active plus State stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::State,
                ..
            })
        ));
    }

    #[test]
    fn shutdown_unknown_session_error_while_active_is_already_exited_cleanup() {
        let response = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::UnknownSession),
            "live-session",
        )
        .expect("unknown-session while Active is cleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "live-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn shutdown_exited_classification_returns_cleanup_for_any_shutdown_error() {
        let response = shutdown_error_response(
            ShutdownSessionClassification::Cleanup(DaemonSessionCleanup {
                session_id: "exited-session".to_string(),
                outcome: "already_exited".to_string(),
            }),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "exited-session",
        )
        .expect("Cleanup classification stays SessionCleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "exited-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn shutdown_stopping_record_is_host_cleanup_not_active() {
        let record = botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId("stopping-session".to_string()),
                registry_state: RegistrySessionState::Stopping,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Stopping),
        };
        let classification = classify_found_session_lifecycle("stopping-session", &record);
        let response = shutdown_error_response(
            classification,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "stopping-session",
        )
        .expect("Stopping is host ShutdownSession cleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "stopping-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn recover_classify_err_preserves_typed_runtime_error() {
        let error = recover_from_exact_classify(
            Err(DaemonTransportError::Client(shutdown_lookup_error(
                botster_core_daemon::CoreDaemonError::Shutdown,
            ))),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "exited-session",
        )
        .expect_err("classify Err does not invent cleanup from collection state");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::Runtime,
                ..
            })
        ));
    }

    #[test]
    fn recover_recorded_stopping_after_classify_err_preserves_typed_error() {
        let error = recover_from_exact_classify(
            Err(DaemonTransportError::Client(shutdown_lookup_error(
                botster_core_daemon::CoreDaemonError::Shutdown,
            ))),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "stopping-session",
        )
        .expect_err("Stopping after classify Err keeps the typed Core error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::Runtime,
                ..
            })
        ));
        let response = daemon_operator_error(match error {
            DaemonTransportError::Client(error) => error,
            other => panic!("expected Client error, got {other:?}"),
        });
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error body");
        assert_eq!(operator.code, "runtime_error");
        assert_eq!(operator.operation, "shutdown");
    }

    #[test]
    fn recover_classify_err_preserves_typed_state_error() {
        let error = recover_from_exact_classify(
            Err(DaemonTransportError::Client(shutdown_lookup_error(
                botster_core_daemon::CoreDaemonError::Shutdown,
            ))),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::State),
            "stale-session",
        )
        .expect_err("classify Err keeps the original typed Core error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::State,
                ..
            })
        ));
    }

    #[test]
    fn recover_exact_missing_returns_unknown_session() {
        let response = recover_from_exact_classify(
            Ok(ShutdownSessionClassification::Missing),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "missing-session",
        )
        .expect("Missing classification stays unknown_session");
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let error = response.error.expect("unknown_session body");
        assert_eq!(error.code, "unknown_session");
        assert_eq!(error.operation, "shutdown");
        assert_eq!(error.message, "unknown session: missing-session");
    }

    #[test]
    fn recover_exact_exited_cleanup_stays_already_exited() {
        let response = recover_from_exact_classify(
            Ok(ShutdownSessionClassification::Cleanup(
                DaemonSessionCleanup {
                    session_id: "exited-session".to_string(),
                    outcome: "already_exited".to_string(),
                },
            )),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "exited-session",
        )
        .expect("exact Exited evidence stays SessionCleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "exited-session");
        assert_eq!(cleanup.outcome, "already_exited");
    }

    #[test]
    fn recover_exact_stale_cleanup_stays_stale_session() {
        let response = recover_from_exact_classify(
            Ok(ShutdownSessionClassification::Cleanup(
                DaemonSessionCleanup {
                    session_id: "stale-session".to_string(),
                    outcome: "stale_session".to_string(),
                },
            )),
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "stale-session",
        )
        .expect("exact Stale evidence stays SessionCleanup");
        assert_eq!(response.kind, DaemonResponseKind::SessionCleanup);
        let cleanup = response.cleanup.expect("cleanup body");
        assert_eq!(cleanup.session_id, "stale-session");
        assert_eq!(cleanup.outcome, "stale_session");
    }

    #[test]
    fn shutdown_active_runtime_error_remains_operator_error() {
        // OperatorError is preserved when exact evidence shows the worker is
        // still Active. Provable natural exit uses Cleanup, not this path.
        let error = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::Runtime),
            "live-session",
        )
        .expect_err("Active plus Runtime stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::Runtime,
                ..
            })
        ));
        let response = daemon_operator_error(match error {
            DaemonTransportError::Client(error) => error,
            other => panic!("expected Client error, got {other:?}"),
        });
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error body");
        assert_eq!(operator.code, "runtime_error");
        assert_eq!(operator.operation, "shutdown");
    }

    #[test]
    fn shutdown_active_state_error_remains_operator_error() {
        // OperatorError is preserved when exact evidence shows the worker is
        // still Active. Provable natural exit uses Cleanup, not this path.
        let error = shutdown_error_response(
            ShutdownSessionClassification::Active,
            shutdown_runtime_error(crate::HubClientRuntimeErrorKind::State),
            "live-session",
        )
        .expect_err("Active plus State stays an error");
        assert!(matches!(
            error,
            DaemonTransportError::Client(crate::HubClientError::Runtime {
                operation: crate::HubClientOperation::Shutdown,
                kind: crate::HubClientRuntimeErrorKind::State,
                ..
            })
        ));
        let response = daemon_operator_error(match error {
            DaemonTransportError::Client(error) => error,
            other => panic!("expected Client error, got {other:?}"),
        });
        assert_eq!(response.kind, DaemonResponseKind::OperatorError);
        let operator = response.error.expect("operator error body");
        assert_eq!(operator.code, "state_error");
        assert_eq!(operator.operation, "shutdown");
    }
}
