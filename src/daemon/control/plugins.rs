//! Plugin MCP and surface request family.

use std::collections::{BTreeMap, BTreeSet};

use botster_core::{
    PluginAdmissionResult, PluginHandlerRef, PluginInvocationClass, PluginInvocationResult,
    RequestId,
};
use botster_hub_client::{DaemonRequest, DaemonResponse, MAX_OUTSTANDING_REQUESTS};
use botster_ui_contract::{PackageSurfaceOperation, UiActionRequest};

use crate::HubDaemon;
use crate::client_api::{HubClientApi, HubClientOperation, HubClientPluginSurface};
use crate::client_api_dto::response::{
    daemon_plugin_action_result, daemon_plugin_lifecycle, daemon_plugin_surface,
    daemon_plugin_tool_result, daemon_plugin_tools,
};
use crate::daemon::control::pending::{ControlPoll, ControlStep};
use crate::daemon::control::reply::RetainedPluginResult;
use crate::daemon::control::{DaemonObservability, request_id};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult, daemon_plugin_tool_error};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::owner_identity::WaiterId;
use crate::{HubClientRequest, HubClientResponseBody, McpToolError};

const OWNER_PLUGIN_REQUEST_PREFIX: &str = "daemon-owner-plugin-";

#[derive(Debug, Clone, PartialEq, Eq)]
struct PluginInvocationIdentity {
    connection_id: String,
    connection_generation: String,
    transport_request_id: String,
    plugin_key: String,
    handler: PluginHandlerRef,
}

enum PendingPluginControlKind {
    McpTool,
    SurfaceRender {
        package_name: String,
        surface_id: String,
    },
    SurfaceAction {
        package_name: String,
        request: UiActionRequest,
    },
}

struct PendingPluginControl {
    waiter_id: WaiterId,
    identity: PluginInvocationIdentity,
    kind: PendingPluginControlKind,
    result: Option<RoutedPluginControlCompletion>,
}

enum RoutedPluginControlCompletion {
    Invocation(RetainedPluginResult<PluginInvocationResult>),
    Inconsistent(RetainedPluginResult<String>),
}

/// Bounded owner-side correlation for asynchronous plugin control work.
#[derive(Default)]
pub(crate) struct PluginControlState {
    next_serial: u64,
    pending: BTreeMap<String, PendingPluginControl>,
    completion_inconsistencies: u64,
    ready_waiters: BTreeSet<WaiterId>,
}

impl std::fmt::Debug for PluginControlState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginControlState")
            .field("pending", &self.pending.len())
            .field(
                "completion_inconsistencies",
                &self.completion_inconsistencies,
            )
            .finish_non_exhaustive()
    }
}

impl PluginControlState {
    #[cfg(test)]
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn has_transport_correlation(
        &self,
        connection_id: &str,
        transport_request_id: &str,
    ) -> bool {
        self.pending.values().any(|entry| {
            entry.identity.connection_id == connection_id
                && entry.identity.transport_request_id == transport_request_id
        })
    }

    fn connection_has_capacity(&self, connection_generation: &str) -> bool {
        self.pending
            .values()
            .filter(|entry| entry.identity.connection_generation == connection_generation)
            .count()
            < MAX_OUTSTANDING_REQUESTS
    }

    fn next_request_id(&mut self) -> Option<RequestId> {
        self.next_serial = self.next_serial.checked_add(1)?;
        Some(RequestId(format!(
            "{OWNER_PLUGIN_REQUEST_PREFIX}{}",
            self.next_serial
        )))
    }

    fn insert(
        &mut self,
        request_id: &RequestId,
        waiter_id: WaiterId,
        identity: PluginInvocationIdentity,
        kind: PendingPluginControlKind,
    ) {
        self.pending.insert(
            request_id.0.clone(),
            PendingPluginControl {
                waiter_id,
                identity,
                kind,
                result: None,
            },
        );
    }

    pub(crate) fn route_completion(
        &mut self,
        completion: RetainedPluginResult<botster_core::PluginCompletion>,
    ) -> Option<RetainedPluginResult<botster_core::PluginCompletion>> {
        let (request_id, handler) = completion_identity(&completion.value().result);
        let request_id = request_id.clone();
        let handler = handler.clone();
        if !request_id.0.starts_with(OWNER_PLUGIN_REQUEST_PREFIX) {
            return Some(completion);
        }
        let Some(entry) = self.pending.get_mut(&request_id.0) else {
            return None;
        };
        if completion.value().class != PluginInvocationClass::RequestResponse
            || entry.identity.plugin_key != handler.plugin_key.0
            || entry.identity.handler != handler
        {
            self.completion_inconsistencies = self.completion_inconsistencies.saturating_add(1);
            if entry.result.is_none() {
                entry.result = Some(RoutedPluginControlCompletion::Inconsistent(completion.map(
                    |_| {
                        format!(
                            "plugin completion identity did not match admitted request {}",
                            request_id.0
                        )
                    },
                )));
                self.ready_waiters.insert(entry.waiter_id);
            }
            return None;
        }
        if entry.result.is_none() {
            entry.result = Some(RoutedPluginControlCompletion::Invocation(
                completion.map(|completion| completion.result),
            ));
            self.ready_waiters.insert(entry.waiter_id);
        }
        None
    }

    fn take_ready(
        &mut self,
        request_id: &RequestId,
        identity: &PluginInvocationIdentity,
    ) -> Option<(PendingPluginControlKind, RoutedPluginControlCompletion)> {
        if !self
            .pending
            .get(&request_id.0)
            .is_some_and(|entry| entry.identity == *identity && entry.result.is_some())
        {
            return None;
        }
        let mut entry = self.pending.remove(&request_id.0)?;
        self.ready_waiters.remove(&entry.waiter_id);
        Some((entry.kind, entry.result.take()?))
    }

    fn retire(&mut self, request_id: &RequestId, identity: &PluginInvocationIdentity) {
        if self
            .pending
            .get(&request_id.0)
            .is_some_and(|entry| entry.identity == *identity)
        {
            if let Some(entry) = self.pending.remove(&request_id.0) {
                self.ready_waiters.remove(&entry.waiter_id);
            }
        }
    }

    pub(crate) fn take_ready_waiters(&mut self, limit: usize) -> Vec<WaiterId> {
        let waiters = self
            .ready_waiters
            .iter()
            .copied()
            .take(limit)
            .collect::<Vec<_>>();
        for waiter_id in &waiters {
            self.ready_waiters.remove(waiter_id);
        }
        waiters
    }
}

fn completion_identity(result: &PluginInvocationResult) -> (&RequestId, &PluginHandlerRef) {
    match result {
        PluginInvocationResult::Completed(success) => (&success.request_id, &success.handler),
        PluginInvocationResult::Failed(failure) => (&failure.request_id, &failure.handler),
    }
}

pub(crate) fn handle_request(
    daemon: &mut HubDaemon,
    request: DaemonRequest,
) -> DaemonTransportResult<DaemonResponse> {
    match request {
        DaemonRequest::PluginLifecycleStatus => plugin_lifecycle_response(daemon),
        _ => unreachable!("plugin control family received a non-plugin request"),
    }
}

fn plugin_lifecycle_response(daemon: &mut HubDaemon) -> DaemonTransportResult<DaemonResponse> {
    let packages = daemon.package_registry().clone();
    let api = HubClientApi::local_operator("botster-hub-daemon-socket");
    let Some(runtime) = daemon.runtime_mut() else {
        return Err(DaemonTransportError::DaemonNotRunning);
    };
    let response = api
        .handle_request(
            runtime,
            &packages,
            HubClientRequest::PluginLifecycleStatus {
                request_id: request_id("daemon-plugin-lifecycle-status"),
            },
        )
        .ready()?;
    let HubClientResponseBody::PluginLifecycle(report) = response.body else {
        return Err(DaemonTransportError::UnexpectedResponse);
    };
    Ok(daemon_plugin_lifecycle(report))
}

pub(crate) fn handle_runtime(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    observability: DaemonObservability,
    request: DaemonRequest,
) -> ControlStep {
    match request {
        DaemonRequest::PluginMcpListTools => daemon
            .runtime()
            .map(|runtime| ControlStep::ready(daemon_plugin_tools(runtime.list_plugin_mcp_tools())))
            .unwrap_or(ControlStep::Ready(Err(
                DaemonTransportError::DaemonNotRunning,
            ))),
        DaemonRequest::PluginMcpCallTool { name, arguments } => {
            let Some(request_id) = state.plugin_controls.next_request_id() else {
                return plugin_control_refused(
                    &PendingPluginControlKind::McpTool,
                    "plugin_request_id_exhausted",
                    "the daemon exhausted unique plugin request identifiers".to_string(),
                );
            };
            let Some(runtime) = daemon.runtime() else {
                return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
            };
            let request = match runtime.prepare_plugin_mcp_tool(
                crate::McpCallRequest { name, arguments },
                request_id,
                None,
            ) {
                Ok(request) => request,
                Err(error) => return ControlStep::ready(daemon_plugin_tool_error(error)),
            };
            start_plugin_control(
                daemon,
                state,
                &observability,
                request,
                PendingPluginControlKind::McpTool,
            )
        }
        DaemonRequest::PluginSurfaceRender {
            package_name,
            surface_id,
            payload,
        } => {
            if let Err(error) = crate::client_api::admit_plugin_surface_operation(
                daemon.package_registry(),
                &package_name,
                &surface_id,
                PackageSurfaceOperation::Render,
                request_id("daemon-plugin-surface-render"),
                HubClientOperation::PluginSurfaceRender,
            ) {
                return ControlStep::Ready(Err(error.into()));
            }
            let Some(request_id) = state.plugin_controls.next_request_id() else {
                return plugin_control_refused(
                    &PendingPluginControlKind::SurfaceRender {
                        package_name,
                        surface_id,
                    },
                    "plugin_request_id_exhausted",
                    "the daemon exhausted unique plugin request identifiers".to_string(),
                );
            };
            let Some(runtime) = daemon.runtime() else {
                return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
            };
            let request = match runtime.prepare_plugin_surface_render(
                &package_name,
                &surface_id,
                payload,
                request_id,
                None,
            ) {
                Ok(request) => request,
                Err(error) => {
                    return ControlStep::Ready(Err(surface_plugin_error(
                        HubClientOperation::PluginSurfaceRender,
                        "daemon-plugin-surface-render",
                        error,
                    )));
                }
            };
            start_plugin_control(
                daemon,
                state,
                &observability,
                request,
                PendingPluginControlKind::SurfaceRender {
                    package_name,
                    surface_id,
                },
            )
        }
        DaemonRequest::PluginSurfaceAction {
            package_name,
            request,
        } => {
            if let Err(error) = crate::client_api::admit_plugin_surface_operation(
                daemon.package_registry(),
                &package_name,
                &request.surface_id.0,
                PackageSurfaceOperation::Action,
                request_id("daemon-plugin-surface-action"),
                HubClientOperation::PluginSurfaceAction,
            ) {
                return ControlStep::Ready(Err(error.into()));
            }
            let Some(request_id) = state.plugin_controls.next_request_id() else {
                return plugin_control_refused(
                    &PendingPluginControlKind::SurfaceAction {
                        package_name,
                        request,
                    },
                    "plugin_request_id_exhausted",
                    "the daemon exhausted unique plugin request identifiers".to_string(),
                );
            };
            let Some(runtime) = daemon.runtime() else {
                return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
            };
            let invocation = match runtime.prepare_plugin_surface_action(
                &package_name,
                &request,
                request_id,
                None,
            ) {
                Ok(invocation) => invocation,
                Err(error) => {
                    return ControlStep::Ready(Err(surface_plugin_error(
                        HubClientOperation::PluginSurfaceAction,
                        "daemon-plugin-surface-action",
                        error,
                    )));
                }
            };
            start_plugin_control(
                daemon,
                state,
                &observability,
                invocation,
                PendingPluginControlKind::SurfaceAction {
                    package_name,
                    request,
                },
            )
        }
        _ => unreachable!("plugin runtime family received a non-plugin request"),
    }
}

fn start_plugin_control(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    observability: &DaemonObservability,
    request: botster_core::PluginInvocationRequest,
    kind: PendingPluginControlKind,
) -> ControlStep {
    let connection_id = observability
        .grant_id
        .clone()
        .or_else(|| observability.client_id.clone())
        .unwrap_or_else(|| "botster-hub-daemon-socket".to_string());
    let connection_generation = observability
        .grant_id
        .as_deref()
        .and_then(|grant_id| {
            state
                .pending_runtime
                .admission
                .webrtc_admissions
                .get(grant_id)
        })
        .map(|admission| match admission {
            crate::admission::unix_hello::WebrtcTerminalAdmission::Admitted {
                peer_generation,
                ..
            }
            | crate::admission::unix_hello::WebrtcTerminalAdmission::Rejected {
                peer_generation,
                ..
            } => format!("webrtc-{peer_generation}"),
        })
        .unwrap_or_else(|| connection_id.clone());
    if !state
        .plugin_controls
        .connection_has_capacity(&connection_generation)
    {
        return plugin_control_refused(
            &kind,
            "plugin_control_limit",
            format!("the connection already holds {MAX_OUTSTANDING_REQUESTS} plugin requests"),
        );
    }
    let identity = PluginInvocationIdentity {
        connection_id,
        connection_generation,
        transport_request_id: observability
            .transport_request_id
            .clone()
            .unwrap_or_else(|| request.request_id.0.clone()),
        plugin_key: request.handler.plugin_key.0.clone(),
        handler: request.handler.clone(),
    };
    let request_id = request.request_id.clone();
    let Some(runtime) = daemon.runtime() else {
        return ControlStep::Ready(Err(DaemonTransportError::DaemonNotRunning));
    };
    match runtime.try_admit_plugin(PluginInvocationClass::RequestResponse, request) {
        PluginAdmissionResult::Queued { .. } => {
            state.plugin_controls.insert(
                &request_id,
                state.current_waiter_id.expect("owner waiter is assigned"),
                identity.clone(),
                kind,
            );
            state.maintenance.try_wake();
            pending_plugin_control(request_id, identity)
        }
        PluginAdmissionResult::Backpressured { reason, .. } => {
            plugin_control_refused(&kind, "plugin_invocation_backpressured", reason)
        }
        PluginAdmissionResult::RejectedBudget { reason, .. } => {
            plugin_control_refused(&kind, "plugin_invocation_rejected", reason)
        }
        PluginAdmissionResult::WorkerStopped { reason, .. } => {
            plugin_control_refused(&kind, "plugin_worker_stopped", reason)
        }
        _ => plugin_control_refused(
            &kind,
            "plugin_invocation_rejected",
            "the plugin worker refused the invocation".to_string(),
        ),
    }
}

fn pending_plugin_control(
    request_id: RequestId,
    identity: PluginInvocationIdentity,
) -> ControlStep {
    let retire_request_id = request_id.clone();
    let retire_identity = identity.clone();
    ControlStep::pending_retirable_in(
        ReadyClass::PluginCompletion,
        move |daemon, state| {
            let Some((kind, result)) = state.plugin_controls.take_ready(&request_id, &identity)
            else {
                return ControlPoll::Pending;
            };
            ControlPoll::ReadyRetained(complete_plugin_control(daemon, kind, result))
        },
        move |_, state, _waiter_id, permit| {
            state
                .plugin_controls
                .retire(&retire_request_id, &retire_identity);
            // Core retains executor and completion capacity until execution
            // terminates. Hub releases only the retired reply reservation.
            state.budget.release(permit);
        },
    )
}

fn complete_plugin_control(
    daemon: &HubDaemon,
    kind: PendingPluginControlKind,
    completion: RoutedPluginControlCompletion,
) -> RetainedPluginResult<DaemonTransportResult<DaemonResponse>> {
    match completion {
        RoutedPluginControlCompletion::Inconsistent(message) => {
            message.map(|message| plugin_control_inconsistent(&kind, message))
        }
        RoutedPluginControlCompletion::Invocation(result) => result.map(|result| {
            let Some(runtime) = daemon.runtime() else {
                return Err(DaemonTransportError::DaemonNotRunning);
            };
            match kind {
                PendingPluginControlKind::McpTool => {
                    Ok(match crate::HubRuntime::complete_plugin_mcp_tool(result) {
                        Ok(value) => daemon_plugin_tool_result(value),
                        Err(error) => daemon_plugin_tool_error(error),
                    })
                }
                PendingPluginControlKind::SurfaceRender {
                    package_name,
                    surface_id,
                } => runtime
                    .complete_plugin_surface_render(&package_name, result)
                    .map(|body| {
                        daemon_plugin_surface(HubClientPluginSurface {
                            package_name,
                            surface_id,
                            body,
                        })
                    })
                    .map_err(|error| {
                        surface_plugin_error(
                            HubClientOperation::PluginSurfaceRender,
                            "daemon-plugin-surface-render",
                            error,
                        )
                    }),
                PendingPluginControlKind::SurfaceAction {
                    package_name,
                    request,
                } => runtime
                    .complete_plugin_surface_action(&package_name, &request, result)
                    .map(daemon_plugin_action_result)
                    .map_err(|error| {
                        surface_plugin_error(
                            HubClientOperation::PluginSurfaceAction,
                            "daemon-plugin-surface-action",
                            error,
                        )
                    }),
            }
        }),
    }
}

fn plugin_control_inconsistent(
    kind: &PendingPluginControlKind,
    message: String,
) -> DaemonTransportResult<DaemonResponse> {
    let error = McpToolError::new("plugin_completion_inconsistent", message);
    match kind {
        PendingPluginControlKind::McpTool => Ok(daemon_plugin_tool_error(error)),
        PendingPluginControlKind::SurfaceRender { .. } => Err(surface_plugin_error(
            HubClientOperation::PluginSurfaceRender,
            "daemon-plugin-surface-render",
            error,
        )),
        PendingPluginControlKind::SurfaceAction { .. } => Err(surface_plugin_error(
            HubClientOperation::PluginSurfaceAction,
            "daemon-plugin-surface-action",
            error,
        )),
    }
}

fn plugin_control_refused(
    kind: &PendingPluginControlKind,
    code: &str,
    message: String,
) -> ControlStep {
    let error = McpToolError::new(code, message);
    match kind {
        PendingPluginControlKind::McpTool => ControlStep::ready(daemon_plugin_tool_error(error)),
        PendingPluginControlKind::SurfaceRender { .. } => {
            ControlStep::Ready(Err(surface_plugin_error(
                HubClientOperation::PluginSurfaceRender,
                "daemon-plugin-surface-render",
                error,
            )))
        }
        PendingPluginControlKind::SurfaceAction { .. } => {
            ControlStep::Ready(Err(surface_plugin_error(
                HubClientOperation::PluginSurfaceAction,
                "daemon-plugin-surface-action",
                error,
            )))
        }
    }
}

fn surface_plugin_error(
    operation: HubClientOperation,
    request_id_value: &str,
    error: McpToolError,
) -> DaemonTransportError {
    DaemonTransportError::Client(crate::client_api::plugin_error(
        request_id(request_id_value),
        operation,
        error,
    ))
}

#[cfg(test)]
mod tests {
    use botster_core::{
        BoundaryJson, PluginCompletion, PluginHandlerKind, PluginInvocationFailure,
        PluginInvocationFailureKind, PluginInvocationSuccess, PluginKey,
    };

    use super::*;

    fn handler(plugin_key: &str, handler_id: &str) -> PluginHandlerRef {
        PluginHandlerRef {
            plugin_key: PluginKey(plugin_key.to_string()),
            kind: PluginHandlerKind::Command,
            handler_id: handler_id.to_string(),
        }
    }

    fn identity(
        connection_generation: &str,
        transport_request_id: &str,
        handler: &PluginHandlerRef,
    ) -> PluginInvocationIdentity {
        PluginInvocationIdentity {
            connection_id: format!("connection-{connection_generation}"),
            connection_generation: connection_generation.to_string(),
            transport_request_id: transport_request_id.to_string(),
            plugin_key: handler.plugin_key.0.clone(),
            handler: handler.clone(),
        }
    }

    fn successful_completion(request_id: RequestId, handler: PluginHandlerRef) -> PluginCompletion {
        PluginCompletion {
            class: PluginInvocationClass::RequestResponse,
            result: PluginInvocationResult::Completed(PluginInvocationSuccess {
                request_id,
                handler,
                payload: Some(BoundaryJson(serde_json::json!({ "value": "ok" }))),
            }),
        }
    }

    fn failed_completion(request_id: RequestId, handler: PluginHandlerRef) -> PluginCompletion {
        PluginCompletion {
            class: PluginInvocationClass::RequestResponse,
            result: PluginInvocationResult::Failed(PluginInvocationFailure {
                request_id,
                handler,
                kind: PluginInvocationFailureKind::HandlerFailed,
                timeout_ms: None,
                reason: "controlled failure".to_string(),
            }),
        }
    }

    fn retained(completion: PluginCompletion) -> RetainedPluginResult<PluginCompletion> {
        let budget = crate::daemon::control::reply::RetainedPluginResultBudget::new();
        let charge = budget.try_reserve(1).expect("test completion charge");
        RetainedPluginResult::new(completion, charge)
    }

    #[test]
    fn completion_routing_requires_the_exact_identity_and_discards_late_results() {
        let mut state = PluginControlState::default();
        let handler = handler("test.plugin", "tool");
        let request_id = state.next_request_id().expect("request id");
        let identity = identity("generation-1", "transport-1", &handler);
        state.insert(
            &request_id,
            WaiterId(1),
            identity.clone(),
            PendingPluginControlKind::McpTool,
        );

        assert!(
            state
                .route_completion(retained(successful_completion(
                    request_id.clone(),
                    handler.clone(),
                )))
                .is_none(),
            "the owner router must claim its exact completion"
        );
        let (_, completion) = state
            .take_ready(&request_id, &identity)
            .expect("exact completion");
        let RoutedPluginControlCompletion::Invocation(completion) = completion else {
            panic!("exact completion must preserve the invocation");
        };
        assert!(matches!(
            completion.value(),
            PluginInvocationResult::Completed(_)
        ));

        assert!(
            state
                .route_completion(retained(failed_completion(request_id, handler)))
                .is_none(),
            "a late completion in the reserved request-id domain must be discarded"
        );
        assert!(!state.has_pending());
    }

    #[test]
    fn completion_routing_rejects_a_handler_mismatch_without_reusing_the_row() {
        let mut state = PluginControlState::default();
        let expected_handler = handler("test.plugin", "expected");
        let request_id = state.next_request_id().expect("request id");
        let identity = identity("generation-1", "transport-1", &expected_handler);
        state.insert(
            &request_id,
            WaiterId(1),
            identity.clone(),
            PendingPluginControlKind::McpTool,
        );

        assert!(
            state
                .route_completion(retained(failed_completion(
                    request_id.clone(),
                    handler("test.plugin", "different"),
                )))
                .is_none()
        );
        let (_, completion) = state
            .take_ready(&request_id, &identity)
            .expect("inconsistent completion");
        assert!(matches!(
            completion,
            RoutedPluginControlCompletion::Inconsistent(_)
        ));
        assert_eq!(state.completion_inconsistencies, 1);
    }

    #[test]
    fn pending_control_capacity_is_exact_and_scoped_to_one_connection_generation() {
        let mut state = PluginControlState::default();
        let handler = handler("test.plugin", "tool");
        for serial in 0..MAX_OUTSTANDING_REQUESTS {
            let request_id = state.next_request_id().expect("request id");
            state.insert(
                &request_id,
                WaiterId(u64::try_from(serial).expect("test waiter id") + 1),
                identity("generation-1", &format!("transport-{serial}"), &handler),
                PendingPluginControlKind::McpTool,
            );
        }

        assert!(!state.connection_has_capacity("generation-1"));
        assert!(state.connection_has_capacity("generation-2"));
    }

    #[test]
    fn owner_plugin_request_identifiers_do_not_wrap() {
        let mut state = PluginControlState {
            next_serial: u64::MAX - 1,
            ..PluginControlState::default()
        };
        assert_eq!(
            state.next_request_id(),
            Some(RequestId(format!(
                "{OWNER_PLUGIN_REQUEST_PREFIX}{}",
                u64::MAX
            )))
        );
        assert_eq!(state.next_request_id(), None);
        assert_eq!(state.next_request_id(), None);
    }
}
