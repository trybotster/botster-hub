//! Host construction and encoding for Status and Shutdown responses.
//!
//! Owner capture and Host preparation share one retained byte allowance.
//! Core inventory preallocation remains a separate dependency.

use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Arc;

use botster_hub_client::{
    DaemonAttachOccupancy, DaemonDiagnostic, DaemonLifecycleCounters,
    DaemonLocalWebrtcTerminalRecord, DaemonOperatorError, DaemonResponse, DaemonResponseKind,
    DaemonRetentionAccounting, MAX_CONTROL_RESPONSE_BYTES,
};
use serde::Serialize;

use crate::HubDaemonStatus;
use crate::bounded_json::{EncodeError, encoded_len};
use crate::event_plane_counters::EventPlaneCounters;
use crate::host_executor::{HostError, HostRetainedPrepared};

pub(crate) struct StatusResponseInput {
    #[cfg(test)]
    pub(crate) drop_probe: Option<crate::host_executor::TestDisposalProbe>,
    #[cfg(test)]
    pub(crate) capture_observer: Option<std::sync::mpsc::Sender<(bool, usize, usize, bool)>>,
    pub(crate) seed: Option<StatusResponseSeed>,
    pub(crate) core: Option<StatusCoreSnapshot>,
    pub(crate) request_id: String,
    pub(crate) shutdown: bool,
    pub(crate) rejected: bool,
}

pub(crate) struct StatusResponseSeed {
    pub(crate) status: HubDaemonStatus,
    pub(crate) session_count: usize,
    pub(crate) egress: Vec<DaemonDiagnostic>,
    pub(crate) lifecycle: DaemonLifecycleCounters,
    pub(crate) installation_home: Option<PathBuf>,
    pub(crate) counters: Arc<EventPlaneCounters>,
    pub(crate) retention: Option<DaemonRetentionAccounting>,
    pub(crate) occupancy: Vec<DaemonAttachOccupancy>,
    pub(crate) terminal_records: Vec<DaemonLocalWebrtcTerminalRecord>,
}

/// The reservation remains after inventory in field destruction order.
pub(crate) struct StatusCoreSnapshot {
    pub(crate) accounting: botster_core_daemon::RetentionAccounting,
    pub(crate) inventory: Option<Vec<botster_core::TerminalSubscriptionRecord>>,
    _reservation: HostRetainedPrepared,
}

impl StatusCoreSnapshot {
    pub(crate) fn new(
        accounting: botster_core_daemon::RetentionAccounting,
        inventory: Option<Vec<botster_core::TerminalSubscriptionRecord>>,
        reservation: HostRetainedPrepared,
    ) -> Self {
        Self {
            accounting,
            inventory,
            _reservation: reservation,
        }
    }

    pub(crate) fn logical_bytes(&self, limit: usize) -> Option<usize> {
        let mut bytes = size_of::<Self>();
        if let Some(inventory) = self.inventory.as_ref() {
            bytes = checked_live_bytes(
                limit,
                [
                    bytes,
                    inventory
                        .len()
                        .checked_mul(size_of::<botster_core::TerminalSubscriptionRecord>())?,
                ],
            )?;
            for row in inventory {
                bytes = checked_live_bytes(
                    limit,
                    [
                        bytes,
                        row.client_id.0.len(),
                        row.session_id.0.len(),
                        row.subscription_id.0.len(),
                    ],
                )?;
                if let Some(capabilities) = row.capabilities.as_ref() {
                    for token in capabilities.iter() {
                        bytes =
                            checked_live_bytes(limit, [bytes, size_of::<String>(), token.len()])?;
                    }
                }
            }
        }
        (bytes <= limit).then_some(bytes)
    }
}

/// Combine all simultaneously live logical storage under one original allowance.
pub(crate) fn checked_live_bytes(
    limit: usize,
    parts: impl IntoIterator<Item = usize>,
) -> Option<usize> {
    parts.into_iter().try_fold(0usize, |bytes, part| {
        let bytes = bytes.checked_add(part)?;
        (bytes <= limit).then_some(bytes)
    })
}

pub(crate) fn lifecycle_bytes(value: &DaemonLifecycleCounters, limit: usize) -> Option<usize> {
    let mut bytes = checked_live_bytes(
        limit,
        [
            size_of::<DaemonLifecycleCounters>(),
            value
                .cleanup_by_reason
                .len()
                .checked_mul(size_of::<(String, u64)>())?,
        ],
    )?;
    for reason in value.cleanup_by_reason.keys() {
        bytes = checked_live_bytes(limit, [bytes, reason.len()])?;
    }
    Some(bytes)
}

impl StatusResponseInput {
    pub(crate) fn logical_bytes(&self, limit: usize) -> Option<usize> {
        let mut bytes = checked_live_bytes(limit, [size_of::<Self>(), self.request_id.len()])?;
        if let Some(seed) = self.seed.as_ref() {
            let status = &seed.status;
            bytes = checked_live_bytes(
                limit,
                [
                    bytes,
                    status.host_id.len(),
                    status.host_display_name.len(),
                    seed.installation_home
                        .as_ref()
                        .map_or(0, |home| home.as_os_str().len()),
                    lifecycle_bytes(&seed.lifecycle, limit)?
                        .checked_sub(size_of::<DaemonLifecycleCounters>())?,
                ],
            )?;
            for rows in [&status.recovered_sessions, &status.stale_sessions] {
                bytes = checked_live_bytes(
                    limit,
                    [
                        bytes,
                        rows.len()
                            .checked_mul(size_of::<botster_core::SessionId>())?,
                    ],
                )?;
                for row in rows {
                    bytes = checked_live_bytes(limit, [bytes, row.0.len()])?;
                }
            }
            bytes = checked_live_bytes(
                limit,
                [
                    bytes,
                    seed.egress
                        .len()
                        .checked_mul(size_of::<DaemonDiagnostic>())?,
                    seed.occupancy
                        .len()
                        .checked_mul(size_of::<DaemonAttachOccupancy>())?,
                    seed.terminal_records
                        .len()
                        .checked_mul(size_of::<DaemonLocalWebrtcTerminalRecord>())?,
                ],
            )?;
            for row in &seed.egress {
                for text in [&row.operation, &row.feature, &row.message]
                    .into_iter()
                    .flatten()
                {
                    bytes = checked_live_bytes(limit, [bytes, text.len()])?;
                }
            }
            for row in &seed.occupancy {
                bytes = checked_live_bytes(
                    limit,
                    [bytes, row.session_id.len(), row.subscription_id.len()],
                )?;
            }
            for row in &seed.terminal_records {
                // Encoded length conservatively includes every owned string.
                bytes = checked_live_bytes(limit, [bytes, encoded_len(row, limit).ok()?])?;
            }
        }
        if let Some(core) = self.core.as_ref() {
            bytes = checked_live_bytes(
                limit,
                [
                    bytes,
                    core.logical_bytes(limit)?
                        .checked_sub(size_of::<StatusCoreSnapshot>())?,
                ],
            )?;
        }
        Some(bytes)
    }
}

/// The original Host permit charges these bytes until delivery or worker disposal.
#[derive(Debug)]
pub(crate) struct PreparedStatusResponse {
    #[cfg(test)]
    pub(crate) dispose_probe: Option<crate::host_executor::TestDisposalProbe>,
    pub(crate) kind: DaemonResponseKind,
    pub(crate) encoded_frame: Option<Vec<u8>>,
    pub(crate) shutdown: bool,
}

impl PreparedStatusResponse {
    pub(crate) fn logical_bytes(&self) -> usize {
        self.encoded_frame.as_ref().map_or(0, Vec::len)
    }
}

#[derive(Serialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
enum BorrowedServerFrame<'a> {
    Response {
        request_id: &'a str,
        response: &'a DaemonResponse,
    },
}

pub(crate) fn too_large() -> HostError {
    HostError::new(
        "host_result_too_large",
        "status response exceeds its prepared-byte reservation",
    )
}

/// Construct, count, encode, and destroy the typed response on one Host worker.
pub(crate) fn prepare(mut input: StatusResponseInput, limit: usize) -> PreparedStatusResponse {
    #[cfg(test)]
    if let Some(observer) = input.capture_observer.take() {
        let (occupancy, terminals) = input.seed.as_ref().map_or((0, 0), |seed| {
            (seed.occupancy.len(), seed.terminal_records.len())
        });
        let _ = observer.send((input.rejected, occupancy, terminals, input.core.is_some()));
    }
    // Obsolete inventory must drop on Host before metadata creates more owned values.
    drop(input.core.take());
    let request_id = std::mem::take(&mut input.request_id);
    let shutdown = input.shutdown;
    match try_prepare(input, &request_id, limit) {
        Ok(prepared) => prepared,
        // try_prepare destroys the rejected typed response before fallback allocation.
        Err(error) => capacity_response(error, &request_id, shutdown, limit),
    }
}

fn capacity_response(
    error: HostError,
    request_id: &str,
    shutdown: bool,
    limit: usize,
) -> PreparedStatusResponse {
    let kind = DaemonResponseKind::OperatorError;
    let operation = if shutdown { "shutdown" } else { "status" };
    let encoded_frame = (|| {
        let typed = size_of::<DaemonResponse>()
            .checked_add(size_of::<HostError>())?
            .checked_add(request_id.len().checked_mul(2)?)?
            .checked_add(operation.len())?
            .checked_add(error.code.len())?
            .checked_add(error.message.len())?;
        let encoded_limit = MAX_CONTROL_RESPONSE_BYTES.min(limit.checked_sub(typed)?);
        let mut response = crate::client_api_dto::response::daemon_response_base(kind);
        response.error = Some(DaemonOperatorError {
            code: error.code,
            request_id: request_id.to_string(),
            operation: operation.to_string(),
            message: error.message,
            diagnostics: Vec::new(),
        });
        let frame = BorrowedServerFrame::Response {
            request_id,
            response: &response,
        };
        let length = encoded_len(&frame, encoded_limit).ok()?;
        crate::bounded_json::encode(&frame, length).ok()
    })();
    // An absent frame closes the reply on Host. It must not cancel an admitted shutdown.
    PreparedStatusResponse {
        #[cfg(test)]
        dispose_probe: None,
        kind,
        encoded_frame,
        shutdown,
    }
}

fn try_prepare(
    input: StatusResponseInput,
    request_id: &str,
    limit: usize,
) -> Result<PreparedStatusResponse, HostError> {
    if input.rejected || input.seed.is_none() {
        return Err(too_large());
    }
    let live = input
        .logical_bytes(limit)
        .and_then(|bytes| checked_live_bytes(limit, [bytes, request_id.len()]))
        .ok_or_else(too_large)?;
    let seed = input.seed.as_ref().expect("the admitted input has a seed");
    let identity_bound = crate::maintenance::status_identity_prepared_bytes(
        seed.installation_home.as_deref(),
        limit.checked_sub(live).ok_or_else(too_large)?,
    )
    .ok_or_else(too_large)?;
    checked_live_bytes(limit, [live, identity_bound]).ok_or_else(too_large)?;
    let (software, installation, compatibility, identity_bytes) =
        crate::maintenance::bounded_status_identity(
            seed.installation_home.as_deref(),
            identity_bound,
        )?;
    // The projection copies host identity, reconciliation rows, and diagnostics.
    // A second seed bounds those copies while the original input remains live.
    let typed = checked_live_bytes(
        limit,
        [
            live,
            live,
            identity_bytes,
            size_of::<DaemonResponse>(),
            size_of::<DaemonDiagnostic>(),
            "connectedstatusshutdowncreatedrunningstoppedloadedinitialized".len(),
        ],
    )
    .ok_or_else(too_large)?;
    let (counters, counter_bytes) = seed
        .counters
        .bounded_snapshot(limit.checked_sub(typed).ok_or_else(too_large)?)
        .ok_or_else(too_large)?;
    let typed = checked_live_bytes(limit, [typed, counter_bytes]).ok_or_else(too_large)?;
    let seed = input.seed.expect("the admitted input has a seed");
    let kind = if input.shutdown {
        DaemonResponseKind::Shutdown
    } else {
        DaemonResponseKind::Status
    };
    let mut response = crate::client_api_dto::response::daemon_response_base(kind);
    let mut status = crate::daemon_projection::daemon_status_from_status(
        &seed.status,
        seed.session_count,
        seed.egress.clone(),
        seed.lifecycle,
        software,
        installation,
        counters,
        seed.retention,
        compatibility,
    );
    status.live_attach_occupancy = seed.occupancy;
    status.local_webrtc_terminal_records = seed.terminal_records;
    response.status = Some(status);
    response.diagnostics = Vec::with_capacity(seed.egress.len() + 1);
    response
        .diagnostics
        .push(DaemonDiagnostic::connected(if input.shutdown {
            "shutdown"
        } else {
            "status"
        }));
    response.diagnostics.extend(seed.egress);
    let frame = BorrowedServerFrame::Response {
        request_id,
        response: &response,
    };
    let encoded_limit =
        MAX_CONTROL_RESPONSE_BYTES.min(limit.checked_sub(typed).ok_or_else(too_large)?);
    let length = encoded_len(&frame, encoded_limit).map_err(encode_error)?;
    let encoded_frame = crate::bounded_json::encode(&frame, length).map_err(encode_error)?;
    // All typed fields, including the remaining input fields, drop on this worker.
    Ok(PreparedStatusResponse {
        #[cfg(test)]
        dispose_probe: None,
        kind,
        encoded_frame: Some(encoded_frame),
        shutdown: input.shutdown,
    })
}

fn encode_error(error: EncodeError) -> HostError {
    match error {
        EncodeError::TooLarge => too_large(),
        EncodeError::Serialize => HostError::new(
            "status_encoding_failed",
            "status response did not serialize",
        ),
    }
}

#[cfg(test)]
pub(crate) fn test_input(shutdown: bool) -> StatusResponseInput {
    StatusResponseInput {
        drop_probe: None,
        capture_observer: None,
        seed: Some(StatusResponseSeed {
            status: HubDaemonStatus {
                lifecycle_state: crate::HubDaemonState::Running,
                host_id: "host-1".to_string(),
                host_display_name: r#"Host "one""#.to_string(),
                schema_version: 3,
                data_dir_configured: true,
                core_initialized: true,
                state_source: crate::HubStateLoadSource::Loaded,
                package_count: 4,
                enabled_package_count: 3,
                provider_count: 2,
                enabled_provider_count: 1,
                recovered_sessions: vec![botster_core::SessionId("recovered".to_string())],
                stale_sessions: vec![botster_core::SessionId("stale".to_string())],
            },
            session_count: 7,
            egress: if shutdown {
                Vec::new()
            } else {
                vec![DaemonDiagnostic::connected("egress")]
            },
            lifecycle: DaemonLifecycleCounters::default(),
            installation_home: None,
            counters: Arc::new(EventPlaneCounters::new()),
            retention: None,
            occupancy: Vec::new(),
            terminal_records: Vec::new(),
        }),
        core: None,
        request_id: "41".to_string(),
        shutdown,
        rejected: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_input_counts_aggregate_storage_at_exact_fit_and_one_byte_short() {
        let mut input = test_input(false);
        input
            .seed
            .as_mut()
            .unwrap()
            .lifecycle
            .cleanup_by_reason
            .insert("retained-reason".repeat(32), 1);
        let bytes = input.logical_bytes(usize::MAX).unwrap();
        assert_eq!(input.logical_bytes(bytes), Some(bytes));
        assert!(input.logical_bytes(bytes - 1).is_none());
        assert!(checked_live_bytes(usize::MAX, [usize::MAX, 1]).is_none());
        let part = bytes / 2 + 1;
        assert_eq!(checked_live_bytes(bytes, [part]), Some(part));
        assert!(checked_live_bytes(bytes, [part, part]).is_none());
    }

    #[test]
    fn status_preparation_counts_input_projection_counters_and_encoding_together() {
        let mut input = test_input(false);
        let request_id = std::mem::take(&mut input.request_id);
        let live = input.logical_bytes(usize::MAX).unwrap() + request_id.len();
        let seed = input.seed.as_ref().unwrap();
        let identity =
            crate::maintenance::status_identity_prepared_bytes(None, usize::MAX).unwrap();
        let (_, counters) = seed.counters.bounded_snapshot(usize::MAX).unwrap();
        let typed = checked_live_bytes(
            usize::MAX,
            [
                live,
                live,
                identity,
                counters,
                size_of::<DaemonResponse>(),
                size_of::<DaemonDiagnostic>(),
                "connectedstatusshutdowncreatedrunningstoppedloadedinitialized".len(),
            ],
        )
        .unwrap();
        let encoded = prepare(
            test_input(false),
            crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
        )
        .encoded_frame
        .unwrap()
        .len();
        let exact = typed + encoded;
        assert!(prepare(test_input(false), exact).encoded_frame.is_some());
        let exact_response = prepare(test_input(false), exact);
        assert_eq!(exact_response.kind, DaemonResponseKind::Status);
        let short = prepare(test_input(false), exact - 1);
        assert_eq!(short.kind, DaemonResponseKind::OperatorError);
        let frame: botster_hub_client::ServerFrame = serde_json::from_slice(
            short
                .encoded_frame
                .as_ref()
                .expect("the bounded fallback fits"),
        )
        .unwrap();
        let botster_hub_client::ServerFrame::Response {
            request_id,
            response,
        } = frame
        else {
            panic!("response frame");
        };
        assert_eq!(request_id, "41");
        assert_eq!(response.error.unwrap().code, "host_result_too_large");
    }

    #[test]
    fn encoded_status_preserves_frame_and_diagnostic_fields() {
        let input = test_input(false);
        input.seed.as_ref().unwrap().counters.register_missing(
            crate::event_plane_counters::AgeIdentity {
                kind: botster_hub_client::DaemonQueueKind::Producer,
                identity: "queue".to_string(),
                generation: Some(3),
            },
        );
        let prepared = prepare(input, crate::host_executor::HOST_PREPARED_BYTE_CAPACITY);
        let frame: botster_hub_client::ServerFrame =
            serde_json::from_slice(prepared.encoded_frame.as_ref().expect("encoded response"))
                .expect("decode response frame");
        let botster_hub_client::ServerFrame::Response {
            request_id,
            response,
        } = frame
        else {
            panic!("response frame");
        };
        assert_eq!(request_id, "41");
        assert_eq!(response.kind, DaemonResponseKind::Status);
        let status = response.status.expect("status is present");
        assert_eq!(status.session_count, 7);
        assert_eq!(status.host_display_name, "Host \"one\"");
        assert_eq!(status.recovered_sessions, ["recovered"]);
        assert_eq!(status.observability.queue_ages[0].identity, "queue");
        assert_eq!(status.diagnostics.len(), 1);
        assert_eq!(response.diagnostics.len(), 2);
        assert!(!prepared.shutdown);
    }

    #[test]
    fn status_preflight_rejects_typed_and_encoded_overflow() {
        assert!(try_prepare(test_input(false), "41", 1).is_err());
        let mut input = test_input(false);
        input.seed.as_mut().unwrap().status.host_display_name =
            "\\".repeat(MAX_CONTROL_RESPONSE_BYTES);
        let error = try_prepare(
            input,
            "41",
            crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
        )
        .expect_err("escaped output exceeds transport limit");
        assert_eq!(error.code, "host_result_too_large");
    }

    #[test]
    fn shutdown_survives_an_unencodable_fallback() {
        let prepared = prepare(test_input(true), 1);
        assert!(prepared.shutdown);
        assert_eq!(prepared.kind, DaemonResponseKind::OperatorError);
        assert!(prepared.encoded_frame.is_none());
    }
}
