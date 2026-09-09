//! Host construction and encoding for Status and Shutdown responses.
//!
//! Seed capture remains Owner work. Core inventory admission is a separate dependency.

use std::mem::size_of;
use std::sync::Arc;

use botster_hub_client::{
    DaemonAttachOccupancy, DaemonCompatibility, DaemonDiagnostic, DaemonInstallationDiagnostic,
    DaemonInstallationIdentity, DaemonLifecycleCounters, DaemonLocalWebrtcTerminalRecord,
    DaemonOperatorError, DaemonResponse, DaemonResponseKind, DaemonRetentionAccounting,
    DaemonSoftwareIdentity, MAX_CONTROL_RESPONSE_BYTES,
};
use serde::Serialize;

use crate::HubDaemonStatus;
use crate::bounded_json::{EncodeError, encoded_len};
use crate::event_plane_counters::EventPlaneCounters;
use crate::host_executor::HostError;

pub(crate) struct StatusResponseInput {
    #[cfg(test)]
    pub(crate) drop_probe: Option<crate::host_executor::TestDisposalProbe>,
    pub(crate) status: HubDaemonStatus,
    pub(crate) session_count: usize,
    pub(crate) egress: Vec<DaemonDiagnostic>,
    pub(crate) lifecycle: DaemonLifecycleCounters,
    pub(crate) software: DaemonSoftwareIdentity,
    pub(crate) installation: DaemonInstallationIdentity,
    pub(crate) compatibility: DaemonCompatibility,
    pub(crate) counters: Arc<EventPlaneCounters>,
    pub(crate) retention: Option<DaemonRetentionAccounting>,
    pub(crate) occupancy: Vec<DaemonAttachOccupancy>,
    pub(crate) terminal_records: Vec<DaemonLocalWebrtcTerminalRecord>,
    pub(crate) request_id: String,
    pub(crate) shutdown: bool,
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

fn too_large() -> HostError {
    HostError::new(
        "host_result_too_large",
        "status response exceeds its prepared-byte reservation",
    )
}

/// Count logical element storage and a conservative bound for owned string bytes.
/// JSON length bounds string contents; element storage covers vector and map entries.
/// This count does not claim allocator capacity or admission of the earlier seed capture.
fn seed_bytes(input: &StatusResponseInput, limit: usize) -> Option<usize> {
    let mut bytes = size_of::<StatusResponseInput>();
    let mut add = |value: usize| {
        bytes = bytes.checked_add(value)?;
        (bytes <= limit).then_some(())
    };
    add(input.status.host_id.len())?;
    add(input.status.host_display_name.len())?;
    add(input.request_id.len())?;
    for rows in [
        &input.status.recovered_sessions,
        &input.status.stale_sessions,
    ] {
        add(rows
            .len()
            .checked_mul(size_of::<botster_core::SessionId>())?)?;
        for row in rows {
            add(row.0.len())?;
        }
    }
    add(encoded_len(&input.egress, limit).ok()?)?;
    add(input
        .egress
        .len()
        .checked_mul(size_of::<DaemonDiagnostic>())?)?;
    add(encoded_len(&input.lifecycle, limit).ok()?)?;
    add(input
        .lifecycle
        .cleanup_by_reason
        .len()
        .checked_mul(size_of::<(String, u64)>())?)?;
    add(encoded_len(&input.software, limit).ok()?)?;
    add(encoded_len(&input.installation, limit).ok()?)?;
    add(input
        .installation
        .diagnostics
        .len()
        .checked_mul(size_of::<DaemonInstallationDiagnostic>())?)?;
    add(encoded_len(&input.compatibility, limit).ok()?)?;
    add(input
        .compatibility
        .features
        .len()
        .checked_mul(size_of::<String>())?)?;
    add(encoded_len(&input.occupancy, limit).ok()?)?;
    add(input
        .occupancy
        .len()
        .checked_mul(size_of::<DaemonAttachOccupancy>())?)?;
    add(encoded_len(&input.terminal_records, limit).ok()?)?;
    add(input
        .terminal_records
        .len()
        .checked_mul(size_of::<DaemonLocalWebrtcTerminalRecord>())?)?;
    Some(bytes)
}

/// Construct, count, encode, and destroy the typed response on one Host worker.
pub(crate) fn prepare(mut input: StatusResponseInput, limit: usize) -> PreparedStatusResponse {
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
    let seed = seed_bytes(&input, limit)
        .and_then(|bytes| bytes.checked_add(request_id.len()))
        .ok_or_else(too_large)?;
    // The projection copies host identity, session IDs, and egress diagnostics.
    // Reserve a second complete seed as a conservative bound for those copies.
    let typed = seed
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<DaemonResponse>()))
        .and_then(|bytes| {
            bytes.checked_add(
                size_of::<DaemonDiagnostic>()
                    + "connectedstatusshutdowncreatedrunningstoppedloadedinitialized".len(),
            )
        })
        .ok_or_else(too_large)?;
    let (counters, counter_bytes) = input
        .counters
        .bounded_snapshot(limit.checked_sub(typed).ok_or_else(too_large)?)
        .ok_or_else(too_large)?;
    let typed = typed.checked_add(counter_bytes).ok_or_else(too_large)?;
    let kind = if input.shutdown {
        DaemonResponseKind::Shutdown
    } else {
        DaemonResponseKind::Status
    };
    let mut response = crate::client_api_dto::response::daemon_response_base(kind);
    let mut status = crate::daemon_projection::daemon_status_from_status(
        &input.status,
        input.session_count,
        input.egress.clone(),
        input.lifecycle,
        input.software,
        input.installation,
        counters,
        input.retention,
        input.compatibility,
    );
    status.live_attach_occupancy = input.occupancy;
    status.local_webrtc_terminal_records = input.terminal_records;
    response.status = Some(status);
    response.diagnostics = Vec::with_capacity(input.egress.len() + 1);
    response
        .diagnostics
        .push(DaemonDiagnostic::connected(if input.shutdown {
            "shutdown"
        } else {
            "status"
        }));
    response.diagnostics.extend(input.egress);
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
        status: HubDaemonStatus {
            lifecycle_state: crate::HubDaemonState::Running,
            host_id: "host-1".to_string(),
            host_display_name: "Host \"one\"".to_string(),
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
        software: DaemonSoftwareIdentity {
            product_id: "botster".to_string(),
            product_name: "Botster".to_string(),
            version: "test".to_string(),
            build_revision: None,
        },
        installation: DaemonInstallationIdentity {
            mode: botster_hub_client::DaemonInstallationMode::Unmanaged,
            provenance: "unmanaged".to_string(),
            release_channel: None,
            provider: None,
            diagnostics: Vec::new(),
        },
        compatibility: DaemonCompatibility::current(),
        counters: Arc::new(EventPlaneCounters::new()),
        retention: None,
        occupancy: Vec::new(),
        terminal_records: Vec::new(),
        request_id: "41".to_string(),
        shutdown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_status_preserves_frame_and_diagnostic_fields() {
        let input = test_input(false);
        input
            .counters
            .register_missing(crate::event_plane_counters::AgeIdentity {
                kind: botster_hub_client::DaemonQueueKind::Producer,
                identity: "queue".to_string(),
                generation: Some(3),
            });
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
        input.status.host_display_name = "\\".repeat(MAX_CONTROL_RESPONSE_BYTES);
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
