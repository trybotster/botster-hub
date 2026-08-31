//! Unix and WebRTC Hello admission registries and decisions.

use std::collections::BTreeMap;
use std::ops::Bound;

use botster_core::TerminalCapabilitySet;
use botster_hub_client::{
    DaemonCompatibility, DaemonDiagnostic, DaemonHello, DaemonHelloAck, DaemonOperatorError,
    DaemonResponse, DaemonResponseKind, PROTOCOL,
};
use botster_terminal_protocol::{
    TerminalCompatibility, ensure_compatible as ensure_terminal_compatible,
};

use crate::client_api_dto::response::daemon_response_base;
use crate::subscription::attach_routes::negotiated_unix_capability_set;
use crate::transport::unix::UnixConnectionMux;
use crate::transport::webrtc::WebRtcConnectionMux;

#[derive(Clone, Debug)]
pub(crate) enum UnixTerminalAdmission {
    Admitted {
        #[allow(dead_code)]
        required_features: Vec<String>,
        capabilities: TerminalCapabilitySet,
        mux: UnixConnectionMux,
    },
    Rejected {
        code: &'static str,
        diagnostic: DaemonDiagnostic,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum WebrtcTerminalAdmission {
    Admitted {
        required_features: Vec<String>,
        mux: WebRtcConnectionMux,
        terminal_requirement: Option<botster_terminal_protocol::TerminalCompatibilityRequirement>,
        peer_generation: u64,
    },
    Rejected {
        code: &'static str,
        diagnostic: DaemonDiagnostic,
    },
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HostCompatibilityRecord {
    pub required_features: Vec<String>,
}

#[derive(Default)]
pub(crate) struct AdmissionState {
    pub unix_admissions: BTreeMap<String, UnixTerminalAdmission>,
    pub webrtc_admissions: BTreeMap<String, WebrtcTerminalAdmission>,
    pub host_compatibility: BTreeMap<String, HostCompatibilityRecord>,
    pub next_peer_generation: u64,
    pub reservations: crate::admission::reservations::TerminalReservationRegistry,
}

pub(crate) fn daemon_hello_ack(diagnostics: Vec<DaemonDiagnostic>) -> DaemonHelloAck {
    DaemonHelloAck {
        protocol: PROTOCOL.to_string(),
        compatibility: DaemonCompatibility::current(),
        terminal_compatibility: Some(TerminalCompatibility::current()),
        diagnostics,
    }
}

pub(crate) fn unix_hello_admission(hello: &DaemonHello) -> (UnixTerminalAdmission, DaemonHelloAck) {
    let mut diagnostics = vec![DaemonDiagnostic::connected("hello")];
    if let Some(requirement) = hello.terminal_compatibility.as_ref()
        && let Err(error) =
            ensure_terminal_compatible(requirement, &TerminalCompatibility::current())
    {
        let diagnostic = DaemonDiagnostic::compatibility_mismatch(error.diagnostic);
        diagnostics.push(diagnostic.clone());
        return (
            UnixTerminalAdmission::Rejected {
                code: "terminal_compatibility",
                diagnostic,
            },
            daemon_hello_ack(diagnostics),
        );
    }
    let capabilities = negotiated_unix_capability_set(
        &hello.compatibility.required_features,
        hello.terminal_compatibility.as_ref(),
    )
    .unwrap_or_else(|_| TerminalCapabilitySet::empty());
    (
        UnixTerminalAdmission::Admitted {
            required_features: hello.compatibility.required_features.clone(),
            capabilities,
            mux: UnixConnectionMux::new(),
        },
        daemon_hello_ack(diagnostics),
    )
}

pub(crate) fn terminal_compatibility_attach_error(
    code: &'static str,
    diagnostic: DaemonDiagnostic,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: "daemon-attach-terminal-compatibility".to_string(),
        operation: "attach".to_string(),
        message: diagnostic
            .message
            .clone()
            .unwrap_or_else(|| "terminal compatibility mismatch".to_string()),
        diagnostics: vec![diagnostic],
    });
    response
}

#[allow(dead_code)]
pub(crate) fn next_admission_key<T>(
    map: &BTreeMap<String, T>,
    after: Option<&str>,
) -> Option<String> {
    match after {
        None => map.keys().next().cloned(),
        Some(seen) => map
            .range::<str, _>((Bound::Excluded(seen), Bound::Unbounded))
            .next()
            .map(|(key, _)| key.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn admission_cursor_uses_exclusive_range_not_a_prefix_scan() {
        let mut admissions = BTreeMap::new();
        for index in 0..20 {
            admissions.insert(format!("client-{index:02}"), ());
        }
        assert_eq!(
            next_admission_key(&admissions, None).as_deref(),
            Some("client-00")
        );
        assert_eq!(
            next_admission_key(&admissions, Some("client-09")).as_deref(),
            Some("client-10")
        );
        assert_eq!(next_admission_key(&admissions, Some("client-19")), None);
    }
}
