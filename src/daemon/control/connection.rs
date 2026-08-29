//! Unix and WebRTC admission registration.

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::HubDaemon;
use crate::admission::unix_hello::{
    HostCompatibilityRecord, UnixTerminalAdmission, WebrtcTerminalAdmission,
};
use crate::daemon::owner_loop::DaemonControlState;

pub(crate) fn register_unix_admission(
    state: &mut DaemonControlState,
    client_id: String,
    admission: UnixTerminalAdmission,
    reply_tx: oneshot::Sender<()>,
    host_required_features: Vec<String>,
) -> bool {
    if let UnixTerminalAdmission::Admitted { mux, .. } = &admission {
        mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
    }
    state.pending_runtime.admission.host_compatibility.insert(
        client_id.clone(),
        HostCompatibilityRecord {
            required_features: host_required_features,
        },
    );
    state
        .pending_runtime
        .admission
        .unix_admissions
        .insert(client_id, admission);
    let _ = reply_tx.send(());
    false
}

pub(crate) fn register_webrtc_admission(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    admission: WebrtcTerminalAdmission,
    host_required_features: Vec<String>,
) -> bool {
    if daemon.local_webrtc().has_live_peer(&grant_id) {
        if let WebrtcTerminalAdmission::Admitted { mux, .. } = &admission {
            mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
        }
        state.pending_runtime.admission.host_compatibility.insert(
            grant_id.clone(),
            HostCompatibilityRecord {
                required_features: host_required_features,
            },
        );
        state
            .pending_runtime
            .admission
            .webrtc_admissions
            .insert(grant_id, admission);
    }
    false
}
