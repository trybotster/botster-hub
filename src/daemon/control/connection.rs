//! Unix and WebRTC admission registration.

use std::sync::Arc;

use tokio::sync::oneshot;

use botster_hub_client::{DaemonEvent, TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED};

use crate::HubDaemon;
use crate::admission::reservations::{ReservationLookup, now_seconds};
use crate::admission::unix_hello::{
    HostCompatibilityRecord, UnixTerminalAdmission, WebrtcTerminalAdmission,
};
use crate::daemon::control::message::{BindReservedError, ControlMessage, ReservationInspectReply};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_loop::tick;
use crate::subscription::attach_routes::{WebrtcBindRequest, bind_webrtc_adapter_after_attaching};

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    message: ControlMessage,
) -> bool {
    match message {
        ControlMessage::AcceptedConnection { .. } | ControlMessage::RejectedConnection => false,
        ControlMessage::RegisterUnixAdmission {
            client_id,
            admission,
            reply_tx,
            host_required_features,
        } => register_unix_admission(
            state,
            client_id,
            admission,
            reply_tx,
            host_required_features,
        ),
        ControlMessage::RegisterWebrtcAdmission {
            grant_id,
            admission,
            host_required_features,
        } => register_webrtc_admission(daemon, state, grant_id, admission, host_required_features),
        ControlMessage::InspectTerminalReservation {
            grant_id,
            label,
            reply_tx,
        } => inspect_terminal_reservation(daemon, state, grant_id, label, reply_tx),
        ControlMessage::BindReservedTerminal {
            grant_id,
            label,
            reply_tx,
        } => bind_reserved_terminal(daemon, state, grant_id, label, reply_tx),
        _ => unreachable!("connection family received a non-connection control message"),
    }
}

fn register_unix_admission(
    state: &mut DaemonControlState,
    client_id: String,
    admission: UnixTerminalAdmission,
    reply_tx: oneshot::Sender<()>,
    host_required_features: Vec<String>,
) -> bool {
    if let UnixTerminalAdmission::Admitted { mux, .. } = &admission {
        mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
        mux.bind_close_source(state.pending_runtime.close_source.clone());
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

fn register_webrtc_admission(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    mut admission: WebrtcTerminalAdmission,
    host_required_features: Vec<String>,
) -> bool {
    if daemon.local_webrtc().has_live_peer(&grant_id) {
        if let WebrtcTerminalAdmission::Admitted { mux, .. } = &admission {
            mux.bind_close_work(Arc::clone(&state.pending_runtime.close_work));
            mux.bind_close_source(state.pending_runtime.close_source.clone());
        }
        state.pending_runtime.admission.host_compatibility.insert(
            grant_id.clone(),
            HostCompatibilityRecord {
                required_features: host_required_features,
            },
        );
        if let WebrtcTerminalAdmission::Admitted {
            peer_generation, ..
        } = &mut admission
        {
            state.pending_runtime.admission.next_peer_generation = state
                .pending_runtime
                .admission
                .next_peer_generation
                .saturating_add(1);
            *peer_generation = state.pending_runtime.admission.next_peer_generation;
        }
        state
            .pending_runtime
            .admission
            .webrtc_admissions
            .insert(grant_id, admission);
    }
    false
}

fn inspect_terminal_reservation(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    label: String,
    reply_tx: oneshot::Sender<ReservationInspectReply>,
) -> bool {
    let _ = daemon;
    let now = now_seconds();
    let lookup = state
        .pending_runtime
        .admission
        .reservations
        .lookup_label(&label, now);
    let reply = match lookup {
        ReservationLookup::Unknown => ReservationInspectReply::Unknown,
        ReservationLookup::Bound => ReservationInspectReply::Bound,
        ReservationLookup::Expired => {
            emit_reservation_expired(state, &grant_id, &label, now);
            match state
                .pending_runtime
                .admission
                .reservations
                .reservation_for_label(&label)
            {
                Some(reservation) => ReservationInspectReply::Expired {
                    session_id: reservation.session_id.clone(),
                    subscription_id: reservation.subscription_id.clone(),
                    generation: reservation.generation,
                },
                None => ReservationInspectReply::Unknown,
            }
        }
        ReservationLookup::Live => match state
            .pending_runtime
            .admission
            .reservations
            .reservation_for_label(&label)
        {
            Some(reservation) => ReservationInspectReply::Live {
                session_id: reservation.session_id.clone(),
                subscription_id: reservation.subscription_id.clone(),
                generation: reservation.generation,
            },
            None => ReservationInspectReply::Unknown,
        },
    };
    let _ = reply_tx.send(reply);
    false
}

fn bind_reserved_terminal(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    grant_id: String,
    label: String,
    reply_tx: oneshot::Sender<
        Result<crate::transport::webrtc::WebRtcTerminalAdapterHandle, BindReservedError>,
    >,
) -> bool {
    let now = now_seconds();
    match state
        .pending_runtime
        .admission
        .reservations
        .lookup_label(&label, now)
    {
        ReservationLookup::Unknown => {
            let _ = reply_tx.send(Err(BindReservedError::Unknown));
            return false;
        }
        ReservationLookup::Bound => {
            let _ = reply_tx.send(Err(BindReservedError::Bound));
            return false;
        }
        ReservationLookup::Expired => {
            emit_reservation_expired(state, &grant_id, &label, now);
            let _ = reply_tx.send(Err(BindReservedError::Expired));
            return false;
        }
        ReservationLookup::Live => {}
    }
    let Some(reservation) = state
        .pending_runtime
        .admission
        .reservations
        .reservation_for_label(&label)
        .cloned()
    else {
        let _ = reply_tx.send(Err(BindReservedError::Unknown));
        return false;
    };
    let Some(WebrtcTerminalAdmission::Admitted {
        required_features,
        mux,
        terminal_requirement,
        ..
    }) = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(&grant_id)
        .cloned()
    else {
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let Some(runtime) = daemon.runtime_mut() else {
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let Some(client_id) = state
        .pending_runtime
        .stream_owner_client_id(&reservation.session_id, &reservation.subscription_id)
    else {
        let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        return false;
    };
    let bind_now = tick(&mut state.logical_clock);
    let result = bind_webrtc_adapter_after_attaching(
        &mut state.pending_runtime,
        runtime,
        WebrtcBindRequest {
            client_id: &client_id,
            session_id: &reservation.session_id,
            subscription_id: &reservation.subscription_id,
            required_features: &required_features,
            terminal_requirement: terminal_requirement.as_ref(),
            now_seconds: bind_now,
            mux: Some(&mux),
        },
    );
    match result {
        Ok(Some(handle)) => {
            let _ = state
                .pending_runtime
                .admission
                .reservations
                .mark_bound(&label);
            let _ = reply_tx.send(Ok(handle));
        }
        Ok(None) | Err(()) => {
            let _ = reply_tx.send(Err(BindReservedError::BindFailed));
        }
    }
    false
}

fn emit_reservation_expired(state: &mut DaemonControlState, grant_id: &str, label: &str, now: u64) {
    let Some(reservation) = state
        .pending_runtime
        .admission
        .reservations
        .expire_label(label, now)
    else {
        return;
    };
    if let Some(WebrtcTerminalAdmission::Admitted { mux, .. }) = state
        .pending_runtime
        .admission
        .webrtc_admissions
        .get(grant_id)
    {
        mux.push_host_event(DaemonEvent::TerminalSubscriptionClosed {
            session_id: reservation.session_id,
            subscription_id: reservation.subscription_id,
            generation: reservation.generation,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_RESERVATION_EXPIRED.to_string(),
        });
    }
}
