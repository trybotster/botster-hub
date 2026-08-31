//! Control vocabulary imported by Unix and WebRTC transports.

use std::sync::mpsc;
use std::time::Instant;

use botster_hub_client::{DaemonHubUpdate, DaemonRequest, DaemonResponse};
use tokio::net::UnixStream as TokioUnixStream;
use tokio::sync::{OwnedSemaphorePermit, mpsc as tokio_mpsc, oneshot};

use crate::admission::unix_hello::{UnixTerminalAdmission, WebrtcTerminalAdmission};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::{LocalWebrtcAttachedSubscription, LocalWebrtcSenderTerminalRecord};

pub(crate) type ControlSender = tokio_mpsc::Sender<ControlMessage>;
pub(crate) type ControlReplySender = oneshot::Sender<DaemonTransportResult<DaemonResponse>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EgressWriteClass {
    Timeout,
    Other,
}

pub(crate) fn egress_write_class(error: &DaemonTransportError) -> EgressWriteClass {
    match error {
        DaemonTransportError::Io(io) if io.kind() == std::io::ErrorKind::TimedOut => {
            EgressWriteClass::Timeout
        }
        _ => EgressWriteClass::Other,
    }
}

#[derive(Debug)]
pub(crate) enum ControlMessage {
    DataPlaneProgress,
    AcceptedConnection {
        stream: TokioUnixStream,
        admission_permit: OwnedSemaphorePermit,
    },
    RejectedConnection,
    SubscribeEntities {
        entity_type: String,
        subscription_id: String,
        frame_tx: EntityFrameSender,
        reply_tx: ControlReplySender,
        /// When set, admission requires a still-live local WebRTC peer for this grant.
        /// Socket-path subscriptions leave this `None`.
        grant_id: Option<String>,
    },
    UnsubscribeEntities {
        subscription_id: String,
        reply_tx: Option<ControlReplySender>,
        /// Same live-peer guard as `SubscribeEntities` when the request originated on WebRTC.
        grant_id: Option<String>,
    },
    Request {
        request: Box<DaemonRequest>,
        reply_tx: ControlReplySender,
        response_delivery_rx: Option<mpsc::Receiver<()>>,
        /// When set, admission requires a still-live local WebRTC peer for this grant.
        /// Socket-path and signal-handler requests leave this `None`.
        grant_id: Option<String>,
        /// Stable Core client identity for one transport connection.
        client_id: Option<String>,
        enqueued_at: Instant,
    },
    HubUpdateCheckCompleted {
        update: DaemonHubUpdate,
    },
    EgressWriteFailed {
        delivery_kind: DaemonDeliveryKind,
        write_class: EgressWriteClass,
    },
    LocalWebrtcPeerClosed {
        grant_id: String,
        attached_subscriptions: Vec<LocalWebrtcAttachedSubscription>,
        entity_subscription_ids: Vec<String>,
        terminal_record: LocalWebrtcSenderTerminalRecord,
    },
    RegisterUnixAdmission {
        client_id: String,
        admission: UnixTerminalAdmission,
        reply_tx: oneshot::Sender<()>,
        host_required_features: Vec<String>,
    },
    RegisterWebrtcAdmission {
        grant_id: String,
        admission: WebrtcTerminalAdmission,
        host_required_features: Vec<String>,
    },
    InspectTerminalReservation {
        grant_id: String,
        label: String,
        reply_tx: oneshot::Sender<ReservationInspectReply>,
    },
    BindReservedTerminal {
        grant_id: String,
        label: String,
        reply_tx: oneshot::Sender<
            Result<crate::transport::webrtc::WebRtcTerminalAdapterHandle, BindReservedError>,
        >,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReservationInspectReply {
    Unknown,
    Expired {
        session_id: String,
        subscription_id: String,
        generation: u64,
    },
    Bound,
    Live {
        session_id: String,
        subscription_id: String,
        generation: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindReservedError {
    Unknown,
    Expired,
    Bound,
    BindFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DaemonDeliveryKind {
    Terminal,
    Control,
}

impl DaemonDeliveryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::Control => "control",
        }
    }
}

pub(crate) fn daemon_delivery_kind(_response: &DaemonResponse) -> DaemonDeliveryKind {
    DaemonDeliveryKind::Control
}
