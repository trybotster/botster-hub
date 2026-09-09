//! Control vocabulary imported by Unix and WebRTC transports.

use std::sync::mpsc;
use std::time::Instant;

use botster_hub_client::{DaemonHubUpdate, DaemonRequest, DaemonResponse};
use tokio::net::UnixStream as TokioUnixStream;
use tokio::sync::{OwnedSemaphorePermit, mpsc as tokio_mpsc, oneshot};

use crate::admission::connection_budget::ChannelClass;
use crate::admission::unix_hello::{UnixTerminalAdmission, WebrtcTerminalAdmission};
use crate::daemon::control::reply::{ControlReply, RetainedPluginResult};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::subscription::entity::EntityFrameSender;
use crate::transport::webrtc::{LocalWebrtcAttachedSubscription, LocalWebrtcSenderTerminalRecord};

pub(crate) type ControlSender = tokio_mpsc::Sender<ControlMessage>;
pub(crate) type ControlReplyReceiver = oneshot::Receiver<ControlReply>;

#[derive(Debug)]
pub(crate) struct ControlReplySender(Option<oneshot::Sender<ControlReply>>);

impl ControlReplySender {
    pub(crate) fn send(
        self,
        response: DaemonTransportResult<DaemonResponse>,
    ) -> Result<(), ControlReply> {
        self.send_reply(ControlReply::plain(response))
    }

    pub(crate) fn send_retained(
        self,
        response: RetainedPluginResult<DaemonTransportResult<DaemonResponse>>,
    ) -> Result<(), ControlReply> {
        self.send_reply(ControlReply::retained(response))
    }

    pub(crate) fn send_reply(self, response: ControlReply) -> Result<(), ControlReply> {
        match self.0 {
            Some(sender) => sender.send(response),
            None => Err(response),
        }
    }

    /// Transfer delivery while the owner keeps the request row for cleanup.
    pub(crate) fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.as_ref().is_none_or(oneshot::Sender::is_closed)
    }
}

pub(crate) fn control_reply_channel() -> (ControlReplySender, ControlReplyReceiver) {
    let (sender, receiver) = oneshot::channel();
    (ControlReplySender(Some(sender)), receiver)
}

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
    /// The data-plane thread recorded one or more coalesced progress facts.
    /// The shared latch owns the facts. This message is only a doorbell.
    DataPlaneProgress,
    /// The data-plane thread published one or more keyed Core results.
    CoreCompletionPublished,
    AcceptedConnection {
        stream: TokioUnixStream,
        admission_permit: OwnedSemaphorePermit,
        cleanup_permit: tokio_mpsc::OwnedPermit<ControlMessage>,
    },
    ConnectionCleanup(crate::transport::unix::connection::ConnectionCleanup),
    RejectedConnection,
    SubscribeEntities {
        entity_type: String,
        subscription_id: String,
        /// Correlation id from the transport frame. Internal requests leave this unset.
        transport_request_id: Option<String>,
        /// Stable identity for one transport connection generation.
        client_id: Option<String>,
        frame_tx: EntityFrameSender,
        frame_rx: Option<tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>>,
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
        /// Correlation id from the transport frame. Internal requests leave this unset.
        transport_request_id: Option<String>,
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
    /// A transport dropped retained plugin-result storage and released byte capacity.
    PluginResultCapacityReleased,
    /// Core published one or more plugin completions for the owner to drain.
    PluginCompletionPublished,
    /// A host job completed or released bounded executor capacity.
    HostProgressPublished,
    /// The causal table published queue capacity or a mutex release.
    CausalProgressPublished,
    /// A Lua worker queued one managed worktree and session spawn operation.
    ManagedSessionSpawnQueued,
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
    InspectReservation {
        grant_id: String,
        label: String,
        reply_tx: oneshot::Sender<ReservationInspectReply>,
    },
    BindReservedSubscription {
        grant_id: String,
        label: String,
        reply_tx: oneshot::Sender<Result<BoundSubscription, BindReservedError>>,
    },
    RetireReservedSubscription {
        grant_id: String,
        label: String,
    },
    AuthorizeSubscriptionSend {
        grant_id: String,
        label: String,
        frame_len: usize,
        reply_tx: oneshot::Sender<Option<crate::admission::connection_budget::AggregateSendPermit>>,
    },
    AuthorizeSubscriptionHelloAck {
        grant_id: String,
        label: String,
        frame_len: usize,
        reply_tx: oneshot::Sender<Option<crate::admission::connection_budget::AggregateSendPermit>>,
    },
}

#[derive(Debug)]
pub(crate) enum BoundSubscription {
    Terminal {
        handle: crate::transport::webrtc::WebRtcTerminalAdapterHandle,
        usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    },
    Entity {
        receiver: tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>,
        usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    },
    Event {
        mailbox: std::sync::Arc<crate::subscription::package_events::ClientEventMailbox>,
        usage: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReservationInspectReply {
    Unknown,
    Stale,
    OverLimit,
    Expired {
        session_id: String,
        subscription_id: String,
        generation: u64,
    },
    Bound,
    Live {
        class: ChannelClass,
        session_id: String,
        subscription_id: String,
        generation: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindReservedError {
    Unknown,
    Stale,
    OverLimit,
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
