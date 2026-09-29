//! Unix framing and mux scheduling for host-control protocol 13.
//!
//! Every frame is one length-prefixed container. Control frames are UTF-8
//! JSON [`ServerFrame`] payloads. Terminal frames are written as two slices,
//! the stack container header and the shared `TerminalBody`, through one
//! vectored write; the body is never copied by Hub.
use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};

use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{
    ClientFrame, DaemonHello, DaemonProtocolErrorCode, DaemonRequest, DaemonResponse,
    DaemonUnixFrame, DaemonUnixTerminalFrame, MAX_CONTROL_REQUEST_BYTES, MAX_UNIX_FRAME_BYTES,
    ServerFrame, UNIX_FRAME_LENGTH_PREFIX_BYTES, decode_unix_frame, encode_control_json,
    encode_server_frame,
};
use botster_terminal_protocol::MAX_TERMINAL_INPUT_FRAME_BYTES;

use crate::admission::budgets::{DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_INCOMPLETE_FRAME_TIMEOUT};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::transport::unix::UnixConnectionMux;

/// Maximum serialized response storage retained by one Unix connection.
pub(crate) const PENDING_RESPONSE_BYTE_CAPACITY: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct MuxWriteState {
    pending: Option<PendingMuxFrame>,
    queued_control: VecDeque<PendingMuxFrame>,
    queued_events: VecDeque<PendingMuxFrame>,
    // Dropping this connection state drops all frames and this local charge.
    pending_response_bytes: usize,
    last_host_class: Option<crate::transport::unix::host_write_order::HostControlClass>,
}

impl MuxWriteState {
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.is_some() || !self.queued_control.is_empty() || !self.queued_events.is_empty()
    }

    pub(crate) fn has_close_after_pending(&self) -> bool {
        self.pending.as_ref().is_some_and(|frame| frame.close_after)
            || self.queued_control.iter().any(|frame| frame.close_after)
    }

    pub(crate) fn has_pending_response(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|frame| frame.class == PendingMuxClass::Response)
            || !self.queued_control.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn pending_response_count(&self) -> usize {
        let pending =
            self.pending
                .as_ref()
                .is_some_and(|frame| frame.class == PendingMuxClass::Response) as usize;
        pending + self.queued_control.len()
    }

    #[cfg(test)]
    pub(crate) fn pending_response_bytes(&self) -> usize {
        self.pending_response_bytes
    }

    /// Queue one correlated response for `request_id`.
    pub(crate) fn enqueue_response(
        &mut self,
        request_id: &str,
        response: DaemonResponse,
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
    ) -> DaemonTransportResult<()> {
        self.enqueue_response_with_receipt(request_id, response, delivery_ack, close_after, None)
    }

    pub(crate) fn enqueue_response_with_receipt(
        &mut self,
        request_id: &str,
        response: DaemonResponse,
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
        delivery_receipt: Option<crate::runtime::SpawnDeliveryReceipt>,
    ) -> DaemonTransportResult<()> {
        let mut frame = control_mux_frame(
            &ServerFrame::Response {
                request_id: request_id.to_string(),
                response,
            },
            PendingMuxClass::Response,
            delivery_ack,
            close_after,
        )?;
        frame.delivery_receipt = delivery_receipt;
        self.enqueue_response_frame(frame)
    }

    pub(crate) fn enqueue_encoded_response(
        &mut self,
        encoded_frame: &[u8],
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
    ) -> DaemonTransportResult<()> {
        let bytes = encode_control_json(encoded_frame).map_err(DaemonTransportError::from)?;
        self.enqueue_response_frame(PendingMuxFrame {
            bytes: PendingMuxBytes::Control(bytes),
            offset: 0,
            class: PendingMuxClass::Response,
            delivery_ack,
            delivery_receipt: None,
            close_after,
        })
    }

    fn enqueue_response_frame(&mut self, frame: PendingMuxFrame) -> DaemonTransportResult<()> {
        let frame_bytes = frame.bytes.total_len();
        if self.pending_response_bytes.saturating_add(frame_bytes) > PENDING_RESPONSE_BYTE_CAPACITY
        {
            return Err(DaemonTransportError::ResponseBackpressured {
                pending_bytes: self.pending_response_bytes,
                frame_bytes,
                capacity: PENDING_RESPONSE_BYTE_CAPACITY,
            });
        }
        self.pending_response_bytes += frame_bytes;
        self.queued_control.push_back(frame);
        Ok(())
    }

    /// Queue one entity subscription frame on the event lane.
    pub(crate) fn enqueue_entity_frame(
        &mut self,
        entity: crate::entity_delivery::EntityDelivery,
    ) -> DaemonTransportResult<()> {
        let entity = match entity {
            crate::entity_delivery::EntityDelivery::Typed(entity) => entity,
            crate::entity_delivery::EntityDelivery::Encoded(delivery) => {
                self.queued_events.push_back(PendingMuxFrame {
                    bytes: PendingMuxBytes::PreparedEntity(delivery),
                    offset: 0,
                    class: PendingMuxClass::Event,
                    delivery_ack: None,
                    delivery_receipt: None,
                    close_after: false,
                });
                return Ok(());
            }
        };
        self.queued_events.push_back(control_mux_frame(
            &ServerFrame::Entity { entity },
            PendingMuxClass::Event,
            None,
            false,
        )?);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingMuxClass {
    Event,
    Response,
}

pub(crate) enum PendingMuxBytes {
    /// One complete control container, length prefix included.
    Control(Vec<u8>),
    PreparedEntity(crate::entity_delivery::PreparedEntityDelivery),
}

impl PendingMuxBytes {
    fn total_len(&self) -> usize {
        match self {
            Self::Control(bytes) => bytes.len(),
            Self::PreparedEntity(delivery) => delivery.container().len(),
        }
    }

    /// Remaining slices after `offset`, in write order.
    fn remaining(&self, offset: usize) -> ([IoSlice<'_>; 2], usize) {
        match self {
            Self::Control(bytes) => ([IoSlice::new(&bytes[offset..]), IoSlice::new(&[])], 1),
            Self::PreparedEntity(delivery) => (
                [
                    IoSlice::new(&delivery.container()[offset..]),
                    IoSlice::new(&[]),
                ],
                1,
            ),
        }
    }
}

pub(crate) struct PendingMuxFrame {
    bytes: PendingMuxBytes,
    offset: usize,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    delivery_receipt: Option<crate::runtime::SpawnDeliveryReceipt>,
    close_after: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MuxWrite {
    Written,
    Pending,
}

pub(crate) async fn flush_pending_responses(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    mux: &UnixConnectionMux,
    write_state: &mut MuxWriteState,
    started: Instant,
    event_mailbox: Option<&crate::subscription::package_events::ClientEventMailbox>,
) -> DaemonTransportResult<()> {
    loop {
        flush_unix_mux_writes(writer, mux, write_state, event_mailbox).await?;
        if !write_state.has_pending_response() {
            return Ok(());
        }
        if started.elapsed() >= DAEMON_CLIENT_WRITE_TIMEOUT {
            return Err(DaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon client write deadline elapsed",
            )));
        }
    }
}

pub(crate) async fn flush_unix_mux_writes(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    mux: &UnixConnectionMux,
    write_state: &mut MuxWriteState,
    event_mailbox: Option<&crate::subscription::package_events::ClientEventMailbox>,
) -> DaemonTransportResult<()> {
    use crate::transport::unix::host_write_order::{
        HostControlClass, MAX_HOST_FRAMES_PER_FLUSH_TURN, next_ready_host_control_class,
    };

    if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
        return Ok(());
    }
    let mut host_frames = 0;
    loop {
        if host_frames >= MAX_HOST_FRAMES_PER_FLUSH_TURN {
            break;
        }
        let control_ready = !write_state.queued_control.is_empty();
        let event_ready = !write_state.queued_events.is_empty()
            || mux.has_pending_event()
            || event_mailbox.is_some_and(
                crate::subscription::package_events::ClientEventMailbox::has_ready_event,
            );
        match next_ready_host_control_class(write_state.last_host_class, control_ready, event_ready)
        {
            Some(HostControlClass::Control) => {
                let Some(frame) = write_state.queued_control.pop_front() else {
                    break;
                };
                write_state.last_host_class = Some(HostControlClass::Control);
                write_state.pending = Some(frame);
                host_frames += 1;
                if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
                    return Ok(());
                }
            }
            Some(HostControlClass::Event) => {
                let frame = match write_state.queued_events.pop_front() {
                    Some(frame) => Some(frame),
                    None => {
                        let event = mux.pop_pending_event().or_else(|| {
                            event_mailbox.and_then(
                                crate::subscription::package_events::ClientEventMailbox::take_ready_event,
                            )
                        });
                        match event {
                            Some(event) => Some(control_mux_frame(
                                &ServerFrame::Event { event },
                                PendingMuxClass::Event,
                                None,
                                false,
                            )?),
                            None => None,
                        }
                    }
                };
                let Some(frame) = frame else {
                    break;
                };
                write_state.last_host_class = Some(HostControlClass::Event);
                write_state.pending = Some(frame);
                host_frames += 1;
                if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
                    return Ok(());
                }
            }
            None => break,
        }
    }
    Ok(())
}

pub(crate) fn control_mux_frame(
    frame: &ServerFrame,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    close_after: bool,
) -> DaemonTransportResult<PendingMuxFrame> {
    let bytes = encode_server_frame(frame).map_err(DaemonTransportError::from)?;
    Ok(PendingMuxFrame {
        bytes: PendingMuxBytes::Control(bytes),
        offset: 0,
        class,
        delivery_ack,
        delivery_receipt: None,
        close_after,
    })
}

pub(crate) async fn resume_pending_mux_write(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    write_state: &mut MuxWriteState,
) -> DaemonTransportResult<MuxWrite> {
    let Some(pending) = write_state.pending.as_mut() else {
        return Ok(MuxWrite::Written);
    };
    match write_frame_bytes_resumable(writer, pending).await? {
        MuxWrite::Written => {
            let pending = write_state.pending.take().expect("pending mux frame");
            if pending.class == PendingMuxClass::Response {
                write_state.pending_response_bytes = write_state
                    .pending_response_bytes
                    .checked_sub(pending.bytes.total_len())
                    .expect("pending response storage charge");
            }
            if let Some(delivery_ack) = pending.delivery_ack {
                let _ = delivery_ack.send(());
            }
            if let Some(receipt) = pending.delivery_receipt {
                receipt.delivered();
            }
            Ok(MuxWrite::Written)
        }
        MuxWrite::Pending => Ok(MuxWrite::Pending),
    }
}

pub(crate) async fn write_frame_bytes_resumable(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    pending: &mut PendingMuxFrame,
) -> DaemonTransportResult<MuxWrite> {
    let total = pending.bytes.total_len();
    while pending.offset < total {
        match tokio::time::timeout(
            Duration::from_millis(50),
            std::future::poll_fn(|context| {
                let (slices, count) = pending.bytes.remaining(pending.offset);
                std::pin::Pin::new(&mut *writer).poll_write_vectored(context, &slices[..count])
            }),
        )
        .await
        {
            Ok(Ok(0)) => {
                return Err(DaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "unix mux write returned zero bytes",
                )));
            }
            Ok(Ok(written)) => pending.offset += written,
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(MuxWrite::Pending);
            }
            Ok(Err(error)) => return Err(DaemonTransportError::Io(error)),
            Err(_) => return Ok(MuxWrite::Pending),
        }
    }
    Ok(MuxWrite::Written)
}

/// One decoded inbound Unix frame from a client.
#[allow(clippy::large_enum_variant)]
pub(crate) enum UnixInbound {
    Hello(DaemonHello),
    Request {
        request_id: String,
        request: DaemonRequest,
    },
    Terminal(DaemonUnixTerminalFrame),
}

/// Why an inbound frame could not be produced.
#[derive(Debug)]
pub(crate) enum UnixInboundError {
    /// Socket-level failure, including a clean client disconnect.
    Transport(ClientDaemonTransportError),
    /// A framing or correlation violation; the connection closes with this code.
    Protocol(DaemonProtocolErrorCode),
}

/// Read one raw frame (container byte plus payload) without the length prefix.
pub(crate) async fn read_async_raw_frame<R>(
    reader: &mut AsyncBufReader<R>,
    first_byte_timeout: Option<Duration>,
) -> Result<Vec<u8>, UnixInboundError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut prefix = [0_u8; UNIX_FRAME_LENGTH_PREFIX_BYTES];
    let mut first = [0_u8; 1];
    let read_first = reader.read(&mut first);
    let count = if let Some(timeout) = first_byte_timeout {
        tokio::time::timeout(timeout, read_first)
            .await
            .map_err(|_| {
                UnixInboundError::Transport(ClientDaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "daemon handshake deadline elapsed",
                )))
            })?
            .map_err(|error| UnixInboundError::Transport(ClientDaemonTransportError::Io(error)))?
    } else {
        read_first
            .await
            .map_err(|error| UnixInboundError::Transport(ClientDaemonTransportError::Io(error)))?
    };
    if count == 0 {
        return Err(UnixInboundError::Transport(
            ClientDaemonTransportError::ClientDisconnected,
        ));
    }
    prefix[0] = first[0];
    let frame =
        tokio::time::timeout(DAEMON_INCOMPLETE_FRAME_TIMEOUT, async {
            reader.read_exact(&mut prefix[1..]).await.map_err(|error| {
                UnixInboundError::Transport(ClientDaemonTransportError::Io(error))
            })?;
            let declared = u32::from_le_bytes(prefix) as usize;
            if declared == 0 {
                return Err(UnixInboundError::Protocol(
                    DaemonProtocolErrorCode::MalformedFrame,
                ));
            }
            if declared > MAX_UNIX_FRAME_BYTES {
                return Err(UnixInboundError::Protocol(
                    DaemonProtocolErrorCode::FrameTooLarge,
                ));
            }
            let mut frame = vec![0_u8; declared];
            reader.read_exact(&mut frame).await.map_err(|error| {
                UnixInboundError::Transport(ClientDaemonTransportError::Io(error))
            })?;
            Ok(frame)
        })
        .await
        .map_err(|_| {
            UnixInboundError::Transport(ClientDaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon incomplete frame deadline elapsed",
            )))
        })??;
    Ok(frame)
}

/// Read and decode one inbound frame. Bounds are transport bounds only.
pub(crate) async fn read_async_inbound<R>(
    reader: &mut AsyncBufReader<R>,
    first_byte_timeout: Option<Duration>,
) -> Result<UnixInbound, UnixInboundError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let raw = read_async_raw_frame(reader, first_byte_timeout).await?;
    match decode_unix_frame::<ClientFrame>(&raw) {
        Ok(DaemonUnixFrame::Control(ClientFrame::Hello { hello })) => {
            if raw.len() - 1 > MAX_CONTROL_REQUEST_BYTES {
                return Err(UnixInboundError::Protocol(
                    DaemonProtocolErrorCode::FrameTooLarge,
                ));
            }
            Ok(UnixInbound::Hello(hello))
        }
        Ok(DaemonUnixFrame::Control(ClientFrame::Request {
            request_id,
            request,
        })) => {
            if raw.len() - 1 > MAX_CONTROL_REQUEST_BYTES {
                return Err(UnixInboundError::Protocol(
                    DaemonProtocolErrorCode::FrameTooLarge,
                ));
            }
            Ok(UnixInbound::Request {
                request_id,
                request,
            })
        }
        Ok(DaemonUnixFrame::Terminal(frame)) => {
            if frame.body.len() > MAX_TERMINAL_INPUT_FRAME_BYTES {
                return Err(UnixInboundError::Protocol(
                    DaemonProtocolErrorCode::FrameTooLarge,
                ));
            }
            Ok(UnixInbound::Terminal(frame))
        }
        Err(code) => Err(UnixInboundError::Protocol(code)),
    }
}

/// Write one complete server frame with the client write deadline.
pub(crate) async fn write_async_server_frame(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &ServerFrame,
) -> DaemonTransportResult<()> {
    let bytes = encode_server_frame(frame).map_err(DaemonTransportError::from)?;
    tokio::time::timeout(DAEMON_CLIENT_WRITE_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| {
            DaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon client write deadline elapsed",
            ))
        })?
        .map_err(DaemonTransportError::Io)
}

/// Build a host event frame for tests and diagnostics.
#[cfg(test)]
pub(crate) fn event_mux_frame(
    event: botster_hub_client::DaemonEvent,
) -> DaemonTransportResult<PendingMuxFrame> {
    control_mux_frame(
        &ServerFrame::Event { event },
        PendingMuxClass::Event,
        None,
        false,
    )
}

#[cfg(test)]
pub(crate) mod mux_write_resume_tests {
    use super::{
        MuxWrite, MuxWriteState, PENDING_RESPONSE_BYTE_CAPACITY, PendingMuxBytes, PendingMuxClass,
        event_mux_frame, flush_pending_responses, flush_unix_mux_writes,
        write_frame_bytes_resumable,
    };
    use crate::client_api_dto::response::daemon_response_base;
    use crate::transport::unix::UnixConnectionMux;
    use botster_hub_client::{
        DaemonEvent, DaemonResponseKind, DaemonUnixFrameReader, DaemonUnixMuxFrame, ServerFrame,
        TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER, encode_server_frame,
    };
    use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};
    use tokio::io::AsyncWrite;

    #[test]
    fn encoded_response_uses_exact_json_and_the_connection_storage_bound() {
        let mut state = MuxWriteState::default();
        // Whitespace distinguishes these bytes from a second serialization.
        let encoded =
            br#"{ "frame": "response", "request_id": "42", "response": { "kind": "status" } }"#;
        state
            .enqueue_encoded_response(encoded, None, false)
            .expect("queue encoded response");
        let first = state.queued_control.front().expect("queued frame");
        let PendingMuxBytes::Control(bytes) = &first.bytes else {
            panic!("control frame");
        };
        assert_eq!(
            &bytes[botster_hub_client::UNIX_FRAME_LENGTH_PREFIX_BYTES + 1..],
            encoded
        );
        state.pending_response_bytes = PENDING_RESPONSE_BYTE_CAPACITY;
        assert!(matches!(
            state.enqueue_encoded_response(encoded, None, false),
            Err(crate::daemon::error::DaemonTransportError::ResponseBackpressured { .. }),
        ));
        assert_eq!(state.queued_control.len(), 1);
    }

    #[tokio::test]
    async fn response_storage_charge_survives_partial_write_and_releases_after_full_write() {
        let mux = UnixConnectionMux::new();
        let mut state = MuxWriteState::default();
        state
            .enqueue_response(
                "1",
                daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue response");
        let charged = state.pending_response_bytes();
        assert!(charged > 0);
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 1,
            allow_remainder: false,
        };
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("partial response write");
        assert_eq!(state.pending_response_bytes(), charged);
        writer.allow_remainder = true;
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("complete response write");
        assert_eq!(state.pending_response_bytes(), 0);
    }

    #[test]
    fn response_storage_refuses_a_frame_above_remaining_capacity() {
        let mut state = MuxWriteState::default();
        let mut refused = None;
        for index in 0..16 {
            let mut response = daemon_response_base(DaemonResponseKind::PluginMcpToolResult);
            response.plugin_tool_result = serde_json::Value::String("x".repeat(800 * 1024));
            match state.enqueue_response(&index.to_string(), response, None, false) {
                Ok(()) => assert!(state.pending_response_bytes() <= PENDING_RESPONSE_BYTE_CAPACITY),
                Err(error) => {
                    refused = Some(error);
                    break;
                }
            }
        }
        assert!(matches!(
            refused,
            Some(
                crate::daemon::error::DaemonTransportError::ResponseBackpressured {
                    capacity: PENDING_RESPONSE_BYTE_CAPACITY,
                    ..
                }
            )
        ));
        assert!(state.pending_response_bytes() <= PENDING_RESPONSE_BYTE_CAPACITY);
    }

    pub(crate) struct PrefixStallWriter {
        written: Vec<u8>,
        stall_after: usize,
        allow_remainder: bool,
    }

    impl AsyncWrite for PrefixStallWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let room = this.stall_after.saturating_sub(this.written.len());
            if room == 0 && !this.allow_remainder {
                return Poll::Pending;
            }
            let take = if this.allow_remainder {
                buf.len()
            } else {
                room.min(buf.len())
            };
            if take == 0 {
                return Poll::Pending;
            }
            this.written.extend_from_slice(&buf[..take]);
            Poll::Ready(Ok(take))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    pub(crate) fn closed_event() -> DaemonEvent {
        DaemonEvent::TerminalSubscriptionClosed {
            session_id: "session".to_string(),
            subscription_id: "sub".to_string(),
            generation: 2,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER.to_string(),
        }
    }

    pub(crate) fn frame_bytes(event: &DaemonEvent) -> Vec<u8> {
        encode_server_frame(&ServerFrame::Event {
            event: event.clone(),
        })
        .expect("encode event frame")
    }

    pub(crate) fn output_frame(route: &str, marker: &str) -> RoutedTerminalFrame {
        RoutedTerminalFrame::new(
            RouteId::new(route).expect("route"),
            1,
            0,
            encode_output(marker.as_bytes()).expect("output frame"),
        )
    }

    pub(crate) fn response_kind(frame: &DaemonUnixMuxFrame) -> Option<DaemonResponseKind> {
        match frame {
            DaemonUnixMuxFrame::Server(ServerFrame::Response { response, .. }) => {
                Some(response.kind)
            }
            _ => None,
        }
    }

    pub(crate) fn is_package_event(frame: &DaemonUnixMuxFrame) -> bool {
        matches!(
            frame,
            DaemonUnixMuxFrame::Server(ServerFrame::Event {
                event: DaemonEvent::PackageEvent { .. }
            })
        )
    }

    pub(crate) fn closed_event_session(frame: &DaemonUnixMuxFrame) -> Option<&str> {
        match frame {
            DaemonUnixMuxFrame::Server(ServerFrame::Event {
                event: DaemonEvent::TerminalSubscriptionClosed { session_id, .. },
            }) => Some(session_id.as_str()),
            _ => None,
        }
    }

    #[tokio::test]
    pub(crate) async fn resumable_mux_write_keeps_offset_and_emits_one_valid_frame() {
        let event = closed_event();
        let expected = frame_bytes(&event);
        let prefix = 8.min(expected.len() - 1);
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: prefix,
            allow_remainder: false,
        };
        let mut pending = event_mux_frame(event).expect("event frame");

        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        assert_eq!(pending.offset, prefix);
        assert_eq!(writer.written, expected[..prefix]);

        writer.allow_remainder = true;
        let second = write_frame_bytes_resumable(&mut writer, &mut pending)
            .await
            .expect("resume write");
        assert!(matches!(second, MuxWrite::Written));
        assert_eq!(writer.written, expected);
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonUnixMuxFrame::Server(ServerFrame::Event {
                event:
                    DaemonEvent::TerminalSubscriptionClosed {
                        session_id,
                        generation,
                        reason,
                        ..
                    },
            }) => {
                assert_eq!(session_id, "session");
                assert_eq!(*generation, 2);
                assert_eq!(reason, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER);
            }
            other => panic!("expected one close event, got {other:?}"),
        }
    }

    #[tokio::test]
    pub(crate) async fn resumable_mux_write_does_not_start_a_second_frame_while_first_is_pending() {
        let first_bytes = frame_bytes(&closed_event());
        let second_bytes = frame_bytes(&DaemonEvent::TerminalSubscriptionClosed {
            session_id: "other".to_string(),
            subscription_id: "sub-2".to_string(),
            generation: 3,
            reason: TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER.to_string(),
        });
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 4,
            allow_remainder: false,
        };
        let mut pending = event_mux_frame(closed_event()).expect("event frame");
        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        assert_eq!(writer.written, first_bytes[..4]);
        assert_ne!(writer.written, [first_bytes.clone(), second_bytes].concat());
        match &pending.bytes {
            PendingMuxBytes::Control(bytes) => assert_eq!(bytes, &first_bytes),
            PendingMuxBytes::PreparedEntity(_) => {
                panic!("event frames are control containers")
            }
        }
    }

    pub(crate) fn parse_written_mux_frames(written: &[u8]) -> Vec<DaemonUnixMuxFrame> {
        let mut cursor = std::io::Cursor::new(written);
        let mut reader = DaemonUnixFrameReader::new();
        let mut frames = Vec::new();
        loop {
            match reader.read_frame(&mut cursor) {
                Ok(frame) => frames.push(frame),
                Err(botster_hub_client::DaemonTransportError::ClientDisconnected) => break,
                Err(error) => panic!("written bytes must decode as complete frames: {error}"),
            }
        }
        assert!(
            !reader.has_partial_frame(),
            "written bytes must not end inside a frame"
        );
        frames
    }

    #[tokio::test]
    pub(crate) async fn partial_package_event_resumes_without_interleaving() {
        let mux = UnixConnectionMux::new();
        let mailbox = crate::subscription::package_events::ClientEventMailbox::new(
            crate::config::PackageEventPlanePolicy::default(),
        );
        mailbox
            .try_push(
                "sub",
                "owner",
                "ready",
                serde_json::json!({ "ok": true }),
                8,
            )
            .expect("admit event");
        let serialized = frame_bytes(&botster_hub_client::DaemonEvent::PackageEvent {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            payload: serde_json::json!({ "ok": true }),
        })
        .len();
        let prefix = 8.min(serialized.saturating_sub(1));
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: prefix,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("partial event write");
        assert!(write_state.pending.is_some());
        assert!(write_state.pending.as_ref().is_some_and(|pending| {
            pending.class == PendingMuxClass::Event && pending.offset == prefix
        }));

        write_state
            .enqueue_response(
                "1",
                daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("resume event then status");
        assert!(!write_state.has_pending());
        let frames = parse_written_mux_frames(&writer.written);
        assert!(
            is_package_event(&frames[0]),
            "partial PackageEvent must finish before a Response: {frames:?}"
        );
        assert_eq!(response_kind(&frames[1]), Some(DaemonResponseKind::Status));
    }

    #[tokio::test]
    pub(crate) async fn one_flush_turn_writes_status_without_draining_the_event_flood() {
        let mux = UnixConnectionMux::new();
        let mailbox = crate::subscription::package_events::ClientEventMailbox::new(
            crate::config::PackageEventPlanePolicy {
                consumer_queue_max_events: 8,
                ..crate::config::PackageEventPlanePolicy::default()
            },
        );
        for index in 0..8 {
            mailbox
                .try_push(
                    "sub",
                    "owner",
                    "ready",
                    serde_json::json!({ "ok": true, "n": index }),
                    8,
                )
                .expect("admit event");
        }
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        write_state
            .enqueue_response(
                "1",
                daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("bounded turn");
        let frames = parse_written_mux_frames(&writer.written);
        assert!(
            frames.len()
                <= crate::transport::unix::host_write_order::MAX_HOST_FRAMES_PER_FLUSH_TURN,
            "one flush turn must not drain the flood: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .any(|frame| response_kind(frame) == Some(DaemonResponseKind::Status)),
            "Status must progress after event draining starts: {frames:?}"
        );
        assert!(
            frames.iter().any(is_package_event),
            "an event frame must also progress: {frames:?}"
        );
        assert!(
            mailbox.has_ready_event(),
            "remaining events stay queued across turns"
        );
    }

    #[tokio::test]
    pub(crate) async fn entity_frames_share_the_event_lane() {
        let mux = UnixConnectionMux::new();
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        write_state
            .enqueue_entity_frame(
                botster_hub_client::DaemonEntityFrame::Remove {
                    subscription_id: "entities".to_string(),
                    entity_type: "session".to_string(),
                    snapshot_seq: 4,
                    id: "session".to_string(),
                }
                .into(),
            )
            .expect("enqueue entity frame");
        write_state
            .enqueue_response(
                "7",
                daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("flush");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DaemonUnixMuxFrame::Server(ServerFrame::Entity {
                entity: botster_hub_client::DaemonEntityFrame::Remove { .. }
            })
        )));
        assert!(
            frames
                .iter()
                .any(|frame| response_kind(frame) == Some(DaemonResponseKind::Status))
        );
    }

    #[tokio::test]
    pub(crate) async fn stalled_response_stays_bounded() {
        let mux = UnixConnectionMux::new();
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        write_state
            .queued_events
            .push_back(event_mux_frame(closed_event()).expect("event frame"));
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("partial event");
        write_state
            .enqueue_response(
                "3",
                daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("response remains pending");
        assert!(write_state.has_pending_response());
        assert_eq!(write_state.pending_response_count(), 1);
        let timed_out = flush_pending_responses(
            &mut writer,
            &mux,
            &mut write_state,
            Instant::now() - Duration::from_secs(3),
            None,
        )
        .await;
        assert!(
            timed_out.is_err(),
            "a stalled Response must not return until Written or timeout"
        );
        assert_eq!(write_state.pending_response_count(), 1);
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_pending_responses(&mut writer, &mux, &mut write_state, Instant::now(), None)
            .await
            .expect("finish the one pending Response");
        assert!(!write_state.has_pending_response());
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 2);
        assert!(closed_event_session(&frames[0]).is_some());
        assert_eq!(response_kind(&frames[1]), Some(DaemonResponseKind::Status));
        assert!(!write_state.has_pending());
    }
}
