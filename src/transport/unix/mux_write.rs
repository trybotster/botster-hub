//! Unix framing and mux scheduling for host-control protocol 9.
//!
//! Every frame is one length-prefixed container. Control frames are UTF-8
//! JSON [`ServerFrame`] payloads. Terminal frames are written as two slices,
//! the stack container header and the shared `TerminalBody`, through one
//! vectored write; the body is never copied by Hub.
use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};

use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{
    ClientFrame, DaemonEntityFrame, DaemonEvent, DaemonHello, DaemonProtocolErrorCode,
    DaemonRequest, DaemonResponse, DaemonUnixFrame, DaemonUnixTerminalFrame,
    MAX_CONTROL_REQUEST_BYTES, MAX_UNIX_FRAME_BYTES, ServerFrame, UNIX_FRAME_LENGTH_PREFIX_BYTES,
    UnixTerminalContainerHeader, decode_unix_frame, encode_server_frame,
};
use botster_terminal_protocol::MAX_TERMINAL_INPUT_FRAME_BYTES;

use crate::admission::budgets::{DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_INCOMPLETE_FRAME_TIMEOUT};
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapterHandle};

#[derive(Default)]
pub(crate) struct MuxWriteState {
    pending: Option<PendingMuxFrame>,
    queued_control: VecDeque<PendingMuxFrame>,
    queued_events: VecDeque<PendingMuxFrame>,
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

    pub(crate) fn pending_response_count(&self) -> usize {
        let pending =
            self.pending
                .as_ref()
                .is_some_and(|frame| frame.class == PendingMuxClass::Response) as usize;
        pending + self.queued_control.len()
    }

    /// Queue one correlated response for `request_id`.
    pub(crate) fn enqueue_response(
        &mut self,
        request_id: &str,
        response: &DaemonResponse,
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
    ) -> DaemonTransportResult<()> {
        self.queued_control.push_back(control_mux_frame(
            &ServerFrame::Response {
                request_id: request_id.to_string(),
                response: response.clone(),
            },
            PendingMuxClass::Response,
            delivery_ack,
            close_after,
        )?);
        Ok(())
    }

    /// Queue one non-response control frame such as a typed close.
    pub(crate) fn enqueue_server_frame(
        &mut self,
        frame: &ServerFrame,
    ) -> DaemonTransportResult<()> {
        self.queued_control.push_back(control_mux_frame(
            frame,
            PendingMuxClass::Response,
            None,
            false,
        )?);
        Ok(())
    }

    /// Queue one entity subscription frame on the event lane.
    pub(crate) fn enqueue_entity_frame(
        &mut self,
        entity: DaemonEntityFrame,
    ) -> DaemonTransportResult<()> {
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
    Terminal,
    Event,
    Response,
}

pub(crate) enum PendingMuxBytes {
    /// One complete control container, length prefix included.
    Control(Vec<u8>),
    /// One terminal container: stack header plus the shared body.
    Terminal {
        header: UnixTerminalContainerHeader,
        body: Arc<[u8]>,
    },
}

impl PendingMuxBytes {
    fn total_len(&self) -> usize {
        match self {
            Self::Control(bytes) => bytes.len(),
            Self::Terminal { header, body } => header.as_bytes().len() + body.len(),
        }
    }

    /// Remaining slices after `offset`, in write order.
    fn remaining(&self, offset: usize) -> ([IoSlice<'_>; 2], usize) {
        match self {
            Self::Control(bytes) => ([IoSlice::new(&bytes[offset..]), IoSlice::new(&[])], 1),
            Self::Terminal { header, body } => {
                let header = header.as_bytes();
                if offset < header.len() {
                    ([IoSlice::new(&header[offset..]), IoSlice::new(body)], 2)
                } else {
                    (
                        [
                            IoSlice::new(&body[offset - header.len()..]),
                            IoSlice::new(&[]),
                        ],
                        1,
                    )
                }
            }
        }
    }
}

pub(crate) struct PendingMuxFrame {
    bytes: PendingMuxBytes,
    offset: usize,
    complete_envelope: Option<UnixTerminalAdapterHandle>,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
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

    abandon_zero_offset_terminal_for_response(write_state);
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
    let mut terminal_passes = 0;
    loop {
        let writes = mux.snapshot_writes();
        if writes.is_empty() {
            break;
        }
        terminal_passes += 1;
        if terminal_passes > 16 {
            break;
        }
        for (handle, frame) in writes {
            let Some(header) = UnixTerminalContainerHeader::new(
                frame.route.as_str(),
                frame.generation,
                frame.stream_epoch,
                frame.frame.len(),
            ) else {
                // A Core-validated route and body always fit; a frame that does
                // not is a contract violation and ends that route only.
                handle.close();
                continue;
            };
            write_state.pending = Some(PendingMuxFrame {
                bytes: PendingMuxBytes::Terminal {
                    header,
                    body: Arc::clone(frame.frame.shared_bytes()),
                },
                offset: 0,
                complete_envelope: Some(handle),
                class: PendingMuxClass::Terminal,
                delivery_ack: None,
                close_after: false,
            });
            if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
                return Ok(());
            }
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
        complete_envelope: None,
        class,
        delivery_ack,
        close_after,
    })
}

pub(crate) fn abandon_zero_offset_terminal_for_response(write_state: &mut MuxWriteState) {
    let should_abandon = write_state.pending.as_ref().is_some_and(|pending| {
        pending.class == PendingMuxClass::Terminal
            && pending.offset == 0
            && !write_state.queued_control.is_empty()
    });
    if should_abandon {
        abandon_pending_terminal(write_state);
    }
}

pub(crate) fn abandon_pending_terminal(write_state: &mut MuxWriteState) {
    if let Some(pending) = write_state.pending.take()
        && let Some(handle) = pending.complete_envelope
    {
        handle.defer_flush();
    }
}

pub(crate) async fn resume_pending_mux_write(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    write_state: &mut MuxWriteState,
) -> DaemonTransportResult<MuxWrite> {
    let Some(pending) = write_state.pending.as_mut() else {
        return Ok(MuxWrite::Written);
    };
    if pending.offset == 0
        && pending
            .complete_envelope
            .as_ref()
            .is_some_and(|handle| handle.is_closed())
    {
        write_state.pending.take();
        return Ok(MuxWrite::Written);
    }
    match write_frame_bytes_resumable(writer, pending).await? {
        MuxWrite::Written => {
            let pending = write_state.pending.take().expect("pending mux frame");
            if let Some(delivery_ack) = pending.delivery_ack {
                let _ = delivery_ack.send(());
            }
            if let Some(handle) = pending.complete_envelope
                && !handle.is_closed()
            {
                let _ = handle.complete_active();
            }
            Ok(MuxWrite::Written)
        }
        MuxWrite::Pending => {
            if pending.class == PendingMuxClass::Terminal && pending.offset == 0 {
                abandon_pending_terminal(write_state);
                return Ok(MuxWrite::Written);
            }
            Ok(MuxWrite::Pending)
        }
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
                if pending.offset == 0
                    && pending
                        .complete_envelope
                        .as_ref()
                        .is_some_and(|handle| handle.is_closed())
                {
                    return std::task::Poll::Ready(None);
                }
                let (slices, count) = pending.bytes.remaining(pending.offset);
                std::pin::Pin::new(&mut *writer)
                    .poll_write_vectored(context, &slices[..count])
                    .map(Some)
            }),
        )
        .await
        {
            Ok(None) => return Ok(MuxWrite::Written),
            Ok(Some(Ok(0))) => {
                return Err(DaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "unix mux write returned zero bytes",
                )));
            }
            Ok(Some(Ok(written))) => pending.offset += written,
            Ok(Some(Err(error))) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(MuxWrite::Pending);
            }
            Ok(Some(Err(error))) => return Err(DaemonTransportError::Io(error)),
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
pub(crate) fn event_mux_frame(event: DaemonEvent) -> DaemonTransportResult<PendingMuxFrame> {
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
        MuxWrite, MuxWriteState, PendingMuxBytes, PendingMuxClass, PendingMuxFrame,
        event_mux_frame, flush_pending_responses, flush_unix_mux_writes, resume_pending_mux_write,
        write_frame_bytes_resumable,
    };
    use crate::client_api_dto::response::daemon_response_base;
    use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapter};
    use botster_core::contract::terminal_adapter::{TerminalAdapter, TerminalAdapterPressure};
    use botster_hub_client::{
        DaemonEvent, DaemonResponseKind, DaemonUnixFrameReader, DaemonUnixMuxFrame, ServerFrame,
        TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER, UnixTerminalContainerHeader,
        encode_server_frame,
    };
    use botster_terminal_protocol::{
        RouteId, RoutedTerminalFrame, encode_output, encode_process_exit,
    };
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};
    use tokio::io::AsyncWrite;

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

    pub(crate) fn terminal_route(frame: &DaemonUnixMuxFrame) -> Option<&str> {
        match frame {
            DaemonUnixMuxFrame::Terminal(frame) => Some(frame.route.as_str()),
            DaemonUnixMuxFrame::Server(_) => None,
        }
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
            PendingMuxBytes::Terminal { .. } => panic!("event frames are control containers"),
        }
    }

    #[tokio::test]
    pub(crate) async fn terminal_container_is_written_as_header_then_shared_body() {
        let frame = output_frame("sub", "vectored");
        let header =
            UnixTerminalContainerHeader::new("sub", 1, 0, frame.frame.len()).expect("header");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: header.as_bytes().len() + 3,
            allow_remainder: false,
        };
        let mut pending = PendingMuxFrame {
            bytes: PendingMuxBytes::Terminal {
                header,
                body: Arc::clone(frame.frame.shared_bytes()),
            },
            offset: 0,
            complete_envelope: None,
            class: PendingMuxClass::Terminal,
            delivery_ack: None,
            close_after: false,
        };
        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        writer.allow_remainder = true;
        write_frame_bytes_resumable(&mut writer, &mut pending)
            .await
            .expect("resume across the body slice");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonUnixMuxFrame::Terminal(decoded) => {
                assert_eq!(decoded.route, "sub");
                assert_eq!(decoded.generation, 1);
                assert_eq!(decoded.body, frame.frame.as_bytes());
            }
            other => panic!("expected a terminal container, got {other:?}"),
        }
    }

    pub(crate) fn occupy_route(
        mux: &UnixConnectionMux,
        session_id: &str,
        subscription_id: &str,
        marker: &str,
    ) -> UnixTerminalAdapter {
        let (mut adapter, handle) = mux.create_adapter();
        mux.register(
            session_id.to_string(),
            subscription_id.to_string(),
            1,
            handle,
        );
        assert_eq!(
            adapter.try_write(&output_frame(subscription_id, marker)),
            Ok(())
        );
        adapter
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
    pub(crate) async fn abandoned_zero_progress_terminal_retries_the_original_frame() {
        let mux = UnixConnectionMux::new();
        let stall = occupy_route(&mux, "stall", "sub", "flood");
        assert_eq!(mux.snapshot_writes().len(), 1);

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 0,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("zero-progress terminal start is abandoned");
        assert_eq!(
            stall.pressure(),
            TerminalAdapterPressure::Full,
            "abandon must keep the original adapter frame"
        );
        assert!(
            mux.snapshot_writes().is_empty(),
            "deferred flush omits the frame only for this pass"
        );

        mux.clear_deferred_flushes();
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("retry the original deferred frame");
        let frames = parse_written_mux_frames(&writer.written);
        assert!(
            frames
                .iter()
                .any(|frame| terminal_route(frame) == Some("sub")),
            "the original flood frame must still be delivered, frames={frames:?}"
        );
    }

    #[tokio::test]
    pub(crate) async fn partial_terminal_then_response_parses_two_complete_mux_frames() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 8,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("first flush");
        assert!(write_state.pending.is_some());
        assert!(write_state.pending.as_ref().is_some_and(|pending| {
            pending.class == PendingMuxClass::Terminal && pending.offset == 8
        }));

        let response = daemon_response_base(DaemonResponseKind::Status);
        write_state
            .enqueue_response("1", &response, None, false)
            .expect("enqueue status");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("resume flush");
        assert!(!write_state.has_pending());
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 2, "expected two complete mux frames");
        assert_eq!(terminal_route(&frames[0]), Some("sub"));
        assert_eq!(response_kind(&frames[1]), Some(DaemonResponseKind::Status));
        assert!(matches!(
            &frames[1],
            DaemonUnixMuxFrame::Server(ServerFrame::Response { request_id, .. }) if request_id == "1"
        ));
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
                &daemon_response_base(DaemonResponseKind::Status),
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
                &daemon_response_base(DaemonResponseKind::Status),
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
            .enqueue_entity_frame(botster_hub_client::DaemonEntityFrame::Remove {
                subscription_id: "entities".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 4,
                id: "session".to_string(),
            })
            .expect("enqueue entity frame");
        write_state
            .enqueue_response(
                "7",
                &daemon_response_base(DaemonResponseKind::Status),
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
    pub(crate) async fn zero_progress_terminal_start_is_abandoned_without_completing_slot() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 0,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("abandon flush");
        assert!(writer.written.is_empty());
        assert!(write_state.pending.is_none());
        assert!(mux.snapshot_writes().is_empty());
        let _sibling = occupy_route(&mux, "sibling", "sub-live", "live");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("sibling flush");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 1);
        assert_eq!(terminal_route(&frames[0]), Some("sub-live"));
    }

    #[tokio::test]
    pub(crate) async fn hard_close_abandons_terminal_while_host_events_and_sibling_progress() {
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
        let (mut adapter, handle) = mux.create_adapter();
        mux.register(
            "late".to_string(),
            "sub-late".to_string(),
            1,
            handle.clone(),
        );
        let exit = RoutedTerminalFrame::new(
            RouteId::new("sub-late").expect("route"),
            1,
            0,
            encode_process_exit(Some(0)).expect("exit frame"),
        );
        assert_eq!(adapter.try_write(&exit), Ok(()));
        handle.close();
        assert!(handle.snapshot_active().is_none());

        let _sibling = occupy_route(&mux, "sibling", "sub-live", "live");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("flush host events and sibling");
        let frames = parse_written_mux_frames(&writer.written);
        assert!(
            frames.iter().any(is_package_event),
            "host events still flush first: {frames:?}"
        );
        assert!(
            !frames
                .iter()
                .any(|frame| terminal_route(frame) == Some("sub-late")),
            "closed route must not send its abandoned frame: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .any(|frame| terminal_route(frame) == Some("sub-live"))
        );
    }

    #[tokio::test]
    pub(crate) async fn hard_close_after_serialization_abandons_zero_offset_terminal() {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("closed".to_string(), "sub".to_string(), 1, handle.clone());
        let frame = output_frame("sub", "closed");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        let active = handle.snapshot_active().expect("active frame");
        let pending = PendingMuxFrame {
            bytes: PendingMuxBytes::Terminal {
                header: UnixTerminalContainerHeader::new("sub", 1, 0, active.frame.len())
                    .expect("header"),
                body: Arc::clone(active.frame.shared_bytes()),
            },
            offset: 0,
            complete_envelope: Some(handle.clone()),
            class: PendingMuxClass::Terminal,
            delivery_ack: None,
            close_after: false,
        };
        handle.close();
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut state = MuxWriteState {
            pending: Some(pending),
            ..MuxWriteState::default()
        };
        resume_pending_mux_write(&mut writer, &mut state)
            .await
            .expect("abandon serialized frame");
        assert!(writer.written.is_empty());
        assert!(!state.has_pending());
        let _sibling = occupy_route(&mux, "sibling", "sub-live", "live");
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("sibling flush");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 1);
        assert_eq!(terminal_route(&frames[0]), Some("sub-live"));
    }

    #[tokio::test]
    pub(crate) async fn partial_envelope_finishes_once_after_close_before_host_and_sibling_frames()
    {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("closing".to_string(), "sub".to_string(), 1, handle.clone());
        assert_eq!(adapter.try_write(&output_frame("sub", "closing")), Ok(()));
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 8,
            allow_remainder: false,
        };
        let mut state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("partial write");
        assert_eq!(writer.written.len(), 8);
        assert!(state.has_pending());
        handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        let _sibling = occupy_route(&mux, "sibling", "sub-live", "live");
        state
            .enqueue_response(
                "1",
                &daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("status response");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("complete framing and sibling traffic");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 4);
        assert_eq!(terminal_route(&frames[0]), Some("sub"));
        assert_eq!(
            frames
                .iter()
                .filter(|frame| terminal_route(frame) == Some("sub"))
                .count(),
            1
        );
        assert!(
            frames[1..]
                .iter()
                .any(|frame| response_kind(frame) == Some(DaemonResponseKind::Status))
        );
        assert!(
            frames[1..]
                .iter()
                .any(|frame| closed_event_session(frame) == Some("closing"))
        );
        assert_eq!(frames.last().and_then(terminal_route), Some("sub-live"));
        assert!(!state.has_pending());
        let length = writer.written.len();
        flush_unix_mux_writes(&mut writer, &mux, &mut state, None)
            .await
            .expect("no replay");
        assert_eq!(writer.written.len(), length);
    }

    #[tokio::test]
    pub(crate) async fn finishing_a_partial_live_write_does_not_defer_the_next_frame() {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("s".to_string(), "sub".to_string(), 1, handle.clone());
        let output = output_frame("sub", "out");
        assert_eq!(adapter.try_write(&output), Ok(()));
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 8,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("partial live write");
        assert!(write_state.has_pending());
        assert_eq!(
            adapter.pressure(),
            TerminalAdapterPressure::Full,
            "a partial envelope must keep the active slot occupied"
        );
        assert_eq!(
            adapter.try_write(&output),
            Err(botster_core::contract::terminal_adapter::TerminalAdapterWriteError::Full)
        );
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("finish live write");
        let exit = RoutedTerminalFrame::new(
            RouteId::new("sub").expect("route"),
            1,
            0,
            encode_process_exit(Some(0)).expect("exit frame"),
        );
        assert_eq!(adapter.try_write(&exit), Ok(()));
        assert_eq!(
            mux.snapshot_writes().len(),
            1,
            "a completed live send must not defer the next occupant"
        );
    }

    #[tokio::test]
    pub(crate) async fn host_event_flushes_before_new_terminal_slots() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let (mut closer, close_handle) = mux.create_adapter();
        mux.register(
            "closing".to_string(),
            "sub-close".to_string(),
            1,
            close_handle.clone(),
        );
        assert_eq!(
            closer.try_write(&output_frame("sub-close", "close")),
            Ok(())
        );
        close_handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("host-first flush");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(
            frames.first().and_then(closed_event_session),
            Some("closing"),
            "host Event must precede new terminal slots: {frames:?}"
        );
    }

    #[tokio::test]
    pub(crate) async fn partial_terminal_then_shutdown_response_acks_after_written() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("partial terminal");
        let (ack_tx, ack_rx) = mpsc::channel();
        write_state
            .enqueue_response(
                "9",
                &daemon_response_base(DaemonResponseKind::Shutdown),
                Some(ack_tx),
                true,
            )
            .expect("enqueue shutdown");
        assert!(ack_rx.try_recv().is_err());
        assert!(write_state.has_close_after_pending());
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("cannot finish while stalled");
        assert!(ack_rx.try_recv().is_err());
        assert!(write_state.has_close_after_pending());
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("finish close-after");
        ack_rx
            .try_recv()
            .expect("ack after complete shutdown frame");
        assert!(!write_state.has_close_after_pending());
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 2);
        assert!(terminal_route(&frames[0]).is_some());
        assert_eq!(
            response_kind(&frames[1]),
            Some(DaemonResponseKind::Shutdown)
        );
    }

    #[tokio::test]
    pub(crate) async fn partial_terminal_then_update_response_acks_after_written() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("partial terminal");
        let (ack_tx, ack_rx) = mpsc::channel();
        write_state
            .enqueue_response(
                "2",
                &daemon_response_base(DaemonResponseKind::HubUpdate),
                Some(ack_tx),
                false,
            )
            .expect("enqueue update");
        assert!(ack_rx.try_recv().is_err());
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("still pending");
        assert!(ack_rx.try_recv().is_err());
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("finish update");
        ack_rx.try_recv().expect("ack after complete update frame");
        let frames = parse_written_mux_frames(&writer.written);
        assert_eq!(frames.len(), 2);
        assert_eq!(
            response_kind(&frames[1]),
            Some(DaemonResponseKind::HubUpdate)
        );
    }

    #[tokio::test]
    pub(crate) async fn stalled_response_stays_bounded() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: 6,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("partial terminal");
        write_state
            .enqueue_response(
                "3",
                &daemon_response_base(DaemonResponseKind::Status),
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
        assert!(terminal_route(&frames[0]).is_some());
        assert_eq!(response_kind(&frames[1]), Some(DaemonResponseKind::Status));
        assert!(!write_state.has_pending());
    }
}
