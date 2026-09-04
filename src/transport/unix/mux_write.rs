//! Unix framing and mux scheduling.
use std::collections::VecDeque;
use std::env;
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};

use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{
    DaemonDiagnostic, DaemonOperatorError, DaemonRequest, DaemonResponse, DaemonResponseKind,
    DaemonUnixTerminalEnvelope,
};
use serde_json::Value;

use crate::admission::budgets::{
    DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_INCOMPLETE_FRAME_TIMEOUT, DAEMON_MAX_FRAME_BYTES,
};
use crate::client_api_dto::response::daemon_response_base;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapterHandle};

#[derive(Default)]
pub(crate) struct MuxWriteState {
    pending: Option<PendingMuxFrame>,
    queued_responses: VecDeque<PendingMuxFrame>,
    last_host_class: Option<crate::transport::unix::host_write_order::HostControlClass>,
}

impl MuxWriteState {
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.is_some() || !self.queued_responses.is_empty()
    }

    pub(crate) fn has_close_after_pending(&self) -> bool {
        self.pending.as_ref().is_some_and(|frame| frame.close_after)
            || self.queued_responses.iter().any(|frame| frame.close_after)
    }

    pub(crate) fn has_pending_response(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|frame| frame.class == PendingMuxClass::Response)
            || !self.queued_responses.is_empty()
    }

    pub(crate) fn pending_response_count(&self) -> usize {
        let pending =
            self.pending
                .as_ref()
                .is_some_and(|frame| frame.class == PendingMuxClass::Response) as usize;
        pending + self.queued_responses.len()
    }

    pub(crate) fn enqueue_response(
        &mut self,
        response: &DaemonResponse,
        delivery_ack: Option<mpsc::Sender<()>>,
        close_after: bool,
    ) -> DaemonTransportResult<()> {
        self.queued_responses.push_back(serialize_mux_frame(
            response,
            None,
            PendingMuxClass::Response,
            delivery_ack,
            close_after,
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

pub(crate) struct PendingMuxFrame {
    bytes: Vec<u8>,
    offset: usize,
    complete_envelope: Option<UnixTerminalAdapterHandle>,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    close_after: bool,
    backpressured: bool,
    from_late: bool,
    released_slot: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MuxWrite {
    Written,
    Pending,
}

pub(crate) fn unix_mux_blocks_entity_subscription(
    mux: &UnixConnectionMux,
    write_state: &MuxWriteState,
) -> bool {
    write_state.has_pending() || mux.has_unsent_mux_writes() || mux.has_bound_routes()
}

pub(crate) fn entity_subscription_mux_busy_error() -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: "unix_mux_owns_connection".to_string(),
        request_id: "daemon-subscribe-entities".to_string(),
        operation: "subscribe_entities".to_string(),
        message: "entity subscription cannot start while the Unix mux owns this connection"
            .to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "subscribe_entities",
            "unix mux still owns bound routes or unsent frames",
        )],
    });
    response
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

pub(crate) fn unix_event_flush_stalled() -> bool {
    unix_event_flush_stalled_from(
        env::var("BOTSTER_ENV").ok().as_deref(),
        env::var_os("BOTSTER_HUB_TEST_STALL_UNIX_EVENT_FLUSH").as_deref(),
    )
}

pub(crate) fn unix_event_flush_stalled_from(
    botster_env: Option<&str>,
    stall_path: Option<&std::ffi::OsStr>,
) -> bool {
    botster_env == Some("test") && stall_path.is_some_and(|path| Path::new(path).exists())
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
        let control_ready = !write_state.queued_responses.is_empty();
        let event_ready = !unix_event_flush_stalled()
            && (mux.has_pending_event()
                || event_mailbox.is_some_and(
                    crate::subscription::package_events::ClientEventMailbox::has_ready_event,
                ));
        match next_ready_host_control_class(write_state.last_host_class, control_ready, event_ready)
        {
            Some(HostControlClass::Control) => {
                let Some(frame) = write_state.queued_responses.pop_front() else {
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
                let event = mux.pop_pending_event().or_else(|| {
                    event_mailbox.and_then(
                        crate::subscription::package_events::ClientEventMailbox::take_ready_event,
                    )
                });
                let Some(event) = event else {
                    break;
                };
                write_state.last_host_class = Some(HostControlClass::Event);
                write_state.pending = Some(serialize_mux_frame(
                    &event,
                    None,
                    PendingMuxClass::Event,
                    None,
                    false,
                )?);
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
        for (session_id, subscription_id, handle, bytes, from_late) in writes {
            let envelope = botster_hub_client::DaemonUnixTerminalEnvelope::from_frame_bytes(
                session_id,
                subscription_id,
                &bytes,
            );
            let mut pending = serialize_mux_frame(
                &envelope,
                Some(handle),
                PendingMuxClass::Terminal,
                None,
                false,
            )?;
            pending.from_late = from_late;
            write_state.pending = Some(pending);
            if resume_pending_mux_write(writer, write_state).await? == MuxWrite::Pending {
                return Ok(());
            }
        }
    }
    Ok(())
}

pub(crate) fn serialize_mux_frame<T: serde::Serialize>(
    frame: &T,
    complete_envelope: Option<UnixTerminalAdapterHandle>,
    class: PendingMuxClass,
    delivery_ack: Option<mpsc::Sender<()>>,
    close_after: bool,
) -> DaemonTransportResult<PendingMuxFrame> {
    let mut bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    bytes.push(b'\n');
    Ok(PendingMuxFrame {
        bytes,
        offset: 0,
        complete_envelope,
        class,
        delivery_ack,
        close_after,
        backpressured: false,
        from_late: false,
        released_slot: false,
    })
}

pub(crate) fn abandon_zero_offset_terminal_for_response(write_state: &mut MuxWriteState) {
    let should_abandon = write_state.pending.as_ref().is_some_and(|pending| {
        pending.class == PendingMuxClass::Terminal
            && pending.offset == 0
            && !write_state.queued_responses.is_empty()
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
    match write_frame_bytes_resumable(writer, pending).await? {
        MuxWrite::Written => {
            let pending = write_state.pending.take().expect("pending mux frame");
            if let Some(delivery_ack) = pending.delivery_ack {
                let _ = delivery_ack.send(());
            }
            if let Some(handle) = pending.complete_envelope {
                if pending.backpressured {
                    handle.defer_flush();
                }
                if !handle.is_closed() && !pending.released_slot {
                    let _ = handle.complete_active();
                }
                if pending.from_late {
                    let _ = handle.take_late_egress();
                }
            }
            Ok(MuxWrite::Written)
        }
        MuxWrite::Pending => {
            if pending.class == PendingMuxClass::Terminal && pending.offset == 0 {
                abandon_pending_terminal(write_state);
                return Ok(MuxWrite::Written);
            }
            pending.backpressured = true;
            if pending.class == PendingMuxClass::Terminal
                && pending.offset > 0
                && !pending.from_late
                && !pending.released_slot
                && let Some(handle) = pending.complete_envelope.as_ref()
                && !handle.is_closed()
            {
                let _ = handle.complete_active();
                pending.released_slot = true;
            }
            Ok(MuxWrite::Pending)
        }
    }
}

pub(crate) async fn write_frame_bytes_resumable(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    pending: &mut PendingMuxFrame,
) -> DaemonTransportResult<MuxWrite> {
    while pending.offset < pending.bytes.len() {
        match tokio::time::timeout(
            Duration::from_millis(50),
            writer.write(&pending.bytes[pending.offset..]),
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

pub(crate) async fn read_async_frame<T, R>(
    reader: &mut AsyncBufReader<R>,
    first_byte_timeout: Option<Duration>,
) -> Result<T, ClientDaemonTransportError>
where
    T: for<'de> serde::Deserialize<'de>,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut first = [0_u8; 1];
    let read_first = reader.read(&mut first);
    let count = if let Some(timeout) = first_byte_timeout {
        tokio::time::timeout(timeout, read_first)
            .await
            .map_err(|_| {
                ClientDaemonTransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "daemon handshake deadline elapsed",
                ))
            })?
            .map_err(ClientDaemonTransportError::Io)?
    } else {
        read_first.await.map_err(ClientDaemonTransportError::Io)?
    };
    if count == 0 {
        return Err(ClientDaemonTransportError::ClientDisconnected);
    }
    bytes.push(first[0]);
    if first[0] != b'\n' {
        tokio::time::timeout(DAEMON_INCOMPLETE_FRAME_TIMEOUT, async {
            loop {
                let available = reader
                    .fill_buf()
                    .await
                    .map_err(ClientDaemonTransportError::Io)?;
                if available.is_empty() {
                    return Err(ClientDaemonTransportError::Protocol(
                        "daemon frame ended before newline",
                    ));
                }
                let consumed = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |index| index + 1);
                if bytes.len().saturating_add(consumed) > DAEMON_MAX_FRAME_BYTES {
                    return Err(ClientDaemonTransportError::Protocol(
                        "daemon frame exceeded size bound",
                    ));
                }
                bytes.extend_from_slice(&available[..consumed]);
                reader.consume(consumed);
                if bytes.last() == Some(&b'\n') {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| {
            ClientDaemonTransportError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "daemon incomplete frame deadline elapsed",
            ))
        })??;
    }
    if bytes.len() > DAEMON_MAX_FRAME_BYTES {
        return Err(ClientDaemonTransportError::Protocol(
            "daemon frame exceeded size bound",
        ));
    }
    if bytes.last() != Some(&b'\n') {
        return Err(ClientDaemonTransportError::Protocol(
            "daemon frame ended before newline",
        ));
    }
    serde_json::from_slice(&bytes).map_err(ClientDaemonTransportError::Json)
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum UnixInbound {
    Request(DaemonRequest),
    Terminal(DaemonUnixTerminalEnvelope),
}

pub(crate) async fn read_async_inbound<R>(
    reader: &mut AsyncBufReader<R>,
    first_byte_timeout: Option<Duration>,
) -> Result<UnixInbound, ClientDaemonTransportError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let value: Value = read_async_frame(reader, first_byte_timeout).await?;
    if value.get("plane").and_then(Value::as_str) == Some(botster_hub_client::UNIX_TERMINAL_PLANE)
        && value.get("kind").and_then(Value::as_str) == Some(botster_hub_client::UNIX_TERMINAL_KIND)
    {
        return serde_json::from_value(value)
            .map(UnixInbound::Terminal)
            .map_err(ClientDaemonTransportError::Json);
    }
    serde_json::from_value(value)
        .map(UnixInbound::Request)
        .map_err(ClientDaemonTransportError::Json)
}

pub(crate) async fn write_async_frame<T>(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &T,
) -> DaemonTransportResult<()>
where
    T: serde::Serialize,
{
    let mut bytes = serde_json::to_vec(frame).map_err(DaemonTransportError::Json)?;
    bytes.push(b'\n');
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

#[cfg(test)]
pub(crate) mod mux_write_resume_tests {
    use super::{
        MuxWrite, MuxWriteState, PendingMuxClass, PendingMuxFrame,
        entity_subscription_mux_busy_error, flush_pending_responses, flush_unix_mux_writes,
        resume_pending_mux_write, serialize_mux_frame, unix_event_flush_stalled_from,
        unix_mux_blocks_entity_subscription, write_frame_bytes_resumable,
    };
    use crate::client_api_dto::response::daemon_response_base;
    use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapter};
    use botster_core::contract::terminal_adapter::{TerminalAdapter, TerminalAdapterPressure};
    use botster_hub_client::{
        DaemonEvent, DaemonResponseKind, DaemonUnixTerminalEnvelope,
        TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER, parse_unix_mux_value,
    };
    use botster_terminal_protocol::TerminalFrame;
    use std::io;
    use std::pin::Pin;
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
        let mut bytes = serde_json::to_vec(event).expect("serialize");
        bytes.push(b'\n');
        bytes
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
        let mut pending = PendingMuxFrame {
            bytes: expected.clone(),
            offset: 0,
            complete_envelope: None,
            class: PendingMuxClass::Event,
            delivery_ack: None,
            close_after: false,
            backpressured: false,
            from_late: false,
            released_slot: false,
        };

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
        assert_eq!(
            writer.written.iter().filter(|byte| **byte == b'\n').count(),
            1
        );
        let line = std::str::from_utf8(&writer.written)
            .expect("utf8")
            .trim_end();
        let parsed = parse_unix_mux_value(serde_json::from_str(line).expect("json"))
            .expect("classify mux frame");
        match parsed {
            botster_hub_client::DaemonUnixMuxFrame::Event(
                DaemonEvent::TerminalSubscriptionClosed {
                    session_id,
                    generation,
                    reason,
                    ..
                },
            ) => {
                assert_eq!(session_id, "session");
                assert_eq!(generation, 2);
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
        let mut pending = PendingMuxFrame {
            bytes: first_bytes.clone(),
            offset: 0,
            complete_envelope: None,
            class: PendingMuxClass::Event,
            delivery_ack: None,
            close_after: false,
            backpressured: false,
            from_late: false,
            released_slot: false,
        };
        let result = write_frame_bytes_resumable(&mut writer, &mut pending).await;
        assert!(matches!(result, Ok(MuxWrite::Pending)));
        assert_eq!(writer.written, first_bytes[..4]);
        assert_ne!(writer.written, [first_bytes.clone(), second_bytes].concat());
        assert_eq!(pending.bytes, first_bytes);
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
        let payload = format!(r#"{{"type":"terminal_output","marker":"{marker}"}}"#);
        let frame = TerminalFrame::from_bytes(payload.as_bytes()).expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        adapter
    }

    pub(crate) fn parse_written_mux_lines(
        written: &[u8],
    ) -> Vec<botster_hub_client::DaemonUnixMuxFrame> {
        written
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                parse_unix_mux_value(serde_json::from_slice(line).expect("json line"))
                    .expect("classify mux frame")
            })
            .collect()
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
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            lines
                .iter()
                .any(|line| matches!(line, botster_hub_client::DaemonUnixMuxFrame::Terminal(_))),
            "the original flood frame must still be delivered, lines={lines:?}"
        );
    }

    #[tokio::test]
    pub(crate) async fn partial_terminal_then_response_parses_two_complete_mux_lines() {
        let mux = UnixConnectionMux::new();
        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        let terminal = mux.snapshot_writes();
        let prefix = 8.min(terminal[0].3.len().saturating_add(16));
        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: prefix,
            allow_remainder: false,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("first flush");
        assert!(write_state.pending.is_some());
        assert!(write_state.pending.as_ref().is_some_and(|pending| {
            pending.class == PendingMuxClass::Terminal && pending.offset == prefix
        }));

        let response = daemon_response_base(DaemonResponseKind::Status);
        write_state
            .enqueue_response(&response, None, false)
            .expect("enqueue status");
        writer.allow_remainder = true;
        writer.stall_after = usize::MAX;
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("resume flush");
        assert!(!write_state.has_pending());
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2, "expected two complete mux lines");
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(DaemonUnixTerminalEnvelope {
                ref session_id,
                ..
            }) if session_id == "stall"
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Status
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
        let serialized = serde_json::to_vec(&botster_hub_client::DaemonEvent::PackageEvent {
            subscription_id: "sub".to_string(),
            owner: "owner".to_string(),
            name: "ready".to_string(),
            payload: serde_json::json!({ "ok": true }),
        })
        .expect("serialize")
        .len()
            + 1;
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
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            matches!(
                lines[0],
                botster_hub_client::DaemonUnixMuxFrame::Event(
                    botster_hub_client::DaemonEvent::PackageEvent { .. }
                )
            ),
            "partial PackageEvent must finish before a Response: {lines:?}"
        );
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Status
        ));
    }

    #[test]
    pub(crate) fn unix_event_stall_latch_requires_test_mode() {
        let stall = std::env::temp_dir().join(format!(
            "bh-event-stall-negative-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::write(&stall, b"stall").expect("stall file");
        assert!(unix_event_flush_stalled_from(
            Some("test"),
            Some(stall.as_os_str())
        ));
        assert!(
            !unix_event_flush_stalled_from(Some("production"), Some(stall.as_os_str())),
            "non-test BOTSTER_ENV must ignore the stall latch"
        );
        assert!(
            !unix_event_flush_stalled_from(None, Some(stall.as_os_str())),
            "unset BOTSTER_ENV must ignore the stall latch"
        );
        let _ = std::fs::remove_file(&stall);
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
                &daemon_response_base(DaemonResponseKind::Status),
                None,
                false,
            )
            .expect("enqueue status");
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("bounded turn");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            lines.len() <= crate::transport::unix::host_write_order::MAX_HOST_FRAMES_PER_FLUSH_TURN,
            "one flush turn must not drain the flood: {lines:?}"
        );
        assert!(
            lines.iter().any(|line| matches!(
                line,
                botster_hub_client::DaemonUnixMuxFrame::Response(response)
                    if response.kind == DaemonResponseKind::Status
            )),
            "Status must progress after event draining starts: {lines:?}"
        );
        assert!(
            lines.iter().any(|line| matches!(
                line,
                botster_hub_client::DaemonUnixMuxFrame::Event(
                    botster_hub_client::DaemonEvent::PackageEvent { .. }
                )
            )),
            "an event frame must also progress: {lines:?}"
        );
        assert!(
            mailbox.has_ready_event(),
            "remaining events stay queued across turns"
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
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 1);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(DaemonUnixTerminalEnvelope {
                ref session_id,
                ..
            }) if session_id == "sibling"
        ));
    }

    #[tokio::test]
    pub(crate) async fn remaining_host_events_do_not_skip_parked_late_terminal() {
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
        let frame = TerminalFrame::from_bytes(br#"{"type":"process_exit","status":0}"#)
            .expect("opaque frame");
        assert_eq!(adapter.try_write(&frame), Ok(()));
        handle.close();
        assert!(handle.peek_late_egress().is_some());

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, Some(&mailbox))
            .await
            .expect("flush remaining host plus late terminal");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            lines.iter().any(|line| matches!(
                line,
                botster_hub_client::DaemonUnixMuxFrame::Event(
                    botster_hub_client::DaemonEvent::PackageEvent { .. }
                )
            )),
            "host events still flush first: {lines:?}"
        );
        assert!(
            lines.iter().any(|line| matches!(
                line,
                botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)
                    if envelope.session_id == "late"
            )),
            "parked late terminal must flush in the same turn as remaining host events: {lines:?}"
        );
    }

    #[tokio::test]
    pub(crate) async fn live_output_completion_does_not_take_parked_process_exit() {
        let mux = UnixConnectionMux::new();
        let (mut adapter, handle) = mux.create_adapter();
        mux.register("live".to_string(), "sub".to_string(), 1, handle.clone());
        let output = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"out"}"#)
            .expect("opaque output");
        assert_eq!(adapter.try_write(&output), Ok(()));
        let output_bytes = handle
            .snapshot_active()
            .expect("live output occupies the slot");
        let envelope = botster_hub_client::DaemonUnixTerminalEnvelope::from_frame_bytes(
            "live".to_string(),
            "sub".to_string(),
            &output_bytes,
        );
        let mut pending = serialize_mux_frame(
            &envelope,
            Some(handle.clone()),
            PendingMuxClass::Terminal,
            None,
            false,
        )
        .expect("serialize live output");
        pending.from_late = false;
        pending.released_slot = true;
        let _ = handle.complete_active();
        let exit = TerminalFrame::from_bytes(br#"{"type":"process_exit","status":0}"#)
            .expect("opaque process_exit");
        assert_eq!(adapter.try_write(&exit), Ok(()));
        handle.close();
        assert!(
            handle.peek_late_egress().is_some(),
            "close must park process_exit while live output is on the wire"
        );

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState {
            pending: Some(pending),
            ..MuxWriteState::default()
        };
        resume_pending_mux_write(&mut writer, &mut write_state)
            .await
            .expect("finish live output");
        assert!(
            handle.peek_late_egress().is_some(),
            "completing live output must not take a process_exit parked during the send"
        );

        flush_unix_mux_writes(&mut writer, &mux, &mut write_state, None)
            .await
            .expect("flush parked process_exit");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            lines.iter().any(|line| matches!(
                line,
                botster_hub_client::DaemonUnixMuxFrame::Terminal(envelope)
                    if envelope.session_id == "live"
                        && envelope
                            .payload_bytes()
                            .is_ok_and(|bytes| bytes.windows(b"process_exit".len()).any(|window| {
                                window == b"process_exit"
                            }))
            )),
            "parked process_exit must still reach the socket: {lines:?}"
        );
        assert!(handle.peek_late_egress().is_none());
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
        let frame = TerminalFrame::from_bytes(br#"{"type":"terminal_output","marker":"close"}"#)
            .expect("opaque frame");
        assert_eq!(closer.try_write(&frame), Ok(()));
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
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            matches!(
                lines.first(),
                Some(botster_hub_client::DaemonUnixMuxFrame::Event(
                    DaemonEvent::TerminalSubscriptionClosed { session_id, .. }
                )) if session_id == "closing"
            ),
            "host Event must precede new terminal slots: {lines:?}"
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
        ack_rx.try_recv().expect("ack after complete shutdown line");
        assert!(!write_state.has_close_after_pending());
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(_)
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Shutdown
        ));
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
        ack_rx.try_recv().expect("ack after complete update line");
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::HubUpdate
        ));
    }

    #[tokio::test]
    pub(crate) async fn stalled_response_stays_bounded_and_blocks_entity_subscription() {
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
        assert!(
            write_state.has_pending(),
            "entity subscription must not start while a mux frame is pending"
        );
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
        assert!(
            unix_mux_blocks_entity_subscription(&mux, &write_state),
            "a bound Unix route must still block the entity-subscription handoff"
        );
        let lines = parse_written_mux_lines(&writer.written);
        assert_eq!(lines.len(), 2);
        assert!(matches!(
            lines[0],
            botster_hub_client::DaemonUnixMuxFrame::Terminal(_)
        ));
        assert!(matches!(
            lines[1],
            botster_hub_client::DaemonUnixMuxFrame::Response(ref response)
                if response.kind == DaemonResponseKind::Status
        ));
        assert!(!write_state.has_pending());
    }

    #[tokio::test]
    pub(crate) async fn bound_route_or_queued_event_blocks_entity_subscription_without_closing_routes()
     {
        let mux = UnixConnectionMux::new();
        let idle = MuxWriteState::default();
        assert!(!unix_mux_blocks_entity_subscription(&mux, &idle));

        let _stall = occupy_route(&mux, "stall", "sub", "flood");
        assert!(mux.has_bound_routes());
        assert!(unix_mux_blocks_entity_subscription(&mux, &idle));
        let handle = mux.snapshot_writes()[0].2.clone();
        assert!(!handle.host_closed());

        handle.close();
        assert_eq!(mux.queue_closed_subscription_events(|_| true), 1);
        assert!(mux.has_unsent_mux_writes());
        assert!(unix_mux_blocks_entity_subscription(&mux, &idle));
        assert!(
            !handle.host_closed(),
            "rejecting SubscribeEntities must not host-close the bound route"
        );
        assert!(mux.has_bound_routes());

        let mut writer = PrefixStallWriter {
            written: Vec::new(),
            stall_after: usize::MAX,
            allow_remainder: true,
        };
        let mut write_state = MuxWriteState::default();
        write_state
            .enqueue_response(&entity_subscription_mux_busy_error(), None, false)
            .expect("enqueue reject");
        flush_pending_responses(&mut writer, &mux, &mut write_state, Instant::now(), None)
            .await
            .expect("write reject");
        let lines = parse_written_mux_lines(&writer.written);
        assert!(
            matches!(
                lines.first(),
                Some(botster_hub_client::DaemonUnixMuxFrame::Response(response))
                    if response.kind == DaemonResponseKind::OperatorError
                        && response.error.as_ref().is_some_and(|error| {
                            error.code == "unix_mux_owns_connection"
                        })
            ),
            "reject must be an OperatorError Response: {lines:?}"
        );
        assert!(
            matches!(
                lines.get(1),
                Some(botster_hub_client::DaemonUnixMuxFrame::Event(
                    DaemonEvent::TerminalSubscriptionClosed { session_id, .. }
                )) if session_id == "stall"
            ),
            "close Event must still flush after the reject: {lines:?}"
        );
        assert!(mux.has_bound_routes());
        assert!(!handle.host_closed());
    }
}
