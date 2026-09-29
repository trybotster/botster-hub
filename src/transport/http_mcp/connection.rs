//! One HTTP/1.1 connection: read a request, gate it, serve it, answer.
//!
//! The connection task owns all socket work. The owner loop sees only
//! `ControlMessage::CallerRequest` values, so a slow or hostile peer can hold
//! nothing but its own task and one admission permit.

use std::time::Duration;

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::admission::budgets::{
    DAEMON_CLIENT_WRITE_TIMEOUT, DAEMON_HANDSHAKE_TIMEOUT, DAEMON_INCOMPLETE_FRAME_TIMEOUT,
};
use crate::daemon::control::message::ControlSender;
use crate::transport::http_mcp::audit::ToolAudit;
use crate::transport::http_mcp::tools::{Dispatched, dispatch};
use crate::transport::http_mcp::wire::{
    AdmittedHead, MAX_HEADER_BLOCK_BYTES, Method, Refusal, admit_head, head_end, refusal_response,
    response_bytes,
};

/// Bytes read from the socket per call.
const READ_CHUNK_BYTES: usize = 8 * 1024;

/// A request that passed every gate, with its whole body.
struct Request {
    head: AdmittedHead,
    body: Vec<u8>,
}

/// Why no request came out of the socket.
enum ReadEnd {
    /// The peer closed, went idle past the deadline, or the daemon is
    /// shutting down: close without an answer.
    Quiet,
    /// The request was refused: answer, then close.
    Refused(Refusal),
}

pub(crate) async fn serve_connection(
    mut stream: TcpStream,
    port: u16,
    control_tx: ControlSender,
    audit: Arc<ToolAudit>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Bytes read past one request belong to the next.
    let mut carry: Vec<u8> = Vec::new();
    loop {
        let request = tokio::select! {
            request = read_request(&mut stream, &mut carry, port) => request,
            _ = shutdown_rx.changed() => return,
        };
        let request = match request {
            Ok(request) => request,
            Err(ReadEnd::Quiet) => return,
            Err(ReadEnd::Refused(refusal)) => {
                let _ = write_all(&mut stream, &refusal_response(refusal)).await;
                return;
            }
        };
        if request.head.method == Method::Other {
            let _ = write_all(&mut stream, &refusal_response(Refusal::MethodNotAllowed)).await;
            return;
        }
        let keep_alive = request.head.keep_alive;
        let (dispatched, record) = tokio::select! {
            served = dispatch(&control_tx, audit.hub_id(), &request.head.token, &request.body) => served,
            _ = shutdown_rx.changed() => return,
        };
        if let Some(record) = record {
            audit.append(&request.head.token, &record).await;
        }
        let bytes = match dispatched {
            Dispatched::Body(body) => response_bytes(200, "OK", &[], &body, keep_alive),
            Dispatched::Accepted => {
                let mut head = b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nCache-Control: no-store\r\nConnection: "
                    .to_vec();
                head.extend_from_slice(if keep_alive { b"keep-alive" } else { b"close" });
                head.extend_from_slice(b"\r\n\r\n");
                head
            }
            Dispatched::Refused(refusal) => {
                let _ = write_all(&mut stream, &refusal_response(refusal)).await;
                return;
            }
        };
        if write_all(&mut stream, &bytes).await.is_err() || !keep_alive {
            return;
        }
    }
}

/// Read one request. `carry` holds bytes read past the previous request and
/// receives any bytes read past this one.
async fn read_request(
    stream: &mut TcpStream,
    carry: &mut Vec<u8>,
    port: u16,
) -> Result<Request, ReadEnd> {
    let mut buffer = std::mem::take(carry);
    // An idle connection waits for its first byte under the handshake
    // deadline. From the first byte, the WHOLE request (head and body) has
    // one frame deadline, so a peer that drips bytes cannot hold the task.
    let mut started = (!buffer.is_empty()).then(Instant::now);
    let head_len = loop {
        if let Some(end) = head_end(&buffer) {
            break end;
        }
        if buffer.len() > MAX_HEADER_BLOCK_BYTES {
            return Err(ReadEnd::Refused(Refusal::HeaderTooLarge));
        }
        let wait = remaining(started, DAEMON_HANDSHAKE_TIMEOUT)?;
        read_some(stream, &mut buffer, wait).await?;
        started.get_or_insert_with(Instant::now);
    };
    // A head that ends beyond the cap is refused even when its blank line
    // arrived in the same read.
    if head_len > MAX_HEADER_BLOCK_BYTES {
        return Err(ReadEnd::Refused(Refusal::HeaderTooLarge));
    }
    let head = admit_head(&buffer[..head_len], port).map_err(ReadEnd::Refused)?;
    let mut body: Vec<u8> = buffer.split_off(head_len);
    while body.len() < head.body_len {
        let wait = remaining(started, DAEMON_HANDSHAKE_TIMEOUT)?;
        read_some(stream, &mut body, wait).await?;
    }
    // Anything past the declared body starts the next request.
    *carry = body.split_off(head.body_len);
    Ok(Request { head, body })
}

/// The time left to read the request: the handshake wait before the first
/// byte, the rest of the frame deadline after it.
fn remaining(started: Option<Instant>, idle: Duration) -> Result<Duration, ReadEnd> {
    match started {
        None => Ok(idle),
        Some(at) => {
            let left = DAEMON_INCOMPLETE_FRAME_TIMEOUT.saturating_sub(at.elapsed());
            if left.is_zero() {
                Err(ReadEnd::Quiet)
            } else {
                Ok(left)
            }
        }
    }
}

/// Read one chunk into `buffer` within `wait`. A close, an error, or the
/// deadline ends the connection quietly.
async fn read_some(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    wait: Duration,
) -> Result<(), ReadEnd> {
    let mut chunk = [0u8; READ_CHUNK_BYTES];
    // timer: deadline — bounds a peer that stops sending; expiry closes the connection.
    match tokio::time::timeout(wait, stream.read(&mut chunk)).await {
        Ok(Ok(0)) | Err(_) | Ok(Err(_)) => Err(ReadEnd::Quiet),
        Ok(Ok(read)) => {
            buffer.extend_from_slice(&chunk[..read]);
            Ok(())
        }
    }
}

async fn write_all(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), ()> {
    // timer: deadline — bounds a peer that stops reading; expiry closes the connection.
    match tokio::time::timeout(DAEMON_CLIENT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
        Ok(Ok(())) => Ok(()),
        _ => Err(()),
    }
}
