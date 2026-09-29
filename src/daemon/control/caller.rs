//! Requests that a session makes over HTTP MCP.
//!
//! The bearer token is the identity. Requests that reach Core (messaging,
//! notification, `Whoami`) carry the token into their Core submission, which
//! proves it against the session's stored digest before it acts, so the proof
//! and the effect see one Core state. A token that does not parse, names no
//! session, or does not match the digest is `caller_unauthenticated`. It is
//! never the operator.

use std::time::Instant;

use crate::HubDaemon;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::request;
use crate::daemon::control::{Caller, operator_refusal};
use crate::daemon::owner_loop::{DaemonControlState, send_control_response};

/// The code every refused token gets, whatever the reason.
pub(crate) const CALLER_UNAUTHENTICATED: &str = "caller_unauthenticated";

pub(crate) fn handle(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    transport_handle: &tokio::runtime::Handle,
    control_tx: ControlSender,
    message: ControlMessage,
) -> bool {
    let ControlMessage::CallerRequest {
        token,
        proven,
        request,
        reply_tx,
        enqueued_at,
    } = message
    else {
        unreachable!("caller owner received a non-caller control message");
    };
    let caller = if request.proves_in_core() {
        Caller::Token(token)
    } else {
        match proven {
            Some(session_id) => Caller::Proven(session_id),
            // Only the HTTP task sends these, after proving the token. A
            // request without a proof has none.
            None => {
                return send_control_response(
                    reply_tx,
                    Ok(operator_refusal(
                        CALLER_UNAUTHENTICATED,
                        "caller_auth",
                        "the bearer token does not identify a running session",
                    )),
                    None,
                );
            }
        }
    };
    let admitted = ControlMessage::Request {
        request: Box::new(request.into_daemon_request()),
        transport_request_id: None,
        reply_tx,
        response_delivery_rx: None,
        grant_id: None,
        client_id: caller
            .session_id()
            .map(|session| format!("http-mcp:{}", session.0)),
        enqueued_at: enqueued_at.min(Instant::now()),
    };
    request::handle_as(
        daemon,
        state,
        transport_handle,
        control_tx,
        admitted,
        caller,
    )
}
