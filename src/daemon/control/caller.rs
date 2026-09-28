//! Requests that a session makes over HTTP MCP.
//!
//! The bearer token is the identity. The owner checks it against the session
//! named in the token, in the same turn that admits the request, then runs the
//! request as that session through the ordinary request path. A token that
//! does not parse, names no session, or does not match the stored digest is
//! `caller_unauthenticated`. It is never the operator.

use std::time::Instant;

use botster_core::SessionId;

use crate::HubDaemon;
use crate::daemon::control::operator_refusal;
use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::control::request;
use crate::daemon::owner_loop::{DaemonControlState, send_control_response};
use crate::session_credential::CallerToken;

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
        request,
        reply_tx,
        enqueued_at,
    } = message
    else {
        unreachable!("caller owner received a non-caller control message");
    };
    let Some(caller) = verified_caller(daemon, &token) else {
        return send_control_response(
            reply_tx,
            Ok(operator_refusal(
                CALLER_UNAUTHENTICATED,
                "caller_auth",
                "the bearer token does not identify a running session",
            )),
            None,
        );
    };
    let admitted = ControlMessage::Request {
        request: Box::new(request.into_daemon_request()),
        transport_request_id: None,
        reply_tx,
        response_delivery_rx: None,
        grant_id: None,
        client_id: Some(format!("http-mcp:{}", caller.0)),
        enqueued_at: enqueued_at.min(Instant::now()),
    };
    request::handle_as(
        daemon,
        state,
        transport_handle,
        control_tx,
        admitted,
        Some(caller),
    )
}

/// The session the token proves, or `None`.
///
/// Verification reads the named session's Core metadata and compares the
/// stored digest with the digest of the presented secret. It waits for Core's
/// `session_metadata` query, which arrives with the Core roll; until then no
/// token verifies, so this path fails closed.
fn verified_caller(_daemon: &HubDaemon, token: &CallerToken) -> Option<SessionId> {
    let _ = token;
    None
}
