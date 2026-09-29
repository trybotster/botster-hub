//! Plugin reads that the Hub owner answers.
//!
//! A plugin thread cannot read the owner's session projection, and no second
//! copy of it exists. It sends one request on the owner's control channel and
//! waits for the answer on a one-slot reply channel. The owner answers in one
//! bounded pass and never waits: a full control queue is a typed refusal for
//! the plugin, an unready projection is a typed reply, and a reply channel the
//! plugin gave up on is ignored.

use std::sync::mpsc;
use std::time::Duration;

use botster_hub_client::DaemonSessionEntity;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::owner_loop::DaemonControlState;
use crate::session_projection::SessionProjection;

/// Rows in one page: the family chunk count.
pub(crate) const SESSIONS_PAGE_ROWS: usize = 8;

#[derive(Debug)]
pub(crate) enum PluginHostCall {
    /// The page of session rows after `after`, in session id order.
    SessionsPage { after: Option<String> },
    /// One session row.
    SessionGet { session_id: String },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PluginHostReply {
    SessionsPage {
        rows: Vec<DaemonSessionEntity>,
        /// The last returned session id when more rows follow.
        next_after: Option<String>,
    },
    Session(Option<DaemonSessionEntity>),
    /// The projection has no complete baseline (start, baseline recovery).
    NotReady,
}

/// One call and where to answer it.
#[derive(Debug)]
pub(crate) struct PluginHostRequest {
    pub(crate) call: PluginHostCall,
    pub(crate) reply: mpsc::SyncSender<PluginHostReply>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostCallError {
    /// The Hub owner is not running, or stopped before it answered.
    OwnerUnavailable,
    /// The control queue is full.
    Backpressured,
    /// The owner did not answer before the deadline.
    TimedOut,
}

/// Send one call to the owner and wait for its answer. Never called on the
/// owner thread.
pub(crate) fn call(
    owner: Option<ControlSender>,
    call: PluginHostCall,
    timeout: Duration,
) -> Result<PluginHostReply, HostCallError> {
    let Some(owner) = owner else {
        return Err(HostCallError::OwnerUnavailable);
    };
    let (reply, receiver) = mpsc::sync_channel(1);
    owner
        .try_send(ControlMessage::PluginHostCall(Box::new(
            PluginHostRequest { call, reply },
        )))
        .map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => HostCallError::Backpressured,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => HostCallError::OwnerUnavailable,
        })?;
    // timer: deadline
    receiver.recv_timeout(timeout).map_err(|error| match error {
        mpsc::RecvTimeoutError::Timeout => HostCallError::TimedOut,
        mpsc::RecvTimeoutError::Disconnected => HostCallError::OwnerUnavailable,
    })
}

/// Answer one call in one bounded pass over the projection. Runs on the owner.
pub(crate) fn serve(state: &DaemonControlState, request: PluginHostRequest) {
    let reply = answer(&state.maintenance.projection, request.call);
    // A plugin that stopped waiting dropped its receiver: nothing to do.
    let _ = request.reply.try_send(reply);
}

fn answer(projection: &SessionProjection, call: PluginHostCall) -> PluginHostReply {
    use std::ops::Bound;
    if !projection.baseline_complete {
        return PluginHostReply::NotReady;
    }
    match call {
        PluginHostCall::SessionGet { session_id } => PluginHostReply::Session(
            projection
                .rows
                .get(&session_id)
                .map(|row| SessionProjection::project_entity(&row.record)),
        ),
        PluginHostCall::SessionsPage { after } => {
            let start = match after.as_deref() {
                Some(after) => Bound::Excluded(after),
                None => Bound::Unbounded,
            };
            let mut rows = Vec::with_capacity(SESSIONS_PAGE_ROWS);
            let mut more = false;
            for (_, row) in projection.rows.range::<str, _>((start, Bound::Unbounded)) {
                if rows.len() == SESSIONS_PAGE_ROWS {
                    more = true;
                    break;
                }
                rows.push(SessionProjection::project_entity(&row.record));
            }
            let next_after = more
                .then(|| rows.last().map(|row| row.session_uuid.clone()))
                .flatten();
            PluginHostReply::SessionsPage { rows, next_after }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::{CoreSessionMetadata, ResizePayload, SessionId, SessionLifecycleState};
    use botster_core_daemon::{
        DaemonSession, RegistrySessionState, SessionLifecycleCursor, SessionLifecycleRecord,
        SessionLifecycleSourceId,
    };

    fn record(id: &str) -> SessionLifecycleRecord {
        SessionLifecycleRecord {
            session: DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        }
    }

    fn sealed(ids: &[&str]) -> SessionProjection {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            SessionLifecycleCursor {
                source_id: SessionLifecycleSourceId("s".to_string()),
                sequence: 1,
            },
            ids.iter().map(|id| record(id)),
        );
        projection
    }

    #[test]
    fn an_unready_projection_is_a_typed_reply_never_an_empty_page() {
        let projection = SessionProjection::default();
        assert_eq!(
            answer(&projection, PluginHostCall::SessionsPage { after: None }),
            PluginHostReply::NotReady
        );
        assert_eq!(
            answer(
                &projection,
                PluginHostCall::SessionGet {
                    session_id: "a".to_string()
                }
            ),
            PluginHostReply::NotReady
        );
    }

    #[test]
    fn a_page_holds_the_chunk_count_and_its_cursor_comes_from_returned_rows() {
        let ids: Vec<String> = (0..SESSIONS_PAGE_ROWS + 3)
            .map(|index| format!("s{index:02}"))
            .collect();
        let projection = sealed(&ids.iter().map(String::as_str).collect::<Vec<_>>());
        let PluginHostReply::SessionsPage { rows, next_after } =
            answer(&projection, PluginHostCall::SessionsPage { after: None })
        else {
            panic!("a page");
        };
        assert_eq!(rows.len(), SESSIONS_PAGE_ROWS);
        assert_eq!(next_after.as_deref(), Some("s07"));
        let PluginHostReply::SessionsPage { rows, next_after } = answer(
            &projection,
            PluginHostCall::SessionsPage { after: next_after },
        ) else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(next_after, None);
    }

    #[test]
    fn a_get_of_an_unknown_session_is_none() {
        let projection = sealed(&["a"]);
        assert_eq!(
            answer(
                &projection,
                PluginHostCall::SessionGet {
                    session_id: "missing".to_string()
                }
            ),
            PluginHostReply::Session(None)
        );
    }

    #[test]
    fn a_closed_reply_channel_is_ignored_and_a_full_queue_is_typed() {
        let (reply, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        // The owner side ignores a plugin that stopped waiting.
        let _ = reply.try_send(PluginHostReply::NotReady);
        let (sender, _keep) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::HostProgressPublished)
            .expect("the one slot is free");
        assert_eq!(
            call(
                Some(sender),
                PluginHostCall::SessionsPage { after: None },
                Duration::from_millis(50)
            ),
            Err(HostCallError::Backpressured)
        );
        assert_eq!(
            call(
                None,
                PluginHostCall::SessionsPage { after: None },
                Duration::from_millis(50)
            ),
            Err(HostCallError::OwnerUnavailable)
        );
    }
}
