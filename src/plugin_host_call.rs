//! Plugin reads that the Hub owner answers.
//!
//! A plugin thread cannot read the owner's session projection, and no second
//! copy of it exists. It sends one request on the owner's control channel and
//! waits for the answer on a one-slot reply channel. The owner answers in one
//! bounded pass and never waits: a full control queue is a typed refusal for
//! the plugin, an unready projection is a typed reply, and a reply channel the
//! plugin gave up on is ignored.
//!
//! Every copy is funded from the plugin's callback account before it is made.
//! The plugin funds the request, its strings and the reply channel. The owner
//! funds the rows it copies and the JSON copy the plugin makes from them, in
//! one reservation made before any row is allocated. The reply carries that
//! charge, so the bytes stay charged until the plugin drops the reply.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use botster_hub_client::DaemonSessionEntity;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::owner_loop::DaemonControlState;
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};
use crate::session_projection::SessionProjection;

/// Rows in one page: the family chunk count.
pub(crate) const SESSIONS_PAGE_ROWS: usize = 8;

/// The longest session id a plugin may name, in bytes.
pub(crate) const MAX_SESSION_ID_BYTES: usize = 256;

/// Retained bytes of the JSON object a plugin builds from one row: sixteen
/// keys, each with its key string, value and map node, and generous slack.
const ROW_JSON_FIXED_BYTES: usize = 4096;

/// The bytes that fund one row's Rust copy and its JSON copy. The JSON copy
/// holds the row's strings again, and the local hub id.
pub(crate) fn row_funding_bytes(entity_bound: usize, hub_id_bytes: usize) -> usize {
    2 * entity_bound + ROW_JSON_FIXED_BYTES + hub_id_bytes
}

#[derive(Debug)]
pub(crate) enum PluginHostCall {
    /// The page of session rows after `after`, in session id order.
    SessionsPage { after: Option<String> },
    /// One session row.
    SessionGet { session_id: String },
}

#[derive(Debug)]
pub(crate) enum PluginHostReply {
    SessionsPage {
        rows: Vec<DaemonSessionEntity>,
        /// The last returned session id when more rows follow.
        next_after: Option<String>,
        /// Funds `rows` and the plugin's JSON copy of them.
        charge: LuaCallbackCharge,
    },
    Session {
        row: Option<Box<DaemonSessionEntity>>,
        /// Funds `row` and the plugin's JSON copy of it.
        charge: LuaCallbackCharge,
    },
    /// The projection has no complete baseline (start, baseline recovery).
    NotReady,
    /// The owner made no copy: the plugin's callback account cannot fund it.
    Refused(Refusal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The account has no room now; a later call may succeed.
    Capacity,
    /// One row is larger than a callback may hold; a retry cannot succeed.
    Quota,
}

/// One call and where to answer it.
#[derive(Debug)]
pub(crate) struct PluginHostRequest {
    pub(crate) call: PluginHostCall,
    /// The calling plugin's account: the owner funds its copies from it.
    pub(crate) memory: Arc<LuaMemoryAccount>,
    /// Bytes of the local hub id the plugin adds to each row.
    pub(crate) hub_id_bytes: usize,
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

/// Bytes the plugin funds before it reads its arguments or sends a call: the
/// control message, the request, the longest string a call may own, and the
/// reply channel with its lease.
pub(crate) fn fixed_call_bytes() -> Option<usize> {
    crate::lua_memory::layout::single_reply_bytes::<PluginHostReply>(true)?
        .checked_add(std::mem::size_of::<ControlMessage>())?
        .checked_add(std::mem::size_of::<PluginHostRequest>())?
        .checked_add(MAX_SESSION_ID_BYTES)
}

/// Send one call to the owner and wait for its answer. Never called on the
/// owner thread. The caller has already funded `fixed_call_bytes()`.
pub(crate) fn call(
    owner: Option<ControlSender>,
    call: PluginHostCall,
    memory: Arc<LuaMemoryAccount>,
    hub_id_bytes: usize,
    timeout: Duration,
) -> Result<PluginHostReply, HostCallError> {
    let Some(owner) = owner else {
        return Err(HostCallError::OwnerUnavailable);
    };
    let (reply, receiver) = mpsc::sync_channel(1);
    owner
        .try_send(ControlMessage::PluginHostCall(Box::new(
            PluginHostRequest {
                call,
                memory,
                hub_id_bytes,
                reply,
            },
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
    let reply = answer(
        &state.maintenance.projection,
        request.call,
        &request.memory,
        request.hub_id_bytes,
    );
    // A plugin that stopped waiting dropped its receiver: the reply, and the
    // charge it carries, drop here.
    let _ = request.reply.try_send(reply);
}

fn refusal(error: crate::lua_memory::LuaCallbackAdmissionError) -> PluginHostReply {
    use crate::lua_memory::LuaCallbackAdmissionError as Admission;
    PluginHostReply::Refused(match error {
        Admission::Quota => Refusal::Quota,
        Admission::Capacity(_) => Refusal::Capacity,
    })
}

pub(crate) fn answer(
    projection: &SessionProjection,
    call: PluginHostCall,
    memory: &Arc<LuaMemoryAccount>,
    hub_id_bytes: usize,
) -> PluginHostReply {
    use std::ops::Bound;
    if !projection.baseline_complete {
        return PluginHostReply::NotReady;
    }
    match call {
        PluginHostCall::SessionGet { session_id } => {
            let Some(row) = projection.rows.get(&session_id) else {
                return match memory.reserve_callback_total(0) {
                    Ok(charge) => PluginHostReply::Session { row: None, charge },
                    Err(error) => refusal(error),
                };
            };
            let bytes = row_funding_bytes(
                SessionProjection::entity_bound_bytes(&row.record),
                hub_id_bytes,
            );
            match memory.reserve_callback_total(bytes) {
                Ok(charge) => PluginHostReply::Session {
                    row: Some(Box::new(SessionProjection::project_entity(&row.record))),
                    charge,
                },
                Err(error) => refusal(error),
            }
        }
        PluginHostCall::SessionsPage { after } => {
            let start = match after.as_deref() {
                Some(after) => Bound::Excluded(after),
                None => Bound::Unbounded,
            };
            let per_callback = memory.limits().per_callback_bytes;
            // Choose the rows first, without copying any, and stop at the
            // count or the callback ceiling. The reply's row storage is funded
            // for the chosen count.
            let mut chosen = [None; SESSIONS_PAGE_ROWS];
            let mut count = 0usize;
            let mut bytes = 0usize;
            let mut more = false;
            for (_, row) in projection.rows.range::<str, _>((start, Bound::Unbounded)) {
                let next = row_funding_bytes(
                    SessionProjection::entity_bound_bytes(&row.record),
                    hub_id_bytes,
                );
                let with_next = bytes.saturating_add(next);
                if count == SESSIONS_PAGE_ROWS || with_next > per_callback {
                    if count == 0 {
                        return PluginHostReply::Refused(Refusal::Quota);
                    }
                    more = true;
                    break;
                }
                bytes = with_next;
                chosen[count] = Some(&row.record);
                count += 1;
            }
            match memory.reserve_callback_total(bytes) {
                Ok(charge) => {
                    let rows: Vec<DaemonSessionEntity> = chosen[..count]
                        .iter()
                        .flatten()
                        .map(|record| SessionProjection::project_entity(record))
                        .collect();
                    let next_after = more
                        .then(|| rows.last().map(|row| row.session_uuid.clone()))
                        .flatten();
                    PluginHostReply::SessionsPage {
                        rows,
                        next_after,
                        charge,
                    }
                }
                Err(error) => refusal(error),
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;
    use botster_core::{CoreSessionMetadata, ResizePayload, SessionId, SessionLifecycleState};
    use botster_core_daemon::{
        DaemonSession, RegistrySessionState, SessionLifecycleCursor, SessionLifecycleRecord,
        SessionLifecycleSourceId,
    };

    const HUB_BYTES: usize = 24;

    pub(crate) fn record(id: &str) -> SessionLifecycleRecord {
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

    pub(crate) fn sealed(ids: &[&str]) -> SessionProjection {
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

    pub(crate) fn account(per_callback: usize, total: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: per_callback,
            total_callback_bytes: total,
        })
        .expect("limits")
    }

    fn roomy() -> Arc<LuaMemoryAccount> {
        account(8 * 1024 * 1024, 64 * 1024 * 1024)
    }

    fn one_row_bytes() -> usize {
        row_funding_bytes(
            SessionProjection::entity_bound_bytes(&record("s00")),
            HUB_BYTES,
        )
    }

    fn page(
        projection: &SessionProjection,
        after: Option<&str>,
        memory: &Arc<LuaMemoryAccount>,
    ) -> PluginHostReply {
        answer(
            projection,
            PluginHostCall::SessionsPage {
                after: after.map(str::to_string),
            },
            memory,
            HUB_BYTES,
        )
    }

    #[test]
    fn an_unready_projection_is_a_typed_reply_never_an_empty_page() {
        let projection = SessionProjection::default();
        assert!(matches!(
            page(&projection, None, &roomy()),
            PluginHostReply::NotReady
        ));
        assert!(matches!(
            answer(
                &projection,
                PluginHostCall::SessionGet {
                    session_id: "a".to_string()
                },
                &roomy(),
                HUB_BYTES
            ),
            PluginHostReply::NotReady
        ));
    }

    #[test]
    fn a_page_holds_the_chunk_count_and_its_cursor_comes_from_returned_rows() {
        let ids: Vec<String> = (0..SESSIONS_PAGE_ROWS + 3)
            .map(|index| format!("s{index:02}"))
            .collect();
        let projection = sealed(&ids.iter().map(String::as_str).collect::<Vec<_>>());
        let memory = roomy();
        let PluginHostReply::SessionsPage {
            rows,
            next_after,
            charge,
        } = page(&projection, None, &memory)
        else {
            panic!("a page");
        };
        assert_eq!(rows.len(), SESSIONS_PAGE_ROWS);
        assert_eq!(next_after.as_deref(), Some("s07"));
        assert_eq!(charge.bytes(), SESSIONS_PAGE_ROWS * one_row_bytes());
        let PluginHostReply::SessionsPage {
            rows, next_after, ..
        } = page(&projection, next_after.as_deref(), &memory)
        else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(next_after, None);
    }

    #[test]
    fn a_get_of_an_unknown_session_is_none() {
        let projection = sealed(&["a"]);
        let PluginHostReply::Session { row, .. } = answer(
            &projection,
            PluginHostCall::SessionGet {
                session_id: "missing".to_string(),
            },
            &roomy(),
            HUB_BYTES,
        ) else {
            panic!("a session reply");
        };
        assert!(row.is_none());
    }

    #[test]
    fn a_page_stops_at_the_callback_ceiling_and_its_cursor_is_the_last_returned_row() {
        let ids = ["s00", "s01", "s02", "s03", "s04"];
        let projection = sealed(&ids);
        // The ceiling holds three rows, not the chunk count.
        let memory = account(3 * one_row_bytes(), 64 * 1024 * 1024);
        let PluginHostReply::SessionsPage {
            rows,
            next_after,
            charge,
        } = page(&projection, None, &memory)
        else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(next_after.as_deref(), Some("s02"));
        assert_eq!(charge.bytes(), 3 * one_row_bytes());
    }

    #[test]
    fn a_row_above_the_callback_ceiling_is_a_quota_refusal_and_nothing_is_charged() {
        let projection = sealed(&["s00"]);
        let memory = account(one_row_bytes() - 1, 64 * 1024 * 1024);
        assert!(matches!(
            page(&projection, None, &memory),
            PluginHostReply::Refused(Refusal::Quota)
        ));
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn an_exhausted_account_refuses_a_page_and_a_get_before_any_copy() {
        let projection = sealed(&["s00", "s01"]);
        // The account holds exactly one row. A held reservation leaves no room.
        let memory = account(one_row_bytes(), one_row_bytes());
        let held = memory
            .reserve_callback_total(one_row_bytes())
            .expect("the held reservation");
        assert!(matches!(
            page(&projection, None, &memory),
            PluginHostReply::Refused(Refusal::Capacity)
        ));
        assert!(matches!(
            answer(
                &projection,
                PluginHostCall::SessionGet {
                    session_id: "s00".to_string()
                },
                &memory,
                HUB_BYTES
            ),
            PluginHostReply::Refused(Refusal::Capacity)
        ));
        assert_eq!(
            memory.usage().1,
            one_row_bytes(),
            "a refusal charges nothing"
        );
        drop(held);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn dropping_a_reply_releases_its_charge() {
        let projection = sealed(&["s00", "s01"]);
        let memory = roomy();
        let reply = page(&projection, None, &memory);
        assert_eq!(memory.usage().1, 2 * one_row_bytes());
        drop(reply);
        assert_eq!(memory.usage().1, 0);
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
        let call_page = || PluginHostCall::SessionsPage { after: None };
        assert_eq!(
            call(
                Some(sender),
                call_page(),
                roomy(),
                HUB_BYTES,
                Duration::from_millis(50)
            )
            .unwrap_err(),
            HostCallError::Backpressured
        );
        assert_eq!(
            call(
                None,
                call_page(),
                roomy(),
                HUB_BYTES,
                Duration::from_millis(50)
            )
            .unwrap_err(),
            HostCallError::OwnerUnavailable
        );
    }
}
