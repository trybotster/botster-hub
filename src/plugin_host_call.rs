//! Plugin reads that the Hub owner answers.
//!
//! A plugin thread cannot read the owner's session projection, and no second
//! copy of it exists. It sends one request on the owner's control channel and
//! waits for the answer on a one-slot reply channel. The owner answers in one
//! bounded pass and never waits: a full control queue is a typed refusal for
//! the plugin, an unready projection is a typed reply, and a reply channel the
//! plugin gave up on is ignored.
//!
//! Every copy is funded from the plugin's callback account before it is made,
//! with ONE charge per call. The plugin funds the request, its strings and the
//! reply channel in a `LuaCallbackCharge` (the lease) and moves it into the
//! request. The owner grows that same lease by the bytes of the rows it copies
//! and the JSON copy the plugin makes from them, before it allocates any row,
//! then moves the lease into the reply. The lease therefore lives with the
//! request in the control queue, including after the plugin's deadline, and
//! with the reply until the plugin drops it.
//!
//! The reply channel's own storage lives until its LAST endpoint drops, and
//! either side can be last: the plugin's receiver, or the owner's sender,
//! which the owner drops just after it sends. So the channel bytes are split
//! off the lease into one `LuaCallbackStorageLease` (the existing shared-storage
//! contract: the charge releases after the last clone frees the `Arc`
//! allocation, which `lease_bytes` inside `single_reply_bytes` covers), held
//! by the plugin until after its receiver drops and by the request until after
//! the owner's sender drops.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use botster_hub_client::DaemonSessionEntity;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::owner_loop::DaemonControlState;
use crate::lua_memory::{
    LuaCallbackCharge, LuaCallbackGrowthError, LuaCallbackStorageLease, LuaMemoryAccount,
};
use crate::session_projection::SessionProjection;

/// Rows in one page: the family chunk count.
pub(crate) const SESSIONS_PAGE_ROWS: usize = 8;

/// Retained bytes of the JSON object a plugin builds from one row: sixteen
/// keys, each with its key string, value and map node, and generous slack.
const ROW_JSON_FIXED_BYTES: usize = 4096;

/// The bytes that fund one row's Rust copy and its JSON copy. The JSON copy
/// holds the row's strings again, and the local hub id. The plugin moves the
/// JSON values into its response; it makes no third copy.
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
pub(crate) enum HostOutcome {
    SessionsPage {
        rows: Vec<DaemonSessionEntity>,
        /// The last returned session id when more rows follow.
        next_after: Option<String>,
    },
    Session(Option<Box<DaemonSessionEntity>>),
    /// The projection has no complete baseline (start, baseline recovery).
    NotReady,
    /// The owner made no copy: the plugin's callback account cannot fund it.
    Refused(Refusal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The account has no room now; a later call may succeed.
    Capacity,
    /// The rows need more than one callback may hold; a retry cannot succeed.
    Quota,
}

/// The owner's answer. `lease` is the request's lease, grown by the rows.
#[derive(Debug)]
pub(crate) struct PluginHostReply {
    pub(crate) outcome: HostOutcome,
    pub(crate) lease: LuaCallbackCharge,
}

/// One call and where to answer it.
#[derive(Debug)]
pub(crate) struct PluginHostRequest {
    pub(crate) call: PluginHostCall,
    /// The calling plugin's account, for its limits.
    pub(crate) memory: Arc<LuaMemoryAccount>,
    /// Bytes of the local hub id the plugin adds to each row.
    pub(crate) hub_id_bytes: usize,
    pub(crate) reply: mpsc::SyncSender<PluginHostReply>,
    /// Funds the reply channel's storage. The plugin holds the other clone.
    /// Keep it after `reply`: it drops after the sender endpoint.
    pub(crate) channel: LuaCallbackStorageLease,
    /// Funds this request and its strings for as long as the request exists,
    /// including in the control queue after the plugin's deadline passed.
    pub(crate) lease: LuaCallbackCharge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostCallError {
    /// The Hub owner is not running, or stopped before it answered.
    OwnerUnavailable,
    /// The control queue is full.
    Backpressured,
    /// The owner did not answer before the deadline.
    TimedOut,
    /// The lease does not hold the channel's bytes.
    Unfunded,
}

/// Bytes the plugin funds before it reads its arguments or sends a call: the
/// control message, the request and the reply channel with its lease. It funds
/// each string the call owns by its length, before it copies the string.
pub(crate) fn fixed_call_bytes() -> Option<usize> {
    crate::lua_memory::layout::single_reply_bytes::<PluginHostReply>(true)?
        .checked_add(std::mem::size_of::<ControlMessage>())?
        .checked_add(std::mem::size_of::<PluginHostRequest>())
}

/// Split the reply channel's bytes off the lease into a shared storage lease.
pub(crate) fn split_channel(lease: &mut LuaCallbackCharge) -> Option<LuaCallbackStorageLease> {
    let bytes = crate::lua_memory::layout::single_reply_bytes::<PluginHostReply>(true)?;
    lease.split_fixed(bytes).map(LuaCallbackStorageLease::new)
}

/// Send one call to the owner and wait for its answer. Never called on the
/// owner thread. `lease` holds `fixed_call_bytes()` and the call's strings;
/// the rest of it moves into the request, so it stays charged until the owner
/// drops the request, even after this call returns on a timeout.
pub(crate) fn call(
    owner: Option<ControlSender>,
    call: PluginHostCall,
    memory: Arc<LuaMemoryAccount>,
    hub_id_bytes: usize,
    mut lease: LuaCallbackCharge,
    timeout: Duration,
) -> Result<PluginHostReply, HostCallError> {
    let Some(owner) = owner else {
        return Err(HostCallError::OwnerUnavailable);
    };
    let Some(channel) = split_channel(&mut lease) else {
        return Err(HostCallError::Unfunded);
    };
    // Declared before the receiver, so it drops after it.
    let (reply, receiver) = mpsc::sync_channel(1);
    owner
        .try_send(ControlMessage::PluginHostCall(Box::new(
            PluginHostRequest {
                call,
                memory,
                hub_id_bytes,
                reply,
                channel: channel.clone(),
                lease,
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
    answer(&state.maintenance.projection, request).deliver();
}

/// One answer and what the owner keeps until it has sent it.
pub(crate) struct Answered {
    pub(crate) reply: PluginHostReply,
    pub(crate) sender: mpsc::SyncSender<PluginHostReply>,
    pub(crate) channel: LuaCallbackStorageLease,
}

impl Answered {
    /// Send the reply, then drop the sender endpoint, then the channel charge.
    /// A plugin that stopped waiting dropped its receiver: the reply, and the
    /// lease it carries, drop with the failed send.
    pub(crate) fn deliver(self) {
        self.deliver_then(|| {});
    }

    /// `deliver`, with `after_send` run between the send and the two drops.
    /// A test uses it to stand in the interval where the plugin has already
    /// consumed the reply and the owner's sender is still live.
    pub(crate) fn deliver_then(self, after_send: impl FnOnce()) {
        let Self {
            reply,
            sender,
            channel,
        } = self;
        let _ = sender.try_send(reply);
        after_send();
        drop(sender);
        drop(channel);
    }
}

/// Grow the lease by the copies the reply will hold, or say why not. The lease
/// is the one parent of the call: its ceiling is the per-callback allowance.
fn fund(lease: &mut LuaCallbackCharge, bytes: usize) -> Result<(), Refusal> {
    lease.grow(bytes).map_err(|error| match error {
        LuaCallbackGrowthError::Capacity(_) => Refusal::Capacity,
        LuaCallbackGrowthError::Quota | LuaCallbackGrowthError::Sealed => Refusal::Quota,
    })
}

/// The reply for one request and the sender to answer on.
pub(crate) fn answer(projection: &SessionProjection, request: PluginHostRequest) -> Answered {
    let PluginHostRequest {
        call,
        memory,
        hub_id_bytes,
        reply,
        channel,
        mut lease,
    } = request;
    let outcome = outcome(
        projection,
        call,
        &memory,
        hub_id_bytes,
        &mut lease,
        channel.bytes(),
    );
    Answered {
        reply: PluginHostReply { outcome, lease },
        sender: reply,
        channel,
    }
}

fn outcome(
    projection: &SessionProjection,
    call: PluginHostCall,
    memory: &Arc<LuaMemoryAccount>,
    hub_id_bytes: usize,
    lease: &mut LuaCallbackCharge,
    channel_bytes: usize,
) -> HostOutcome {
    use std::ops::Bound;
    if !projection.baseline_complete {
        return HostOutcome::NotReady;
    }
    match call {
        PluginHostCall::SessionGet { session_id } => {
            let Some(row) = projection.rows.get(&session_id) else {
                return HostOutcome::Session(None);
            };
            let bytes = row_funding_bytes(
                SessionProjection::entity_bound_bytes(&row.record),
                hub_id_bytes,
            );
            match fund(lease, bytes) {
                Ok(()) => HostOutcome::Session(Some(Box::new(SessionProjection::project_entity(
                    &row.record,
                )))),
                Err(refusal) => HostOutcome::Refused(refusal),
            }
        }
        PluginHostCall::SessionsPage { after } => {
            let start = match after.as_deref() {
                Some(after) => Bound::Excluded(after),
                None => Bound::Unbounded,
            };
            // What the lease may still grow by within one callback: the
            // channel's bytes were split off the same parent.
            let remaining = memory
                .limits()
                .per_callback_bytes
                .saturating_sub(lease.bytes())
                .saturating_sub(channel_bytes);
            // Choose the rows first, without copying any, and stop at the
            // count or the callback ceiling.
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
                if count == SESSIONS_PAGE_ROWS || with_next > remaining {
                    if count == 0 {
                        return HostOutcome::Refused(Refusal::Quota);
                    }
                    more = true;
                    break;
                }
                bytes = with_next;
                chosen[count] = Some(&row.record);
                count += 1;
            }
            match fund(lease, bytes) {
                Ok(()) => {
                    let rows: Vec<DaemonSessionEntity> = chosen[..count]
                        .iter()
                        .flatten()
                        .map(|record| SessionProjection::project_entity(record))
                        .collect();
                    let next_after = more
                        .then(|| rows.last().map(|row| row.session_uuid.clone()))
                        .flatten();
                    HostOutcome::SessionsPage { rows, next_after }
                }
                Err(refusal) => HostOutcome::Refused(refusal),
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
    /// The bytes a test lease holds before the owner grows it.
    const FIXED: usize = 1000;

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

    /// Serve one call as the owner does: the request carries a lease of FIXED bytes.
    fn asked(
        projection: &SessionProjection,
        call: PluginHostCall,
        memory: &Arc<LuaMemoryAccount>,
    ) -> PluginHostReply {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let (channel, lease) = funded_call(memory);
        answer(
            projection,
            PluginHostRequest {
                call,
                memory: Arc::clone(memory),
                hub_id_bytes: HUB_BYTES,
                reply: sender,
                channel,
                lease,
            },
        )
        .reply
    }

    /// The bytes the reply channel takes.
    fn chan() -> usize {
        crate::lua_memory::layout::single_reply_bytes::<PluginHostReply>(true)
            .expect("channel bytes")
    }

    /// A funded call as the plugin makes it: FIXED bytes plus the channel's,
    /// with the channel split off.
    fn funded_call(memory: &Arc<LuaMemoryAccount>) -> (LuaCallbackStorageLease, LuaCallbackCharge) {
        let mut lease = memory
            .reserve_callback_total(FIXED + chan())
            .expect("the plugin funds its call");
        let channel = split_channel(&mut lease).expect("the channel bytes split off");
        (channel, lease)
    }

    fn page(
        projection: &SessionProjection,
        after: Option<&str>,
        memory: &Arc<LuaMemoryAccount>,
    ) -> PluginHostReply {
        asked(
            projection,
            PluginHostCall::SessionsPage {
                after: after.map(str::to_string),
            },
            memory,
        )
    }

    fn get(
        projection: &SessionProjection,
        id: &str,
        memory: &Arc<LuaMemoryAccount>,
    ) -> PluginHostReply {
        asked(
            projection,
            PluginHostCall::SessionGet {
                session_id: id.to_string(),
            },
            memory,
        )
    }

    #[test]
    fn an_unready_projection_is_a_typed_reply_never_an_empty_page() {
        let projection = SessionProjection::default();
        assert!(matches!(
            page(&projection, None, &roomy()).outcome,
            HostOutcome::NotReady
        ));
        assert!(matches!(
            get(&projection, "a", &roomy()).outcome,
            HostOutcome::NotReady
        ));
    }

    #[test]
    fn a_page_holds_the_chunk_count_and_its_cursor_comes_from_returned_rows() {
        let ids: Vec<String> = (0..SESSIONS_PAGE_ROWS + 3)
            .map(|index| format!("s{index:02}"))
            .collect();
        let projection = sealed(&ids.iter().map(String::as_str).collect::<Vec<_>>());
        let memory = roomy();
        let reply = page(&projection, None, &memory);
        let HostOutcome::SessionsPage { rows, next_after } = reply.outcome else {
            panic!("a page");
        };
        assert_eq!(rows.len(), SESSIONS_PAGE_ROWS);
        assert_eq!(next_after.as_deref(), Some("s07"));
        assert_eq!(
            reply.lease.bytes(),
            FIXED + SESSIONS_PAGE_ROWS * one_row_bytes(),
            "one lease holds the call and its rows"
        );
        let HostOutcome::SessionsPage { rows, next_after } =
            page(&projection, next_after.as_deref(), &memory).outcome
        else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(next_after, None);
    }

    #[test]
    fn a_get_of_an_unknown_session_is_none_and_grows_nothing() {
        let projection = sealed(&["a"]);
        let reply = get(&projection, "missing", &roomy());
        assert!(matches!(reply.outcome, HostOutcome::Session(None)));
        assert_eq!(reply.lease.bytes(), FIXED);
    }

    #[test]
    fn a_page_stops_at_the_callback_ceiling_and_its_cursor_is_the_last_returned_row() {
        let projection = sealed(&["s00", "s01", "s02", "s03", "s04"]);
        // The ceiling holds the call and three rows, not the chunk count.
        let memory = account(FIXED + chan() + 3 * one_row_bytes(), 64 * 1024 * 1024);
        let reply = page(&projection, None, &memory);
        let HostOutcome::SessionsPage { rows, next_after } = reply.outcome else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(next_after.as_deref(), Some("s02"));
        assert_eq!(reply.lease.bytes(), FIXED + 3 * one_row_bytes());
    }

    #[test]
    fn the_call_and_its_rows_share_one_callback_ceiling() {
        let projection = sealed(&["s00", "s01", "s02"]);
        // Two rows fit the ceiling alone, but not with the call's own bytes.
        let memory = account(FIXED + chan() + 2 * one_row_bytes() - 1, 64 * 1024 * 1024);
        let reply = page(&projection, None, &memory);
        let HostOutcome::SessionsPage { rows, next_after } = reply.outcome else {
            panic!("a page");
        };
        assert_eq!(rows.len(), 1, "the fixed bytes count against the ceiling");
        assert_eq!(next_after.as_deref(), Some("s00"));
        assert!(reply.lease.bytes() <= memory.limits().per_callback_bytes);
    }

    #[test]
    fn a_row_above_the_callback_ceiling_is_a_quota_refusal_and_grows_nothing() {
        let projection = sealed(&["s00"]);
        let memory = account(FIXED + chan() + one_row_bytes() - 1, 64 * 1024 * 1024);
        let reply = page(&projection, None, &memory);
        assert!(matches!(
            reply.outcome,
            HostOutcome::Refused(Refusal::Quota)
        ));
        assert_eq!(reply.lease.bytes(), FIXED);
        assert_eq!(memory.usage().1, FIXED);
    }

    #[test]
    fn an_exhausted_account_refuses_a_page_and_a_get_before_any_copy() {
        let projection = sealed(&["s00", "s01"]);
        let memory = account(
            FIXED + chan() + one_row_bytes(),
            FIXED + chan() + one_row_bytes(),
        );
        // Another callback holds one byte of the total: the row cannot be funded.
        let other = memory.reserve_callback_total(1).expect("other callback");
        let reply = page(&projection, None, &memory);
        assert!(matches!(
            reply.outcome,
            HostOutcome::Refused(Refusal::Capacity)
        ));
        assert_eq!(reply.lease.bytes(), FIXED);
        drop(reply);
        let reply = get(&projection, "s00", &memory);
        assert!(matches!(
            reply.outcome,
            HostOutcome::Refused(Refusal::Capacity)
        ));
        assert_eq!(
            memory.usage().1,
            FIXED + 1,
            "a refusal charges nothing more"
        );
        drop(reply);
        drop(other);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn dropping_a_reply_releases_its_lease() {
        let projection = sealed(&["s00", "s01"]);
        let memory = roomy();
        let reply = page(&projection, None, &memory);
        assert_eq!(memory.usage().1, FIXED + 2 * one_row_bytes());
        drop(reply);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn a_closed_reply_channel_is_ignored_and_a_full_queue_is_typed() {
        let memory = roomy();
        let (sender, _keep) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::HostProgressPublished)
            .expect("the one slot is free");
        let call_page = || PluginHostCall::SessionsPage { after: None };
        let lease = || {
            memory
                .reserve_callback_total(FIXED + chan())
                .expect("lease")
        };
        assert_eq!(
            call(
                Some(sender),
                call_page(),
                Arc::clone(&memory),
                HUB_BYTES,
                lease(),
                Duration::from_millis(50)
            )
            .unwrap_err(),
            HostCallError::Backpressured
        );
        assert_eq!(
            call(
                None,
                call_page(),
                Arc::clone(&memory),
                HUB_BYTES,
                lease(),
                Duration::from_millis(50)
            )
            .unwrap_err(),
            HostCallError::OwnerUnavailable
        );
        assert_eq!(memory.usage().1, 0, "a failed send returns the lease");
    }

    /// The plugin's endpoint, its channel charge and its reply lease are gone,
    /// but the owner's sender is still live: the channel's bytes stay charged.
    #[test]
    fn the_channel_charge_outlives_the_plugins_endpoint_and_the_reply() {
        let memory = roomy();
        let projection = sealed(&["s00"]);
        let (sender, mut queue) = tokio::sync::mpsc::channel(1);
        let (sent_tx, sent_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let owner = std::thread::spawn(move || {
            let message = queue.blocking_recv().expect("the request");
            let ControlMessage::PluginHostCall(request) = message else {
                panic!("a plugin host call");
            };
            answer(&projection, *request).deliver_then(|| {
                // The owner is past the send and still holds the sender
                // endpoint and its channel charge.
                sent_tx.send(()).expect("signal");
                go_rx.recv().expect("go");
            });
        });
        let (_, lease) = {
            // The plugin funds its call; `call` splits the channel off itself.
            let lease = memory
                .reserve_callback_total(FIXED + chan())
                .expect("the plugin funds its call");
            ((), lease)
        };
        let reply = call(
            Some(sender),
            PluginHostCall::SessionsPage { after: None },
            Arc::clone(&memory),
            HUB_BYTES,
            lease,
            Duration::from_secs(5),
        )
        .expect("an answer");
        assert!(matches!(reply.outcome, HostOutcome::SessionsPage { .. }));
        // The plugin drops the reply, and with it the lease. `call` already
        // dropped its receiver and its channel charge.
        drop(reply);
        sent_rx.recv().expect("the owner sent");
        assert_eq!(
            memory.usage().1,
            chan(),
            "the owner's sender is live, so its channel bytes are still charged"
        );
        go_tx.send(()).expect("go");
        owner.join().expect("the owner thread");
        assert_eq!(memory.usage().1, 0);
    }
}
