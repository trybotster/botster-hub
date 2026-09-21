//! Bounded background work for the Hub owner loop.
//!
//! Domain code records exact, coalesced wake bits. The owner moves each bit
//! into its one central ready queue. This module contains no selection cursor.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::SessionId;
use botster_core::{
    BoundaryJson, PluginAdmissionResult, PluginCompletion, PluginHandlerKind, PluginHandlerRef,
    PluginInvocationClass, PluginInvocationContext, PluginInvocationFailureKind,
    PluginInvocationRequest, PluginInvocationResult, RequestId,
};
use botster_core_daemon::{
    LifecycleBaselineBudget, ObserveLifecycleBudget, ObserveLifecycleCursor, ObserveLifecycleSlice,
    SessionLifecycleBaselinePage, SessionLifecycleChange, SessionLifecycleCursor,
    SessionLifecyclePage, SessionLifecyclePageError, SessionLifecycleResyncReason,
};

use crate::HubRuntime;
use crate::data_plane::driver::{CoreTicket, CoreTicketPoll};
use crate::lifecycle::{PACKAGE_EVENT_INVOCATION_ORIGIN, SESSION_FAMILY_INVOCATION_ORIGIN};
use crate::session_projection::SessionProjection;

/// Published owner-turn budget after isolated-path measurement.
pub const MAX_OWNER_TURN_MS: u64 = 25;
/// Published ready-operation wait budget through the production owner loop.
pub const MAX_READY_OPERATION_WAIT_MS: u64 = 50;

/// Observe slice used by the production owner loop.
pub const OBSERVE_SLICE_BUDGET: ObserveLifecycleBudget = ObserveLifecycleBudget {
    max_sessions: 8,
    max_encoded_result_bytes: 64 * 1024,
    max_elapsed: Duration::from_millis(8),
};

/// Baseline page used by the production owner loop.
pub const BASELINE_PAGE_BUDGET: LifecycleBaselineBudget = LifecycleBaselineBudget {
    max_rows: 16,
    max_bytes: 64 * 1024,
    max_elapsed: Duration::from_millis(8),
};

const JOURNAL_PAGE_MAX_CHANGES: usize = 16;
const JOURNAL_PAGE_MAX_BYTES: usize = 64 * 1024;
const APPLY_MAX_CHANGES: usize = 16;
/// Plugin event handler invocation bound for maintenance-driven deliveries.
const EVENT_INVOCATION_TIMEOUT_MS: u64 = 1_000;
const SESSION_CHUNK_MAX_ITEMS: usize = 8;
const SESSION_CHUNK_MAX_BYTES: usize = 32 * 1024;
const COMPLETION_DRAIN_MAX_ITEMS: usize = 8;
const FAMILY_QUEUE_MAX_ITEMS: usize = 32;
const FAMILY_QUEUE_MAX_BYTES: usize = 64 * 1024;
const FANOUT_QUEUE_MAX_ITEMS: usize = 32;
const FANOUT_QUEUE_MAX_BYTES: usize = 64 * 1024;
const HOST_BRIDGE_MAX_BYTES: usize = 64 * 1024;
const CONSUMER_REFRESH_MAX: usize = 8;
/// CloseEvents visits this many admissions per Pump slice.
#[allow(dead_code)]
pub const PUMP_MAX_ADMISSIONS_VISITED: usize = 8;
/// CloseEvents classifies this many closed-route candidates per Pump slice.
#[allow(dead_code)]
pub const PUMP_MAX_CANDIDATE_CLASSIFICATIONS: usize = 8;
/// CloseEvents visits this many mux route entries per Pump slice, including
/// open and already-reported rows.
#[allow(dead_code)]
pub const PUMP_MAX_ROUTE_ENTRIES_VISITED: usize = 8;
/// InventoryReconcile visits this many stream-map entries per Pump slice,
/// including unbound rows.
pub const PUMP_MAX_ROUTES_VALIDATED: usize = 8;

fn queued_queue_bytes(frames: &VecDeque<serde_json::Value>) -> usize {
    frames
        .iter()
        .map(|frame| {
            serde_json::to_vec(frame)
                .map(|body| body.len())
                .unwrap_or(0)
        })
        .sum()
}

/// Round-robin maintenance kinds. One owner turn runs one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceSliceKind {
    Observe,
    JournalPull,
    ProjectionApply,
    Baseline,
    HostBridge,
    SubscriberDelivery,
    CompletionDrain,
    ProviderResync,
    PackageEventDelivery,
}

impl MaintenanceSliceKind {
    pub const ALL: [Self; 9] = [
        Self::Observe,
        Self::JournalPull,
        Self::ProjectionApply,
        Self::Baseline,
        Self::HostBridge,
        Self::SubscriberDelivery,
        Self::CompletionDrain,
        Self::ProviderResync,
        Self::PackageEventDelivery,
    ];

    const fn bit(self) -> u16 {
        match self {
            Self::Observe => 1 << 0,
            Self::JournalPull => 1 << 1,
            Self::ProjectionApply => 1 << 2,
            Self::Baseline => 1 << 3,
            Self::HostBridge => 1 << 4,
            Self::SubscriberDelivery => 1 << 5,
            Self::CompletionDrain => 1 << 6,
            Self::ProviderResync => 1 << 7,
            Self::PackageEventDelivery => 1 << 8,
        }
    }
}

/// Exact coalesced wake bits for maintenance work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceWakes(u16);

impl Default for MaintenanceWakes {
    fn default() -> Self {
        let mut wakes = Self(0);
        wakes.mark_all();
        wakes
    }
}

impl MaintenanceWakes {
    pub fn mark(&mut self, kind: MaintenanceSliceKind) {
        self.0 |= kind.bit();
    }

    pub fn mark_all(&mut self) {
        for kind in MaintenanceSliceKind::ALL {
            self.mark(kind);
        }
    }

    pub fn take(&mut self, kind: MaintenanceSliceKind) -> bool {
        let present = self.0 & kind.bit() != 0;
        self.0 &= !kind.bit();
        present
    }

    #[must_use]
    pub const fn has_any(self) -> bool {
        self.0 != 0
    }
}

/// Resume cursor over Unix then WebRTC admission maps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PumpAdmissionCursor {
    Unix {
        after: Option<String>,
        after_route: Option<(String, String, u64)>,
    },
    #[allow(dead_code)]
    Webrtc {
        after: Option<String>,
        after_route: Option<(String, String, u64)>,
    },
}

impl Default for PumpAdmissionCursor {
    fn default() -> Self {
        Self::Unix {
            after: None,
            after_route: None,
        }
    }
}

/// Continuation state for causal inventory work.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PumpState {
    pub close_cursor: PumpAdmissionCursor,
    pub reconcile_after: Option<(String, String)>,
    inventory_reconcile_again: bool,
}

impl PumpState {
    pub(crate) fn note_inventory_change_during_reconcile(&mut self, active: bool) {
        self.inventory_reconcile_again |= active;
    }

    pub(crate) fn take_inventory_reconcile_again(&mut self) -> bool {
        std::mem::take(&mut self.inventory_reconcile_again)
    }
}

/// In-progress paged baseline recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineRecovery {
    snapshot: Option<SessionLifecycleCursor>,
    after: Option<SessionId>,
}

/// One in-flight session-family frame waiting for a matching completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FamilyFrameKind {
    Begin,
    Chunk,
    End,
    Delta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InFlightSessionFamily {
    request_id: RequestId,
    snapshot_sequence: u64,
    kind: FamilyFrameKind,
}

fn family_frame_kind(payload: &serde_json::Value) -> FamilyFrameKind {
    match payload.get("type").and_then(serde_json::Value::as_str) {
        Some("snapshot_begin") => FamilyFrameKind::Begin,
        Some("snapshot_chunk") => FamilyFrameKind::Chunk,
        Some("snapshot_end") => FamilyFrameKind::End,
        _ => FamilyFrameKind::Delta,
    }
}

/// Pending session-family work for one plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionFamilyConsumer {
    plugin_key: String,
    handler: Option<PluginHandlerRef>,
    snapshot_sequence: u64,
    in_flight: Option<InFlightSessionFamily>,
    pending: VecDeque<serde_json::Value>,
    held_deltas: VecDeque<serde_json::Value>,
    snapshot_complete: bool,
    gap: bool,
    need_snapshot_chunks: bool,
    snapshot_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FanoutJob {
    frame: serde_json::Value,
    after: Option<String>,
    bytes: usize,
}

struct HostBridgeBudget {
    started: Instant,
    visits: usize,
    bytes: usize,
}

impl HostBridgeBudget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            visits: 0,
            bytes: 0,
        }
    }

    fn remaining_visits(&self) -> usize {
        CONSUMER_REFRESH_MAX.saturating_sub(self.visits)
    }

    fn take(&mut self) -> bool {
        if self.exhausted() || self.visits >= CONSUMER_REFRESH_MAX {
            return false;
        }
        self.visits = self.visits.saturating_add(1);
        true
    }

    fn add_bytes(&mut self, extra_bytes: usize) -> bool {
        if self.exhausted() || self.bytes.saturating_add(extra_bytes) > HOST_BRIDGE_MAX_BYTES {
            return false;
        }
        self.bytes = self.bytes.saturating_add(extra_bytes);
        true
    }

    fn exhausted(&self) -> bool {
        self.visits >= CONSUMER_REFRESH_MAX
            || self.bytes >= HOST_BRIDGE_MAX_BYTES
            || self.started.elapsed() >= Duration::from_millis(MAX_OWNER_TURN_MS)
    }
}

/// Hub-owned `/session` delivery with one in-flight frame per plugin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionFamilyBridge {
    consumers: BTreeMap<String, SessionFamilyConsumer>,
    next_request_serial: u64,
    refresh_after: Option<String>,
    refresh_seen: BTreeSet<String>,
    in_flight_by_request: BTreeMap<String, String>,
    pending_fanout: VecDeque<FanoutJob>,
    fanout_bytes: usize,
    gap_after: Option<String>,
    need_gap_pass: bool,
    snapshot_start_after: Option<String>,
    snapshot_start_sequence: Option<u64>,
    admit_after: Option<String>,
    prune_after: Option<String>,
    need_prune: bool,
    busy_count: u32,
}

fn consumer_busy(consumer: &SessionFamilyConsumer) -> bool {
    consumer.in_flight.is_some()
        || !consumer.pending.is_empty()
        || !consumer.held_deltas.is_empty()
        || consumer.need_snapshot_chunks
}

impl SessionFamilyBridge {
    fn has_work(&self) -> bool {
        self.busy_count > 0
            || !self.pending_fanout.is_empty()
            || self.need_gap_pass
            || self.snapshot_start_sequence.is_some()
            || self.need_prune
            || self.refresh_after.is_some()
    }

    fn adjust_busy(&mut self, was_busy: bool, now_busy: bool) {
        if now_busy && !was_busy {
            self.busy_count = self.busy_count.saturating_add(1);
        } else if !now_busy && was_busy {
            self.busy_count = self.busy_count.saturating_sub(1);
        }
    }

    fn consumer_mut(&mut self, plugin_key: &str) -> &mut SessionFamilyConsumer {
        self.consumers
            .entry(plugin_key.to_string())
            .or_insert_with(|| SessionFamilyConsumer {
                plugin_key: plugin_key.to_string(),
                handler: None,
                snapshot_sequence: 0,
                in_flight: None,
                pending: VecDeque::new(),
                held_deltas: VecDeque::new(),
                snapshot_complete: false,
                gap: true,
                need_snapshot_chunks: false,
                snapshot_after: None,
            })
    }

    fn mark_gap(&mut self, plugin_key: &str) {
        let request_id = self
            .consumers
            .get(plugin_key)
            .and_then(|consumer| consumer.in_flight.as_ref())
            .map(|flight| flight.request_id.clone());
        if let Some(request_id) = request_id {
            self.in_flight_by_request.remove(&request_id.0);
        }
        self.touch_consumer(plugin_key, |consumer| {
            consumer.gap = true;
            consumer.snapshot_complete = false;
            consumer.need_snapshot_chunks = false;
            consumer.snapshot_after = None;
            consumer.pending.clear();
            consumer.held_deltas.clear();
            consumer.in_flight = None;
        });
    }

    fn begin_snapshot(&mut self, plugin_key: &str, sequence: u64) {
        self.touch_consumer(plugin_key, |consumer| {
            consumer.snapshot_sequence = sequence;
            consumer.snapshot_complete = false;
            consumer.gap = false;
            consumer.need_snapshot_chunks = true;
            consumer.snapshot_after = None;
            consumer.pending.clear();
            consumer.held_deltas.clear();
            consumer.pending.push_back(serde_json::json!({
                "type": "snapshot_begin",
                "family": "/session",
                "snapshot_sequence": sequence,
            }));
        });
    }

    #[cfg(test)]
    fn queue_snapshot(&mut self, plugin_key: &str, sequence: u64, items: &[serde_json::Value]) {
        self.begin_snapshot(plugin_key, sequence);
        let consumer = self.consumer_mut(plugin_key);
        let mut start = 0;
        while start < items.len() {
            match pack_session_chunk(items, start) {
                Ok((chunk, next)) => {
                    consumer
                        .pending
                        .push_back(session_chunk_frame(sequence, &chunk));
                    start = next;
                }
                Err(()) => {
                    consumer.gap = true;
                    consumer.need_snapshot_chunks = false;
                    consumer.pending.clear();
                    return;
                }
            }
        }
        consumer.need_snapshot_chunks = false;
        consumer
            .pending
            .push_back(session_end_frame(sequence, true));
    }

    fn queue_delta(&mut self, plugin_key: &str, frame: serde_json::Value) -> bool {
        self.touch_consumer(plugin_key, |consumer| {
            if consumer.gap {
                return true;
            }
            let bytes = serde_json::to_vec(&frame)
                .map(|body| body.len())
                .unwrap_or(0);
            let queued_items = consumer.pending.len() + consumer.held_deltas.len();
            let queued_bytes =
                queued_queue_bytes(&consumer.pending) + queued_queue_bytes(&consumer.held_deltas);
            if queued_items >= FAMILY_QUEUE_MAX_ITEMS
                || queued_bytes.saturating_add(bytes) > FAMILY_QUEUE_MAX_BYTES
            {
                return false;
            }
            if !consumer.snapshot_complete {
                consumer.held_deltas.push_back(frame);
            } else {
                consumer.pending.push_back(frame);
            }
            true
        })
    }

    fn touch_consumer<R>(
        &mut self,
        plugin_key: &str,
        f: impl FnOnce(&mut SessionFamilyConsumer) -> R,
    ) -> R {
        let was_busy = self.consumers.get(plugin_key).is_some_and(consumer_busy);
        let result = f(self.consumer_mut(plugin_key));
        let now_busy = self.consumers.get(plugin_key).is_some_and(consumer_busy);
        self.adjust_busy(was_busy, now_busy);
        result
    }

    fn touch_existing_consumer(
        &mut self,
        plugin_key: &str,
        f: impl FnOnce(&mut SessionFamilyConsumer),
    ) {
        let was_busy = self.consumers.get(plugin_key).is_some_and(consumer_busy);
        let now_busy = {
            let Some(consumer) = self.consumers.get_mut(plugin_key) else {
                return;
            };
            f(consumer);
            consumer_busy(consumer)
        };
        self.adjust_busy(was_busy, now_busy);
    }

    fn next_request_id(&mut self, plugin_key: &str, sequence: u64) -> RequestId {
        self.next_request_serial = self.next_request_serial.saturating_add(1);
        RequestId(format!(
            "session-family-{plugin_key}-{sequence}-{}",
            self.next_request_serial
        ))
    }
}

/// Owner-loop maintenance state. Independent of subscriber count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaintenanceState {
    pub wakes: MaintenanceWakes,
    pub projection: SessionProjection,
    pub observe_resume: Option<ObserveLifecycleCursor>,
    pub pending_changes: VecDeque<SessionLifecycleChange>,
    pub baseline: Option<BaselineRecovery>,
    /// Latest journal source watermark observed by a successful pull.
    pub journal_source_watermark: Option<SessionLifecycleCursor>,
    /// True after a successful pull that returned no changes at the watermark.
    pub journal_caught_up_confirmed: bool,
    /// Pending session ids whose Spawn reply this Hub process already returned.
    /// The first snapshot waits until the projection contains every pending id.
    /// After the projection observes a pending id, Hub retires it.
    pub acknowledged_spawn_ids: BTreeSet<String>,
    /// Latest watermark for which omitted-row recover already ran once.
    omitted_row_recover_at: Option<SessionLifecycleCursor>,
    pub session_family: SessionFamilyBridge,
    pub last_owner_turn: Duration,
    pub journal_page_reads: u64,
    pub baseline_page_reads: u64,
    /// Times Core returned `resync_required` and Hub reminted a baseline.
    pub resync_reads: u64,
    pub projection_dirty: bool,
    pub event_in_flight: BTreeMap<String, EventDeliveryFlight>,
    pub pending_retirements: VecDeque<EventDeliveryFlight>,
    pub event_causal_blocked: bool,
    pub event_causal_faulted: bool,
    /// Core reported a lifecycle-journal advance since the last consumer read it.
    pub journal_wake_pending: bool,
}

/// Core lifecycle reads the maintenance slices have in flight.
///
/// Each slice requests one read on one owner turn and applies it on a later
/// turn; the owner never waits for Core. Held outside [`MaintenanceState`]
/// because tickets are neither clonable nor comparable.
#[derive(Debug, Default)]
pub struct MaintenanceCoreReads {
    observe: Option<CoreTicket<Result<ObserveLifecycleSlice, SessionLifecyclePageError>>>,
    /// Journal page read plus the journal-advanced flag observed when it started.
    journal: Option<(
        CoreTicket<Result<SessionLifecyclePage, SessionLifecyclePageError>>,
        bool,
    )>,
    baseline: Option<CoreTicket<Result<SessionLifecycleBaselinePage, SessionLifecyclePageError>>>,
}

impl MaintenanceCoreReads {
    /// True while any lifecycle read waits on Core.
    #[must_use]
    pub fn in_flight(&self) -> bool {
        self.observe.is_some() || self.journal.is_some() || self.baseline.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDeliveryFlight {
    pub envelope_id: u64,
    pub plugin_key: String,
    pub generation: u64,
    pub scope_id: Option<u64>,
    pub request_id: String,
    pub holder_retired: bool,
    pub pull_id: Option<u64>,
}

fn package_event_request_id(delivery: &crate::package_event_router::ReadyDelivery) -> RequestId {
    RequestId(format!(
        "package-event-{}-{}-{}-{}",
        delivery.owner,
        delivery.name,
        delivery.envelope_id,
        delivery.pull_id()
    ))
}

fn event_flight(
    delivery: &crate::package_event_router::ReadyDelivery,
    scope_id: Option<u64>,
    request_id: String,
) -> EventDeliveryFlight {
    EventDeliveryFlight {
        envelope_id: delivery.envelope_id,
        plugin_key: delivery.holder.plugin_key.clone(),
        generation: delivery.holder.generation,
        scope_id,
        request_id,
        holder_retired: false,
        pull_id: Some(delivery.pull_id()),
    }
}

impl MaintenanceState {
    pub(crate) fn note_causal_capacity_progress(&mut self) {
        if self.event_causal_blocked && !self.event_causal_faulted {
            self.event_causal_blocked = false;
            self.wakes.mark(MaintenanceSliceKind::PackageEventDelivery);
        }
    }

    /// Coalesce one O(1) wake after an authoritative mutation.
    pub fn try_wake(&mut self) {
        self.wakes.mark_all();
    }

    /// Record Core's coalesced journal-advanced wake for the next consumer.
    pub fn note_journal_advanced(&mut self) {
        self.journal_wake_pending = true;
    }

    /// Take the journal-advanced wake. Mirrors Core's coalesced bit.
    pub fn take_journal_wake(&mut self) -> bool {
        std::mem::take(&mut self.journal_wake_pending)
    }

    /// After an authoritative mutation, pull the journal on the next idle turn.
    pub fn note_authoritative_mutation(&mut self) {
        self.journal_caught_up_confirmed = false;
        self.wakes.mark(MaintenanceSliceKind::JournalPull);
    }

    pub fn needs_work(&self) -> bool {
        // `observe_resume` is continuation state for the next Observe kind.
        // It must not rearm the whole nine-kind rotation as idle wakes.
        self.wakes.has_any()
            || self.baseline.is_some()
            || !self.pending_changes.is_empty()
            || self.session_family.has_work()
            || (!self.pending_retirements.is_empty()
                && !self.event_causal_blocked
                && !self.event_causal_faulted)
    }

    /// True when the canonical session projection has consumed the latest
    /// observed journal watermark. First session snapshots wait for this.
    ///
    /// The comparison uses the latest stored watermark, not a subscribe-time
    /// snapshot. Pulls observe that watermark and outpace single-row mutations
    /// 16:1, so a stable fixture set terminates.
    #[must_use]
    pub fn projection_caught_up(&self) -> bool {
        if !self.projection.baseline_complete || self.projection.gap || self.baseline.is_some() {
            return false;
        }
        if !self.pending_changes.is_empty() {
            return false;
        }
        let Some(cursor) = self.projection.cursor.as_ref() else {
            return false;
        };
        let Some(watermark) = self.journal_source_watermark.as_ref() else {
            return false;
        };
        if cursor.source_id != watermark.source_id {
            return false;
        }
        if !self.journal_caught_up_confirmed || cursor.sequence < watermark.sequence {
            return false;
        }
        self.acknowledged_spawn_ids
            .iter()
            .all(|session_id| self.projection.rows.contains_key(session_id))
    }
}

/// Copy process-level Spawn acknowledgements onto owner-loop state.
///
/// Union, do not replace. A replace would wipe control-path inserts when the
/// runtime set is still empty and make an empty set vacuously caught-up.
pub fn sync_acknowledged_spawns(runtime: &HubRuntime, state: &mut MaintenanceState) {
    state
        .acknowledged_spawn_ids
        .extend(runtime.acknowledged_spawn_ids());
}

fn retire_projected_spawn_acks(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let projected: Vec<String> = state
        .acknowledged_spawn_ids
        .iter()
        .filter(|session_id| state.projection.rows.contains_key(*session_id))
        .cloned()
        .collect();
    for session_id in projected {
        state.acknowledged_spawn_ids.remove(&session_id);
        runtime.retire_acknowledged_spawn(&session_id);
    }
}

fn acknowledged_spawns_missing_from_projection(state: &MaintenanceState) -> bool {
    !state.acknowledged_spawn_ids.is_empty()
        && state
            .acknowledged_spawn_ids
            .iter()
            .any(|session_id| !state.projection.rows.contains_key(session_id))
}

fn journal_consume_is_at_watermark(state: &MaintenanceState) -> bool {
    if state.journal_caught_up_confirmed {
        return true;
    }
    match (
        state.projection.cursor.as_ref(),
        state.journal_source_watermark.as_ref(),
    ) {
        (Some(cursor), Some(watermark)) => {
            cursor.source_id == watermark.source_id && cursor.sequence >= watermark.sequence
        }
        _ => false,
    }
}

fn rewind_journal_cursor_for_omitted_recover(state: &mut MaintenanceState) {
    if let Some(cursor) = state.projection.cursor.as_mut() {
        cursor.sequence = 0;
    }
    state.journal_caught_up_confirmed = false;
    state.wakes.mark(MaintenanceSliceKind::JournalPull);
}

fn omitted_row_recover_key(state: &MaintenanceState) -> Option<SessionLifecycleCursor> {
    if let Some(watermark) = state.journal_source_watermark.clone() {
        return Some(watermark);
    }
    state
        .projection
        .cursor
        .as_ref()
        .map(|cursor| SessionLifecycleCursor {
            source_id: cursor.source_id.clone(),
            sequence: 0,
        })
}

fn start_omitted_row_recover(state: &mut MaintenanceState) -> bool {
    let Some(key) = omitted_row_recover_key(state) else {
        return false;
    };
    if state.omitted_row_recover_at.as_ref() == Some(&key) {
        return false;
    }
    state.omitted_row_recover_at = Some(key);
    rewind_journal_cursor_for_omitted_recover(state);
    true
}

/// Hold first-snapshot completion until pending Spawn ids are projected.
///
/// A freeze minted at the current watermark can omit live registry rows whose
/// journal sequences are already at or below that watermark. One bounded
/// recover may probe the journal from sequence 0. If that cursor expired,
/// Core lost prefix history and Hub remints a fresh baseline. This path does
/// not call Core `list()` and does not replace Observe.
pub fn refresh_projection_if_inventory_ahead(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
) -> bool {
    sync_acknowledged_spawns(runtime, state);
    retire_projected_spawn_acks(runtime, state);
    if acknowledged_spawns_missing_from_projection(state) && journal_consume_is_at_watermark(state)
    {
        start_omitted_row_recover(state);
        return false;
    }
    state.projection_caught_up()
}

fn journal_caught_up_after_pull(
    received: bool,
    at_watermark: bool,
    journal_advanced: bool,
) -> bool {
    !received && at_watermark && !journal_advanced
}

fn session_chunk_frame(sequence: u64, items: &[serde_json::Value]) -> serde_json::Value {
    serde_json::json!({
        "type": "snapshot_chunk",
        "family": "/session",
        "snapshot_sequence": sequence,
        "items": items,
    })
}

fn session_end_frame(sequence: u64, complete: bool) -> serde_json::Value {
    serde_json::json!({
        "type": "snapshot_end",
        "family": "/session",
        "snapshot_sequence": sequence,
        "complete": complete,
    })
}

/// Pack one snapshot chunk without dropping later rows.
///
/// `Err(())` means the item at `start` cannot fit the byte budget.
pub(crate) fn pack_session_chunk(
    items: &[serde_json::Value],
    start: usize,
) -> Result<(Vec<serde_json::Value>, usize), ()> {
    if start >= items.len() {
        return Ok((Vec::new(), start));
    }
    let mut packed = Vec::new();
    let mut index = start;
    while index < items.len() && packed.len() < SESSION_CHUNK_MAX_ITEMS {
        packed.push(items[index].clone());
        let encoded = session_chunk_frame(0, &packed);
        let bytes = serde_json::to_vec(&encoded)
            .map(|body| body.len())
            .unwrap_or(usize::MAX);
        if bytes > SESSION_CHUNK_MAX_BYTES {
            packed.pop();
            if packed.is_empty() {
                return Err(());
            }
            break;
        }
        index += 1;
    }
    Ok((packed, index))
}

/// Run one runtime-owned maintenance slice through the production Core facades.
pub fn run_maintenance_kind(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    kind: MaintenanceSliceKind,
) {
    run_maintenance_kind_with_owner(runtime, state, reads, kind, None);
}

/// Run one maintenance slice whose Core read publishes to the Hub owner.
pub(crate) fn run_maintenance_kind_for_owner(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    kind: MaintenanceSliceKind,
    waiter_id: crate::owner_identity::WaiterId,
) {
    run_maintenance_kind_with_owner(runtime, state, reads, kind, Some(waiter_id));
}

fn run_maintenance_kind_with_owner(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    kind: MaintenanceSliceKind,
    waiter_id: Option<crate::owner_identity::WaiterId>,
) {
    match kind {
        MaintenanceSliceKind::Observe => run_observe_slice(runtime, state, reads, waiter_id),
        MaintenanceSliceKind::JournalPull => {
            run_journal_pull_slice(runtime, state, reads, waiter_id)
        }
        MaintenanceSliceKind::ProjectionApply => run_projection_apply_slice(Some(runtime), state),
        MaintenanceSliceKind::Baseline => run_baseline_slice(runtime, state, reads, waiter_id),
        MaintenanceSliceKind::HostBridge => run_host_bridge_slice(runtime, state),
        MaintenanceSliceKind::CompletionDrain => run_completion_drain_slice(runtime, state),
        MaintenanceSliceKind::PackageEventDelivery => {
            run_package_event_delivery_slice(runtime, state)
        }
        MaintenanceSliceKind::SubscriberDelivery | MaintenanceSliceKind::ProviderResync => {}
    }
}

/// Test helper: run one slice kind and follow its Core reads to completion.
///
/// Tests drive slices from their own thread, not the owner loop, so each
/// call may wait for the data-plane thread. Reads live in a thread-local so
/// call sites keep the production signature minus the reads argument.
#[cfg(test)]
pub(crate) fn run_maintenance_kind_to_completion(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    kind: MaintenanceSliceKind,
) {
    thread_local! {
        static READS: std::cell::RefCell<MaintenanceCoreReads> =
            std::cell::RefCell::new(MaintenanceCoreReads::default());
    }
    READS.with(|reads| {
        let mut reads = reads.borrow_mut();
        let deadline = Instant::now() + Duration::from_secs(30);
        run_maintenance_kind(runtime, state, &mut reads, kind);
        while reads.in_flight() {
            assert!(
                Instant::now() < deadline,
                "maintenance slice {kind:?} did not complete its Core read"
            );
            std::thread::sleep(Duration::from_millis(1));
            run_maintenance_kind(runtime, state, &mut reads, kind);
        }
    });
}

fn handle_unavailable_observe_pass(state: &mut MaintenanceState) {
    state.observe_resume = None;
    if !state.projection.baseline_complete || state.baseline.is_some() {
        start_baseline_recovery(state);
    }
}

/// Apply a successful observe slice to Maintenance scheduling.
///
/// An incomplete pass stores [`MaintenanceState::observe_resume`] and leaves
/// the nine-kind pointer unchanged. A journal-advanced wake prefers
/// JournalPull. This function is the only incomplete-pass scheduler
/// transition in production.
fn apply_observe_pass_result(
    state: &mut MaintenanceState,
    complete: bool,
    journal_advanced: bool,
    resume: Option<ObserveLifecycleCursor>,
) {
    state.observe_resume = if complete { None } else { resume };
    if journal_advanced {
        state.wakes.mark(MaintenanceSliceKind::JournalPull);
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn run_observe_slice(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    waiter_id: Option<crate::owner_identity::WaiterId>,
) {
    let Some(ticket) = reads.observe.as_mut() else {
        let resume = state.observe_resume.clone();
        reads.observe = Some(match waiter_id {
            Some(waiter_id) => runtime.observe_lifecycle_slice_for_owner(
                waiter_id,
                now_seconds(),
                resume.as_ref(),
                OBSERVE_SLICE_BUDGET,
            ),
            None => runtime.observe_lifecycle_slice(
                now_seconds(),
                resume.as_ref(),
                OBSERVE_SLICE_BUDGET,
            ),
        });
        return;
    };
    let result = match ticket.poll() {
        CoreTicketPoll::Pending => return,
        // Refused: the single slot is freed and the next slice resubmits.
        CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
            reads.observe = None;
            return;
        }
        CoreTicketPoll::Ready(result) => result,
    };
    reads.observe = None;
    match result {
        Ok(slice) => {
            if let Some(reason) = slice.resync_required {
                if matches!(reason, SessionLifecycleResyncReason::SourceChanged) {
                    state.observe_resume = None;
                    start_baseline_recovery(state);
                } else if matches!(reason, SessionLifecycleResyncReason::ObservePassUnavailable) {
                    handle_unavailable_observe_pass(state);
                }
                return;
            }
            let journal_advanced = state.take_journal_wake();
            apply_observe_pass_result(
                state,
                slice.complete,
                journal_advanced,
                if slice.complete {
                    None
                } else {
                    Some(ObserveLifecycleCursor {
                        pass_id: slice.pass_id,
                        last_visited: slice.last_visited,
                    })
                },
            );
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.wakes.mark_all();
        }
        Err(_) => state.wakes.mark_all(),
    }
}

fn run_journal_pull_slice(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    waiter_id: Option<crate::owner_identity::WaiterId>,
) {
    let (result, woke) = match reads.journal.as_mut() {
        Some((ticket, woke)) => match ticket.poll() {
            CoreTicketPoll::Pending => return,
            CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                reads.journal = None;
                return;
            }
            CoreTicketPoll::Ready(result) => {
                let woke = *woke;
                reads.journal = None;
                (result, woke)
            }
        },
        None => {
            let woke = state.take_journal_wake();
            if state.baseline.is_some() || !state.projection.baseline_complete {
                if woke && state.baseline.is_none() && !state.session_family.need_gap_pass {
                    start_baseline_recovery(state);
                }
                return;
            }
            let Some(cursor) = state.projection.cursor.clone() else {
                if woke {
                    start_baseline_recovery(state);
                }
                return;
            };
            let ticket = match waiter_id {
                Some(waiter_id) => runtime.lifecycle_changes_page_for_owner(
                    waiter_id,
                    &cursor,
                    JOURNAL_PAGE_MAX_CHANGES,
                    JOURNAL_PAGE_MAX_BYTES,
                ),
                None => runtime.lifecycle_changes_page(
                    &cursor,
                    JOURNAL_PAGE_MAX_CHANGES,
                    JOURNAL_PAGE_MAX_BYTES,
                ),
            };
            reads.journal = Some((ticket, woke));
            return;
        }
    };
    match result {
        Ok(page) => {
            state.journal_page_reads = state.journal_page_reads.saturating_add(1);
            if let Some(reason) = page.resync_required {
                handle_resync_reason(state, reason);
                return;
            }
            state.journal_source_watermark = Some(page.source_watermark.clone());
            let received = !page.changes.is_empty();
            let at_watermark = page.next == page.source_watermark;
            let journal_advanced = woke || state.take_journal_wake();
            // An empty page can still be stale when a journal-advanced wake
            // arrives in the same slice. Confirmation must wait for a later
            // pull that sees no wake.
            state.journal_caught_up_confirmed =
                journal_caught_up_after_pull(received, at_watermark, journal_advanced);
            state.pending_changes.extend(page.changes);
            if state.journal_caught_up_confirmed && state.projection_dirty {
                state.wakes.mark(MaintenanceSliceKind::SubscriberDelivery);
            } else if journal_advanced && !received {
                state.wakes.mark(MaintenanceSliceKind::JournalPull);
            } else if received || !at_watermark || journal_advanced {
                state.wakes.mark_all();
            }
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.wakes.mark_all();
        }
        Err(_) => start_baseline_recovery(state),
    }
}

fn run_projection_apply_slice(runtime: Option<&HubRuntime>, state: &mut MaintenanceState) {
    if !state.projection.baseline_complete {
        state.pending_changes.clear();
        return;
    }
    let mut applied = 0;
    while applied < APPLY_MAX_CHANGES {
        let Some(change) = state.pending_changes.pop_front() else {
            break;
        };
        state.projection.apply_change(&change);
        queue_family_delta(state, &change);
        applied += 1;
    }
    if let Some(runtime) = runtime {
        retire_projected_spawn_acks(runtime, state);
    }
    if applied > 0 {
        state.projection_dirty = true;
        state.wakes.mark(MaintenanceSliceKind::SubscriberDelivery);
    } else if !state.pending_changes.is_empty() {
        state.wakes.mark_all();
    }
}

fn run_baseline_slice(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    reads: &mut MaintenanceCoreReads,
    waiter_id: Option<crate::owner_identity::WaiterId>,
) {
    let result = match reads.baseline.as_mut() {
        Some(ticket) => match ticket.poll() {
            CoreTicketPoll::Pending => return,
            CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                reads.baseline = None;
                return;
            }
            CoreTicketPoll::Ready(result) => {
                reads.baseline = None;
                result
            }
        },
        None => {
            let Some(recovery) = state.baseline.as_ref() else {
                return;
            };
            let snapshot_ref = recovery.snapshot.clone();
            let after_ref = recovery.after.clone();
            reads.baseline = Some(match waiter_id {
                Some(waiter_id) => runtime.lifecycle_baseline_page_for_owner(
                    waiter_id,
                    snapshot_ref.as_ref(),
                    after_ref.as_ref(),
                    BASELINE_PAGE_BUDGET,
                ),
                None => runtime.lifecycle_baseline_page(
                    snapshot_ref.as_ref(),
                    after_ref.as_ref(),
                    BASELINE_PAGE_BUDGET,
                ),
            });
            return;
        }
    };
    match result {
        Ok(page) => {
            state.baseline_page_reads = state.baseline_page_reads.saturating_add(1);
            if let Some(reason) = page.resync_required {
                handle_resync_reason(state, reason);
                return;
            }
            let complete = page.complete;
            let snapshot = page.snapshot_sequence.clone();
            if let Some(recovery) = state.baseline.as_mut() {
                recovery.snapshot = Some(snapshot.clone());
                recovery.after = page.next.clone();
            }
            state
                .projection
                .ingest_baseline_rows(snapshot.sequence, page.sessions);
            if complete {
                state.baseline = None;
                state.projection.seal_baseline(snapshot.clone());
                sync_acknowledged_spawns(runtime, state);
                retire_projected_spawn_acks(runtime, state);
                // Keep the sealed snapshot cursor. A rewind on every seal
                // expires after journal retention and remints forever.
                // CursorExpired still remints one fresh baseline because
                // Core treats an expired cursor as lost history.
                state.journal_caught_up_confirmed = false;
                if acknowledged_spawns_missing_from_projection(state) {
                    start_omitted_row_recover(state);
                } else {
                    state.wakes.mark(MaintenanceSliceKind::JournalPull);
                }
                state.projection_dirty = true;
                begin_family_snapshots(state, snapshot.sequence);
            } else {
                state.wakes.mark_all();
            }
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.wakes.mark_all();
        }
        Err(_) => start_baseline_recovery(state),
    }
}

fn run_host_bridge_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let mut budget = HostBridgeBudget::new();
    refresh_session_family_consumers(runtime, state, &mut budget);
    if !budget.exhausted() {
        continue_gap_pass(state, &mut budget);
    }
    if !budget.exhausted() {
        continue_snapshot_starts(state, &mut budget);
    }
    if !budget.exhausted() {
        continue_family_fanout(state, &mut budget);
    }
    if !budget.exhausted() {
        continue_consumer_prune(state, &mut budget);
    }
    if budget.exhausted() {
        state.wakes.mark_all();
        return;
    }
    let Some((plugin_key, handler, payload)) = next_session_family_admission(state, &mut budget)
    else {
        return;
    };
    let sequence = payload
        .get("snapshot_sequence")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let kind = family_frame_kind(&payload);
    let request_id = state.session_family.next_request_id(&plugin_key, sequence);
    let admission = runtime.try_admit_plugin(
        PluginInvocationClass::Background,
        PluginInvocationRequest {
            request_id: request_id.clone(),
            handler,
            timeout_ms: EVENT_INVOCATION_TIMEOUT_MS,
            context: PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: Some(SESSION_FAMILY_INVOCATION_ORIGIN.to_string()),
                metadata: None,
            },
            payload: BoundaryJson(payload),
        },
    );
    match admission {
        PluginAdmissionResult::Queued { .. } => {
            state
                .session_family
                .touch_consumer(&plugin_key, |consumer| {
                    consumer.in_flight = Some(InFlightSessionFamily {
                        request_id: request_id.clone(),
                        snapshot_sequence: consumer.snapshot_sequence,
                        kind,
                    });
                });
            state
                .session_family
                .in_flight_by_request
                .insert(request_id.0, plugin_key);
        }
        _ => {
            state.session_family.mark_gap(&plugin_key);
            start_baseline_recovery(state);
        }
    }
}

const EVENT_DELIVERY_MAX_ITEMS: usize = 8;
const EVENT_DELIVERY_MAX_BYTES: usize = 32 * 1024;
const EVENT_DELIVERY_MAX_ELAPSED: Duration = Duration::from_millis(8);

fn flush_pending_event_retirements(runtime: &HubRuntime, state: &mut MaintenanceState) {
    if state.event_causal_blocked || state.event_causal_faulted {
        return;
    }
    let Some(mut flight) = state.pending_retirements.pop_front() else {
        return;
    };
    if !retire_event_holder(runtime, &mut flight) {
        state.event_causal_blocked = flight.holder_retired && flight.scope_id.is_some();
        state.event_causal_faulted = state.event_causal_blocked && runtime.causal_faulted();
        state.pending_retirements.push_front(flight);
    }
    if !state.pending_retirements.is_empty()
        && !state.event_causal_blocked
        && !state.event_causal_faulted
    {
        state.wakes.mark(MaintenanceSliceKind::PackageEventDelivery);
    }
}

fn retire_event_holder(runtime: &HubRuntime, flight: &mut EventDeliveryFlight) -> bool {
    if !flight.holder_retired {
        let result = match flight.pull_id {
            Some(pull_id) => runtime.package_event_router().retire_pulled(
                pull_id,
                flight.envelope_id,
                &flight.plugin_key,
                flight.generation,
            ),
            None => runtime.package_event_router().retire_holder(
                flight.envelope_id,
                &flight.plugin_key,
                flight.generation,
            ),
        };
        match result {
            Ok(_) => {
                flight.holder_retired = true;
                flight.pull_id = None;
            }
            Err(_) => return false,
        }
    }
    if let Some(scope_id) = flight.scope_id {
        return matches!(
            runtime.admit_causal_op(crate::package_event_router::CausalOp::Release {
                scope_id,
                identity: crate::package_event_router::LeaseIdentity::EventInFlight,
            }),
            crate::package_event_router::CausalAdmitResult::Applied
        );
    }
    true
}

fn queue_event_retirement(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    flight: EventDeliveryFlight,
) {
    if flight.holder_retired && flight.scope_id.is_some() {
        state.event_causal_blocked = true;
        state.event_causal_faulted |= runtime.causal_faulted();
    }
    state.pending_retirements.push_back(flight);
    if !state.event_causal_blocked && !state.event_causal_faulted {
        state.wakes.mark(MaintenanceSliceKind::PackageEventDelivery);
    }
}

fn run_package_event_delivery_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    flush_pending_event_retirements(runtime, state);
    if state.event_causal_blocked || state.event_causal_faulted {
        return;
    }
    let woke = runtime.package_event_router().take_delivery_wake();
    if runtime.package_event_router().peek_delivery_wake() {
        state.wakes.mark_all();
    }
    if !woke {
        return;
    }
    let batch = match runtime.package_event_router().pull_ready_batch(
        EVENT_DELIVERY_MAX_ITEMS,
        EVENT_DELIVERY_MAX_BYTES,
        Instant::now(),
        EVENT_DELIVERY_MAX_ELAPSED,
    ) {
        Ok(batch) => batch,
        Err(_) => {
            runtime.package_event_router().set_delivery_wake();
            state.wakes.mark_all();
            return;
        }
    };
    for delivery in batch {
        let request_id = package_event_request_id(&delivery);
        let Some(handler) = runtime.package_event_handler(
            &delivery.holder.plugin_key,
            &delivery.owner,
            &delivery.name,
            &delivery.holder.handler_id,
        ) else {
            let mut flight = event_flight(&delivery, None, request_id.0);
            if !retire_event_holder(runtime, &mut flight) {
                queue_event_retirement(runtime, state, flight);
            }
            continue;
        };
        let reservation = match runtime.reserve_causal_transition() {
            Ok(reservation) => reservation,
            Err(status) => {
                state.event_causal_blocked = true;
                state.event_causal_faulted |=
                    status == crate::runtime::CausalTransitionStatus::Fault;
                if let Err((delivery, _)) =
                    runtime.package_event_router().requeue_delivery(delivery)
                {
                    let mut flight = event_flight(&delivery, None, request_id.0);
                    if !retire_event_holder(runtime, &mut flight) {
                        queue_event_retirement(runtime, state, flight);
                    }
                }
                continue;
            }
        };
        let Some(scope_id) = runtime.causal_scopes().mint_with_lease(Some(
            crate::package_event_router::LeaseIdentity::EventInFlight,
        )) else {
            let mut flight = event_flight(&delivery, None, request_id.0.clone());
            if !retire_event_holder(runtime, &mut flight) {
                queue_event_retirement(runtime, state, flight);
            }
            continue;
        };
        let metadata = Some(BoundaryJson(
            serde_json::json!({ "causal_scope_id": scope_id }),
        ));
        let admission = runtime.try_admit_plugin(
            PluginInvocationClass::Background,
            PluginInvocationRequest {
                request_id: request_id.clone(),
                handler: handler.handler,
                timeout_ms: EVENT_INVOCATION_TIMEOUT_MS,
                context: PluginInvocationContext {
                    client_id: None,
                    session_id: None,
                    subscription_id: None,
                    surface_id: None,
                    origin: Some(PACKAGE_EVENT_INVOCATION_ORIGIN.to_string()),
                    metadata,
                },
                payload: BoundaryJson(delivery.payload_json.clone()),
            },
        );
        match admission {
            PluginAdmissionResult::Queued { .. } => {
                if runtime
                    .package_event_router()
                    .note_admitted(
                        delivery.envelope_id,
                        &delivery.holder.plugin_key,
                        delivery.holder.generation,
                    )
                    .is_err()
                {
                    state.wakes.mark_all();
                }
                state.event_in_flight.insert(
                    request_id.0.clone(),
                    event_flight(&delivery, Some(scope_id), request_id.0),
                );
            }
            PluginAdmissionResult::Backpressured { .. } => {
                reservation.commit(crate::package_event_router::CausalOp::Release {
                    scope_id,
                    identity: crate::package_event_router::LeaseIdentity::EventInFlight,
                });
                match runtime.package_event_router().requeue_delivery(delivery) {
                    Ok(()) => {
                        state.wakes.mark_all();
                    }
                    Err((delivery, _)) => {
                        let mut flight = event_flight(&delivery, None, request_id.0);
                        if !retire_event_holder(runtime, &mut flight) {
                            queue_event_retirement(runtime, state, flight);
                        }
                    }
                }
            }
            _ => {
                reservation.commit(crate::package_event_router::CausalOp::Release {
                    scope_id,
                    identity: crate::package_event_router::LeaseIdentity::EventInFlight,
                });
                let mut flight = event_flight(&delivery, None, request_id.0);
                if !retire_event_holder(runtime, &mut flight) {
                    queue_event_retirement(runtime, state, flight);
                }
            }
        }
    }
    if runtime.package_event_router().peek_delivery_wake()
        && !state.event_causal_blocked
        && !state.event_causal_faulted
    {
        state.wakes.mark(MaintenanceSliceKind::PackageEventDelivery);
    }
}

fn run_completion_drain_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let budget = crate::daemon::control::reply::RetainedPluginResultBudget::new();
    let _ = run_completion_drain_slice_with_controls(runtime, state, &budget);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletionDrainProgress {
    pub(crate) item_count: usize,
    pub(crate) has_remaining: bool,
}

pub(crate) fn run_completion_drain_slice_for_owner(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    controls: &mut crate::daemon::control::plugins::PluginControlState,
    entities: &mut crate::daemon::control::entities::PluginEntityState,
    result_budget: &crate::daemon::control::reply::RetainedPluginResultBudget,
) -> CompletionDrainProgress {
    run_completion_drain_slice_with_owner_routes(
        runtime,
        state,
        Some(controls),
        Some(entities),
        result_budget,
    )
}

fn run_completion_drain_slice_with_controls(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    result_budget: &crate::daemon::control::reply::RetainedPluginResultBudget,
) -> CompletionDrainProgress {
    run_completion_drain_slice_with_owner_routes(runtime, state, None, None, result_budget)
}

fn run_completion_drain_slice_with_owner_routes(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    mut controls: Option<&mut crate::daemon::control::plugins::PluginControlState>,
    mut entities: Option<&mut crate::daemon::control::entities::PluginEntityState>,
    result_budget: &crate::daemon::control::reply::RetainedPluginResultBudget,
) -> CompletionDrainProgress {
    flush_pending_event_retirements(runtime, state);
    let drain = runtime
        .drain_plugin_completions(COMPLETION_DRAIN_MAX_ITEMS, result_budget.available_bytes());
    let progress = CompletionDrainProgress {
        item_count: drain.item_count,
        has_remaining: drain.has_remaining,
    };
    for item in drain.completions {
        let charge = result_budget
            .try_reserve(item.encoded_len)
            .expect("Core completion drain exceeded the supplied retained-result byte limit");
        let completion =
            crate::daemon::control::reply::RetainedPluginResult::new(item.completion, charge);
        let completion = if let Some(controls) = controls.as_deref_mut() {
            match controls.route_completion(completion) {
                None => continue,
                Some(completion) => completion,
            }
        } else {
            completion
        };
        let completion = if let Some(entities) = entities.as_deref_mut() {
            match entities.route_completion(completion) {
                None => continue,
                Some(completion) => completion,
            }
        } else {
            completion
        };
        apply_plugin_completion(runtime, state, completion.value());
    }
    progress
}

fn apply_plugin_completion(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    completion: &PluginCompletion,
) {
    let request_id = match &completion.result {
        PluginInvocationResult::Completed(success) => success.request_id.clone(),
        PluginInvocationResult::Failed(failure) => failure.request_id.clone(),
    };
    if state.event_in_flight.contains_key(&request_id.0) {
        match &completion.result {
            PluginInvocationResult::Completed(_) => {
                runtime.event_plane_counters().record_handler_completed_ok();
            }
            PluginInvocationResult::Failed(failure) => match failure.kind {
                PluginInvocationFailureKind::TimedOut => {
                    runtime.event_plane_counters().record_handler_timed_out();
                }
                PluginInvocationFailureKind::HandlerFailed
                | PluginInvocationFailureKind::CompletionTooLarge => {
                    // Oversized results use the existing handler failure counter.
                    runtime.event_plane_counters().record_handler_failed();
                }
                PluginInvocationFailureKind::Cancelled => {
                    runtime.event_plane_counters().record_handler_cancelled();
                }
                PluginInvocationFailureKind::Backpressured => {
                    runtime
                        .event_plane_counters()
                        .record_handler_backpressured();
                }
                PluginInvocationFailureKind::WorkerStopped => {
                    runtime
                        .event_plane_counters()
                        .record_handler_worker_stopped();
                }
            },
        }
    }
    if let Some(mut flight) = state.event_in_flight.remove(&request_id.0) {
        if !retire_event_holder(runtime, &mut flight) {
            queue_event_retirement(runtime, state, flight);
        }
        state.wakes.mark_all();
        return;
    }
    let Some(plugin_key) = state
        .session_family
        .in_flight_by_request
        .remove(&request_id.0)
    else {
        return;
    };
    let success = matches!(completion.result, PluginInvocationResult::Completed(_));
    state
        .session_family
        .touch_existing_consumer(&plugin_key, |consumer| {
            let ended_this_snapshot = consumer.in_flight.as_ref().is_some_and(|flight| {
                flight.kind == FamilyFrameKind::End
                    && flight.snapshot_sequence == consumer.snapshot_sequence
            });
            consumer.in_flight = None;
            if ended_this_snapshot && success {
                consumer.snapshot_complete = true;
                consumer.pending.extend(consumer.held_deltas.drain(..));
            }
        });
    if !success {
        state.session_family.mark_gap(&plugin_key);
        start_baseline_recovery(state);
        return;
    }
    state.wakes.mark_all();
}

/// Start a paged baseline recovery. Incomplete pages are not ended evidence.
pub fn start_baseline_recovery(state: &mut MaintenanceState) {
    state.projection.begin_baseline_recovery();
    state.pending_changes.clear();
    state.journal_source_watermark = None;
    state.journal_caught_up_confirmed = false;
    state.omitted_row_recover_at = None;
    state.observe_resume = None;
    state.session_family.need_gap_pass = true;
    state.session_family.gap_after = None;
    state.session_family.pending_fanout.clear();
    state.session_family.fanout_bytes = 0;
    state.session_family.snapshot_start_sequence = None;
    state.session_family.snapshot_start_after = None;
    state.baseline = None;
    state.wakes.mark_all();
}

fn continue_gap_pass(state: &mut MaintenanceState, budget: &mut HostBridgeBudget) {
    if !state.session_family.need_gap_pass {
        return;
    }
    let max = budget.remaining_visits();
    if max == 0 {
        return;
    }
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.gap_after.as_deref(),
        max,
    );
    let mut last_key = state.session_family.gap_after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if !budget.take() {
            break;
        }
        state.session_family.mark_gap(plugin_key);
        last_key = Some(plugin_key.clone());
        visited += 1;
    }
    if visited < keys.len() || keys.len() == max {
        state.session_family.gap_after = last_key;
        state.wakes.mark_all();
        return;
    }
    state.session_family.need_gap_pass = false;
    state.session_family.gap_after = None;
    state.baseline = Some(BaselineRecovery {
        snapshot: None,
        after: None,
    });
    state.wakes.mark_all();
}

fn continue_snapshot_starts(state: &mut MaintenanceState, budget: &mut HostBridgeBudget) {
    let Some(sequence) = state.session_family.snapshot_start_sequence else {
        return;
    };
    let max = budget.remaining_visits();
    if max == 0 {
        return;
    }
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.snapshot_start_after.as_deref(),
        max,
    );
    let mut last_key = state.session_family.snapshot_start_after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if !budget.take() {
            break;
        }
        state.session_family.begin_snapshot(plugin_key, sequence);
        last_key = Some(plugin_key.clone());
        visited += 1;
    }
    if visited < keys.len() || keys.len() == max {
        state.session_family.snapshot_start_after = last_key;
        state.wakes.mark_all();
        return;
    }
    state.session_family.snapshot_start_sequence = None;
    state.session_family.snapshot_start_after = None;
}

fn continue_family_fanout(state: &mut MaintenanceState, budget: &mut HostBridgeBudget) {
    while !budget.exhausted() {
        let Some(mut job) = state.session_family.pending_fanout.pop_front() else {
            return;
        };
        state.session_family.fanout_bytes =
            state.session_family.fanout_bytes.saturating_sub(job.bytes);
        let more = fanout_family_frame(state, &mut job, budget);
        if more {
            state.session_family.fanout_bytes =
                state.session_family.fanout_bytes.saturating_add(job.bytes);
            state.session_family.pending_fanout.push_front(job);
            state.wakes.mark_all();
            return;
        }
    }
    if !state.session_family.pending_fanout.is_empty() {
        state.wakes.mark_all();
    }
}

fn handle_resync_reason(state: &mut MaintenanceState, _reason: SessionLifecycleResyncReason) {
    state.resync_reads = state.resync_reads.saturating_add(1);
    start_baseline_recovery(state);
}

fn refresh_session_family_consumers(
    runtime: &HubRuntime,
    state: &mut MaintenanceState,
    budget: &mut HostBridgeBudget,
) {
    let max = budget.remaining_visits();
    if max == 0 {
        return;
    }
    let after = state.session_family.refresh_after.clone();
    let (handlers, last_visited, visited, more) =
        runtime.session_family_event_handlers_page(after.as_deref(), max);
    for _ in 0..visited {
        if !budget.take() {
            break;
        }
    }
    for handler in handlers {
        if handler.handler.kind != PluginHandlerKind::Event {
            continue;
        }
        let plugin_key = handler.handler.plugin_key.0.clone();
        state.session_family.refresh_seen.insert(plugin_key.clone());
        let existed = state.session_family.consumers.contains_key(&plugin_key);
        let consumer = state.session_family.consumer_mut(&plugin_key);
        consumer.handler = Some(handler.handler);
        if !existed {
            consumer.gap = true;
            if state.projection.baseline_complete {
                let sequence = state
                    .projection
                    .cursor
                    .as_ref()
                    .map(|cursor| cursor.sequence)
                    .unwrap_or(0);
                state.session_family.begin_snapshot(&plugin_key, sequence);
            }
        }
    }
    if more {
        state.session_family.refresh_after = last_visited;
        state.wakes.mark_all();
        return;
    }
    state.session_family.refresh_after = None;
    state.session_family.need_prune = true;
    state.session_family.prune_after = None;
}

fn continue_consumer_prune(state: &mut MaintenanceState, budget: &mut HostBridgeBudget) {
    if !state.session_family.need_prune {
        return;
    }
    let max = budget.remaining_visits();
    if max == 0 {
        return;
    }
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.prune_after.as_deref(),
        max,
    );
    let mut last_key = state.session_family.prune_after.clone();
    let mut visited = 0;
    let mut drop_keys = Vec::new();
    for plugin_key in &keys {
        if !budget.take() {
            break;
        }
        last_key = Some(plugin_key.clone());
        visited += 1;
        if !state.session_family.refresh_seen.contains(plugin_key) {
            drop_keys.push(plugin_key.clone());
        }
    }
    for plugin_key in drop_keys {
        let Some(consumer) = state.session_family.consumers.remove(&plugin_key) else {
            continue;
        };
        if consumer_busy(&consumer) {
            state.session_family.busy_count = state.session_family.busy_count.saturating_sub(1);
        }
        if let Some(flight) = consumer.in_flight {
            state
                .session_family
                .in_flight_by_request
                .remove(&flight.request_id.0);
        }
    }
    if visited < keys.len() || keys.len() == max {
        state.session_family.prune_after = last_key;
        state.wakes.mark_all();
        return;
    }
    state.session_family.need_prune = false;
    state.session_family.prune_after = None;
    state.session_family.refresh_seen.clear();
}

fn next_session_family_admission(
    state: &mut MaintenanceState,
    budget: &mut HostBridgeBudget,
) -> Option<(String, PluginHandlerRef, serde_json::Value)> {
    let max = budget.remaining_visits();
    if max == 0 {
        return None;
    }
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.admit_after.as_deref(),
        max,
    );
    if keys.is_empty() {
        state.session_family.admit_after = None;
        return None;
    }
    for plugin_key in &keys {
        if !budget.take() {
            state.wakes.mark_all();
            return None;
        }
        let Some(peeked) = peek_session_family_payload(state, plugin_key) else {
            continue;
        };
        let payload = peeked.frame().clone();
        let bytes = serde_json::to_vec(&payload)
            .map(|body| body.len())
            .unwrap_or(0);
        if !budget.add_bytes(bytes) {
            state.wakes.mark_all();
            return None;
        }
        commit_session_family_payload(state, plugin_key, peeked);
        let handler = state
            .session_family
            .consumers
            .get(plugin_key)
            .and_then(|consumer| consumer.handler.clone())?;
        state.session_family.admit_after = Some(plugin_key.clone());
        return Some((plugin_key.clone(), handler, payload));
    }
    if keys.len() == max {
        state.session_family.admit_after = keys.last().cloned();
        state.wakes.mark_all();
    } else {
        state.session_family.admit_after = None;
    }
    None
}

enum PeekedFamilyPayload {
    Queued(serde_json::Value),
    Chunk {
        frame: serde_json::Value,
        last_id: String,
    },
    End(serde_json::Value),
}

impl PeekedFamilyPayload {
    fn frame(&self) -> &serde_json::Value {
        match self {
            Self::Queued(frame) | Self::Chunk { frame, .. } | Self::End(frame) => frame,
        }
    }
}

fn peek_session_family_payload(
    state: &MaintenanceState,
    plugin_key: &str,
) -> Option<PeekedFamilyPayload> {
    let consumer = state.session_family.consumers.get(plugin_key)?;
    if consumer.in_flight.is_some() || consumer.handler.is_none() {
        return None;
    }
    if let Some(payload) = consumer.pending.front() {
        return Some(PeekedFamilyPayload::Queued(payload.clone()));
    }
    if !consumer.need_snapshot_chunks {
        return None;
    }
    match next_projection_chunk(&state.projection, consumer.snapshot_after.as_deref()) {
        Ok(Some((chunk, last_id))) => Some(PeekedFamilyPayload::Chunk {
            frame: session_chunk_frame(consumer.snapshot_sequence, &chunk),
            last_id,
        }),
        Ok(None) => Some(PeekedFamilyPayload::End(session_end_frame(
            consumer.snapshot_sequence,
            true,
        ))),
        Err(()) => None,
    }
}

fn commit_session_family_payload(
    state: &mut MaintenanceState,
    plugin_key: &str,
    peeked: PeekedFamilyPayload,
) {
    match peeked {
        PeekedFamilyPayload::Queued(payload) => {
            state
                .session_family
                .touch_existing_consumer(plugin_key, |consumer| {
                    if consumer.pending.front() == Some(&payload) {
                        consumer.pending.pop_front();
                    }
                });
        }
        PeekedFamilyPayload::Chunk { last_id, .. } => {
            state
                .session_family
                .touch_existing_consumer(plugin_key, |consumer| {
                    consumer.snapshot_after = Some(last_id);
                });
        }
        PeekedFamilyPayload::End(_) => {
            state
                .session_family
                .touch_existing_consumer(plugin_key, |consumer| {
                    consumer.need_snapshot_chunks = false;
                });
        }
    }
}

#[cfg(test)]
fn next_session_family_payload(
    state: &mut MaintenanceState,
    plugin_key: &str,
) -> Option<serde_json::Value> {
    let peeked = peek_session_family_payload(state, plugin_key)?;
    let payload = peeked.frame().clone();
    commit_session_family_payload(state, plugin_key, peeked);
    Some(payload)
}

fn next_projection_chunk(
    projection: &SessionProjection,
    after: Option<&str>,
) -> Result<Option<(Vec<serde_json::Value>, String)>, ()> {
    let mut packed = Vec::new();
    let mut last_id = None;
    let start = match after {
        Some(after) => Bound::Excluded(after),
        None => Bound::Unbounded,
    };
    for (id, row) in projection.rows.range::<str, _>((start, Bound::Unbounded)) {
        let item =
            serde_json::to_value(SessionProjection::project_entity(&row.record)).map_err(|_| ())?;
        packed.push(item);
        match pack_session_chunk(&packed, 0) {
            Ok((chunk, next)) if next == packed.len() => {
                last_id = Some(id.clone());
                packed = chunk;
                if packed.len() >= SESSION_CHUNK_MAX_ITEMS {
                    break;
                }
            }
            Ok((chunk, _)) => {
                packed = chunk;
                break;
            }
            Err(()) => {
                if packed.len() == 1 {
                    return Err(());
                }
                packed.pop();
                break;
            }
        }
    }
    match last_id {
        Some(id) if !packed.is_empty() => Ok(Some((packed, id))),
        _ => Ok(None),
    }
}

fn queue_family_delta(state: &mut MaintenanceState, change: &SessionLifecycleChange) {
    let frame = match &change.kind {
        botster_core_daemon::SessionLifecycleChangeKind::Upsert { record } => {
            let entity = SessionProjection::project_entity(record);
            serde_json::json!({
                "type": "entity_upsert",
                "family": "/session",
                "snapshot_sequence": change.cursor.sequence,
                "id": record.session.session_id.0,
                "entity": entity,
            })
        }
        botster_core_daemon::SessionLifecycleChangeKind::Removed { session_id } => {
            serde_json::json!({
                "type": "entity_remove",
                "family": "/session",
                "snapshot_sequence": change.cursor.sequence,
                "id": session_id.0,
            })
        }
        _ => return,
    };
    let bytes = serde_json::to_vec(&frame)
        .map(|body| body.len())
        .unwrap_or(0);
    if state.session_family.pending_fanout.len() >= FANOUT_QUEUE_MAX_ITEMS
        || state.session_family.fanout_bytes.saturating_add(bytes) > FANOUT_QUEUE_MAX_BYTES
    {
        start_baseline_recovery(state);
        return;
    }
    state.session_family.fanout_bytes = state.session_family.fanout_bytes.saturating_add(bytes);
    state.session_family.pending_fanout.push_back(FanoutJob {
        frame,
        after: None,
        bytes,
    });
    state.wakes.mark_all();
}

fn fanout_family_frame(
    state: &mut MaintenanceState,
    job: &mut FanoutJob,
    budget: &mut HostBridgeBudget,
) -> bool {
    let max = budget.remaining_visits();
    if max == 0 {
        return true;
    }
    let keys = consumer_keys_page(&state.session_family.consumers, job.after.as_deref(), max);
    let mut last_key = job.after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if !budget.take() {
            break;
        }
        if !budget.add_bytes(job.bytes) {
            break;
        }
        if !state
            .session_family
            .queue_delta(plugin_key, job.frame.clone())
        {
            state.session_family.mark_gap(plugin_key);
            if state.projection.baseline_complete {
                let sequence = state
                    .projection
                    .cursor
                    .as_ref()
                    .map(|cursor| cursor.sequence)
                    .unwrap_or(0);
                state.session_family.begin_snapshot(plugin_key, sequence);
            }
        }
        last_key = Some(plugin_key.clone());
        visited += 1;
    }
    if visited < keys.len() || keys.len() == max {
        job.after = last_key;
        true
    } else {
        job.after = None;
        false
    }
}

fn begin_family_snapshots(state: &mut MaintenanceState, sequence: u64) {
    state.session_family.snapshot_start_sequence = Some(sequence);
    state.session_family.snapshot_start_after = None;
    state.wakes.mark_all();
}

fn consumer_keys_page(
    consumers: &BTreeMap<String, SessionFamilyConsumer>,
    after: Option<&str>,
    max: usize,
) -> Vec<String> {
    let start = match after {
        Some(after) => Bound::Excluded(after),
        None => Bound::Unbounded,
    };
    consumers
        .range::<str, _>((start, Bound::Unbounded))
        .map(|(key, _)| key.clone())
        .take(max)
        .collect()
}

/// Fail if this module's source imports terminal bodies or names product policy.
#[cfg(test)]
pub fn assert_maintenance_source_stays_control_plane(source: &str) {
    crate::session_projection::assert_projection_source_stays_control_plane(source);
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use botster_core_daemon::ObserveLifecyclePassId;

    use super::*;

    #[test]
    fn source_stays_control_plane() {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/daemon_maintenance.rs"
        ));
        assert_maintenance_source_stays_control_plane(source);
        assert!(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/daemon_maintenance.rs")
                .exists()
        );
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);
        assert!(
            !production
                .replace("observe_lifecycle_slice", "")
                .contains("observe_lifecycle(")
        );
        assert!(
            !production
                .replace("lifecycle_baseline_page", "")
                .contains("lifecycle_baseline(")
        );
    }

    #[test]
    fn session_family_completion_fence_holds_later_frames() {
        let mut bridge = SessionFamilyBridge::default();
        bridge.queue_snapshot(
            "plugin.one",
            7,
            &[
                serde_json::json!({"id": "a"}),
                serde_json::json!({"id": "b"}),
            ],
        );
        let consumer = bridge.consumers.get("plugin.one").expect("consumer");
        assert_eq!(
            consumer.pending.front().and_then(|frame| frame.get("type")),
            Some(&serde_json::json!("snapshot_begin"))
        );
        assert!(
            consumer
                .pending
                .iter()
                .any(|frame| frame.get("type") == Some(&serde_json::json!("snapshot_chunk")))
        );
        assert_eq!(
            consumer.pending.back().and_then(|frame| frame.get("type")),
            Some(&serde_json::json!("snapshot_end"))
        );
        let mut state = MaintenanceState {
            session_family: bridge,
            ..MaintenanceState::default()
        };
        state
            .session_family
            .consumers
            .get_mut("plugin.one")
            .expect("consumer")
            .handler = Some(PluginHandlerRef {
            plugin_key: botster_core::PluginKey("plugin.one".to_string()),
            kind: PluginHandlerKind::Event,
            handler_id: "session_family".to_string(),
        });
        let first =
            next_session_family_admission(&mut state, &mut HostBridgeBudget::new()).expect("begin");
        assert_eq!(
            first.2.get("type"),
            Some(&serde_json::json!("snapshot_begin"))
        );
        state
            .session_family
            .consumers
            .get_mut("plugin.one")
            .unwrap()
            .in_flight = Some(InFlightSessionFamily {
            request_id: RequestId("session-family-plugin.one-7".to_string()),
            snapshot_sequence: 7,
            kind: FamilyFrameKind::Begin,
        });
        assert!(
            next_session_family_admission(&mut state, &mut HostBridgeBudget::new()).is_none(),
            "must not admit the next frame while one is in flight"
        );
    }

    #[test]
    fn pack_session_chunk_keeps_every_row_under_the_byte_budget() {
        let items = (0..20)
            .map(|index| serde_json::json!({ "session_uuid": format!("s{index:02}") }))
            .collect::<Vec<_>>();
        let mut start = 0;
        let mut seen = Vec::new();
        while start < items.len() {
            let (chunk, next) = pack_session_chunk(&items, start).expect("chunk fits");
            assert!(!chunk.is_empty());
            let encoded = serde_json::to_vec(&session_chunk_frame(1, &chunk)).expect("encode");
            assert!(encoded.len() <= SESSION_CHUNK_MAX_BYTES);
            seen.extend(chunk.iter().filter_map(|item| {
                item.get("session_uuid")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            }));
            start = next;
        }
        assert_eq!(
            seen,
            items
                .iter()
                .filter_map(|item| item.get("session_uuid")?.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn pack_session_chunk_rejects_a_single_row_over_the_byte_budget() {
        let huge = serde_json::json!({ "session_uuid": "x".repeat(SESSION_CHUNK_MAX_BYTES) });
        assert!(pack_session_chunk(&[huge], 0).is_err());
    }

    fn test_record(id: &str) -> botster_core_daemon::SessionLifecycleRecord {
        botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: botster_core_daemon::RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(botster_core::SessionLifecycleState::Running),
        }
    }

    fn complete_after_frame(state: &mut MaintenanceState, plugin_key: &str, kind: FamilyFrameKind) {
        let consumer = state
            .session_family
            .consumers
            .get_mut(plugin_key)
            .expect("consumer");
        consumer.in_flight = None;
        if kind == FamilyFrameKind::End {
            consumer.snapshot_complete = true;
            consumer.pending.extend(consumer.held_deltas.drain(..));
        }
    }

    #[test]
    fn production_begin_completion_does_not_release_deltas() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            botster_core_daemon::SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 3,
            },
            vec![test_record("a")],
        );
        let mut state = MaintenanceState {
            projection,
            ..MaintenanceState::default()
        };
        state.session_family.begin_snapshot("plugin.one", 3);
        state
            .session_family
            .consumers
            .get_mut("plugin.one")
            .expect("consumer")
            .handler = Some(PluginHandlerRef {
            plugin_key: botster_core::PluginKey("plugin.one".to_string()),
            kind: PluginHandlerKind::Event,
            handler_id: "session_family".to_string(),
        });
        assert!(state.session_family.queue_delta(
            "plugin.one",
            serde_json::json!({"type": "entity_upsert", "id": "live-after-begin"}),
        ));
        let begin = next_session_family_payload(&mut state, "plugin.one").expect("begin");
        assert_eq!(
            begin.get("type"),
            Some(&serde_json::json!("snapshot_begin"))
        );
        assert_eq!(family_frame_kind(&begin), FamilyFrameKind::Begin);
        complete_after_frame(&mut state, "plugin.one", FamilyFrameKind::Begin);
        {
            let consumer = state
                .session_family
                .consumers
                .get("plugin.one")
                .expect("consumer");
            assert!(!consumer.snapshot_complete);
            assert_eq!(consumer.held_deltas.len(), 1);
            assert!(consumer.need_snapshot_chunks);
        }
        let chunk = next_session_family_payload(&mut state, "plugin.one").expect("chunk");
        assert_eq!(
            chunk.get("type"),
            Some(&serde_json::json!("snapshot_chunk"))
        );
        assert!(state.session_family.queue_delta(
            "plugin.one",
            serde_json::json!({"type": "entity_upsert", "id": "live-after-chunk"}),
        ));
        complete_after_frame(&mut state, "plugin.one", FamilyFrameKind::Chunk);
        {
            let consumer = state
                .session_family
                .consumers
                .get("plugin.one")
                .expect("consumer");
            assert!(!consumer.snapshot_complete);
            assert_eq!(consumer.held_deltas.len(), 2);
        }
        let end = next_session_family_payload(&mut state, "plugin.one").expect("end");
        assert_eq!(end.get("type"), Some(&serde_json::json!("snapshot_end")));
        complete_after_frame(&mut state, "plugin.one", FamilyFrameKind::End);
        let consumer = state
            .session_family
            .consumers
            .get("plugin.one")
            .expect("consumer");
        assert!(consumer.snapshot_complete);
        assert!(consumer.held_deltas.is_empty());
        assert_eq!(consumer.pending.len(), 2);
    }

    #[test]
    fn session_family_request_ids_are_unique_per_frame() {
        let mut bridge = SessionFamilyBridge::default();
        let first = bridge.next_request_id("plugin.one", 7);
        let second = bridge.next_request_id("plugin.one", 7);
        assert_ne!(first, second);
    }

    #[test]
    fn exact_wakes_coalesce_without_clearing_other_kinds() {
        let mut wakes = MaintenanceWakes(0);
        wakes.mark(MaintenanceSliceKind::Observe);
        wakes.mark(MaintenanceSliceKind::Observe);
        wakes.mark(MaintenanceSliceKind::JournalPull);
        assert!(wakes.take(MaintenanceSliceKind::Observe));
        assert!(!wakes.take(MaintenanceSliceKind::Observe));
        assert!(wakes.take(MaintenanceSliceKind::JournalPull));
        assert!(!wakes.has_any());
    }

    fn incomplete_resume() -> ObserveLifecycleCursor {
        ObserveLifecycleCursor {
            pass_id: ObserveLifecyclePassId("incomplete".into()),
            last_visited: None,
        }
    }

    #[test]
    fn incomplete_observe_does_not_invent_an_unrelated_wake() {
        let mut state = MaintenanceState::default();
        for kind in MaintenanceSliceKind::ALL {
            assert!(state.wakes.take(kind));
        }
        apply_observe_pass_result(&mut state, false, false, Some(incomplete_resume()));
        assert!(state.observe_resume.is_some());
        assert!(!state.wakes.has_any());
    }

    #[test]
    fn queue_delta_pressure_restarts_a_complete_snapshot() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            botster_core_daemon::SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 4,
            },
            Vec::new(),
        );
        let mut state = MaintenanceState {
            projection,
            ..MaintenanceState::default()
        };
        state.session_family.begin_snapshot("plugin.slow", 4);
        {
            let consumer = state
                .session_family
                .consumers
                .get_mut("plugin.slow")
                .expect("consumer");
            consumer.snapshot_complete = true;
            consumer.need_snapshot_chunks = false;
            consumer.pending.clear();
        }
        let mut restarted = false;
        for index in 0..(FAMILY_QUEUE_MAX_ITEMS + 2) {
            let change = SessionLifecycleChange {
                cursor: botster_core_daemon::SessionLifecycleCursor {
                    source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                    sequence: 4 + index as u64,
                },
                kind: botster_core_daemon::SessionLifecycleChangeKind::Removed {
                    session_id: SessionId(format!("gone-{index}")),
                },
            };
            queue_family_delta(&mut state, &change);
            continue_family_fanout(&mut state, &mut HostBridgeBudget::new());
            let consumer = state
                .session_family
                .consumers
                .get("plugin.slow")
                .expect("consumer");
            if consumer.pending.front().and_then(|frame| frame.get("type"))
                == Some(&serde_json::json!("snapshot_begin"))
                && !consumer.snapshot_complete
            {
                restarted = true;
                break;
            }
        }
        assert!(restarted, "pressure must restart a snapshot");
        let consumer = state
            .session_family
            .consumers
            .get("plugin.slow")
            .expect("consumer");
        assert_eq!(
            consumer.pending.front().and_then(|frame| frame.get("type")),
            Some(&serde_json::json!("snapshot_begin"))
        );
        assert!(consumer.held_deltas.is_empty());
        assert!(!consumer.snapshot_complete);
        assert!(consumer.pending.len() < FAMILY_QUEUE_MAX_ITEMS);
    }

    #[test]
    fn ingest_baseline_pages_stay_within_owner_turn() {
        let mut projection = SessionProjection::default();
        for page in 0..16 {
            let started = Instant::now();
            let rows = (0..BASELINE_PAGE_BUDGET.max_rows)
                .map(|index| {
                    test_record(&format!(
                        "s-{:03}",
                        page * BASELINE_PAGE_BUDGET.max_rows + index
                    ))
                })
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), BASELINE_PAGE_BUDGET.max_rows);
            projection.ingest_baseline_rows(1, rows);
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "one baseline ingest page hung"
            );
        }
        projection.seal_baseline(botster_core_daemon::SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        assert_eq!(projection.rows.len(), 256);
        assert!(projection.baseline_complete);
        assert!(!projection.gap);
        assert_eq!(BASELINE_PAGE_BUDGET.max_rows, 16);
    }

    fn seed_family_consumers(state: &mut MaintenanceState, count: usize) {
        for index in 0..count {
            let plugin_key = format!("plugin.{index:02}");
            state.session_family.begin_snapshot(&plugin_key, 1);
            let consumer = state
                .session_family
                .consumers
                .get_mut(&plugin_key)
                .expect("consumer");
            consumer.snapshot_complete = true;
            consumer.need_snapshot_chunks = false;
            consumer.pending.clear();
            consumer.held_deltas.clear();
            consumer.handler = Some(PluginHandlerRef {
                plugin_key: botster_core::PluginKey(plugin_key),
                kind: PluginHandlerKind::Event,
                handler_id: "session_family".to_string(),
            });
        }
    }

    fn drain_family_fanout(state: &mut MaintenanceState) {
        let started = Instant::now();
        while !state.session_family.pending_fanout.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "family fanout hang"
            );
            continue_family_fanout(state, &mut HostBridgeBudget::new());
        }
    }

    #[test]
    fn family_fanout_keeps_ordered_frames_for_more_than_sixteen_consumers() {
        let mut projection = SessionProjection::default();
        projection.replace_complete_baseline(
            botster_core_daemon::SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 2,
            },
            Vec::new(),
        );
        let mut state = MaintenanceState {
            projection,
            ..MaintenanceState::default()
        };
        seed_family_consumers(&mut state, 20);
        for (index, id) in ["first", "second"].into_iter().enumerate() {
            queue_family_delta(
                &mut state,
                &SessionLifecycleChange {
                    cursor: botster_core_daemon::SessionLifecycleCursor {
                        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                        sequence: 3 + index as u64,
                    },
                    kind: botster_core_daemon::SessionLifecycleChangeKind::Removed {
                        session_id: SessionId(id.to_string()),
                    },
                },
            );
        }
        drain_family_fanout(&mut state);
        for index in 0..20 {
            let consumer = state
                .session_family
                .consumers
                .get(&format!("plugin.{index:02}"))
                .expect("consumer");
            let ids = consumer
                .pending
                .iter()
                .filter_map(|frame| frame.get("id")?.as_str())
                .collect::<Vec<_>>();
            assert_eq!(ids, ["first", "second"], "plugin.{index:02} lost a frame");
        }
    }

    #[test]
    fn baseline_restart_marks_every_consumer_then_starts_snapshots() {
        let mut state = MaintenanceState::default();
        seed_family_consumers(&mut state, 20);
        start_baseline_recovery(&mut state);
        let started = Instant::now();
        while state.session_family.need_gap_pass {
            assert!(started.elapsed() < Duration::from_secs(1), "gap pass hang");
            continue_gap_pass(&mut state, &mut HostBridgeBudget::new());
        }
        for index in 0..20 {
            let consumer = state
                .session_family
                .consumers
                .get(&format!("plugin.{index:02}"))
                .expect("consumer");
            assert!(consumer.gap, "plugin.{index:02} missed the gap");
        }
        begin_family_snapshots(&mut state, 9);
        while state.session_family.snapshot_start_sequence.is_some() {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "snapshot start hang"
            );
            continue_snapshot_starts(&mut state, &mut HostBridgeBudget::new());
        }
        for index in 0..20 {
            let consumer = state
                .session_family
                .consumers
                .get(&format!("plugin.{index:02}"))
                .expect("consumer");
            assert!(!consumer.gap);
            assert_eq!(
                consumer.pending.front().and_then(|frame| frame.get("type")),
                Some(&serde_json::json!("snapshot_begin"))
            );
            assert_eq!(consumer.snapshot_sequence, 9);
        }
    }

    #[test]
    fn many_plugin_owner_turn_stays_within_budget() {
        let mut state = MaintenanceState::default();
        seed_family_consumers(&mut state, 64);
        for index in 0..3 {
            queue_family_delta(
                &mut state,
                &SessionLifecycleChange {
                    cursor: botster_core_daemon::SessionLifecycleCursor {
                        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                        sequence: 4 + index,
                    },
                    kind: botster_core_daemon::SessionLifecycleChangeKind::Removed {
                        session_id: SessionId(format!("gone-{index}")),
                    },
                },
            );
        }
        let started = Instant::now();
        assert!(state.needs_work());
        continue_family_fanout(&mut state, &mut HostBridgeBudget::new());
        start_baseline_recovery(&mut state);
        continue_gap_pass(&mut state, &mut HostBridgeBudget::new());
        assert!(started.elapsed() < Duration::from_millis(MAX_OWNER_TURN_MS));
        assert!(state.session_family.has_work());
        assert!(state.session_family.need_gap_pass);
        assert_eq!(state.session_family.pending_fanout.len(), 0);
    }

    #[test]
    fn fanout_queue_pressure_starts_baseline_recovery() {
        let mut state = MaintenanceState::default();
        seed_family_consumers(&mut state, 1);
        let mut saw_pressure = false;
        for index in 0..(FANOUT_QUEUE_MAX_ITEMS + 8) {
            assert!(state.session_family.pending_fanout.len() <= FANOUT_QUEUE_MAX_ITEMS);
            assert!(state.session_family.fanout_bytes <= FANOUT_QUEUE_MAX_BYTES);
            queue_family_delta(
                &mut state,
                &SessionLifecycleChange {
                    cursor: botster_core_daemon::SessionLifecycleCursor {
                        source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                        sequence: 10 + index as u64,
                    },
                    kind: botster_core_daemon::SessionLifecycleChangeKind::Removed {
                        session_id: SessionId(format!("gone-{index}")),
                    },
                },
            );
            if state.session_family.need_gap_pass {
                saw_pressure = true;
                break;
            }
        }
        assert!(saw_pressure);
        assert!(state.session_family.pending_fanout.is_empty());
        assert_eq!(state.session_family.fanout_bytes, 0);
    }

    #[test]
    fn host_bridge_slice_stays_within_budget_with_many_plugins() {
        let data_directory = std::env::temp_dir().join(format!(
            "hub-host-bridge-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "host-bridge".to_string(),
                display_name: "Host Bridge".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = HubRuntime::new(config).expect("runtime");
        for index in 0..32 {
            runtime.insert_test_event_handler(&format!("other.{index:02}"), "other_event");
            runtime.insert_test_event_handler(&format!("session.{index:02}"), "session_family");
        }
        let mut state = MaintenanceState::default();
        let started = Instant::now();
        run_host_bridge_slice(&runtime, &mut state);
        assert!(started.elapsed() < Duration::from_millis(MAX_OWNER_TURN_MS));
        assert!(state.session_family.refresh_after.is_some() || state.session_family.has_work());
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn admission_preserves_payload_when_byte_budget_rejects() {
        let mut state = MaintenanceState::default();
        state.session_family.begin_snapshot("plugin.one", 1);
        state
            .session_family
            .consumers
            .get_mut("plugin.one")
            .expect("consumer")
            .handler = Some(PluginHandlerRef {
            plugin_key: botster_core::PluginKey("plugin.one".to_string()),
            kind: PluginHandlerKind::Event,
            handler_id: "session_family".to_string(),
        });
        let pending_before = state
            .session_family
            .consumers
            .get("plugin.one")
            .expect("consumer")
            .pending
            .clone();
        let mut budget = HostBridgeBudget {
            started: Instant::now(),
            visits: 0,
            bytes: HOST_BRIDGE_MAX_BYTES,
        };
        assert!(next_session_family_admission(&mut state, &mut budget).is_none());
        assert_eq!(
            state
                .session_family
                .consumers
                .get("plugin.one")
                .expect("consumer")
                .pending,
            pending_before
        );
    }

    #[test]
    fn fanout_stops_before_shared_byte_limit() {
        let mut state = MaintenanceState::default();
        seed_family_consumers(&mut state, 2);
        queue_family_delta(
            &mut state,
            &SessionLifecycleChange {
                cursor: botster_core_daemon::SessionLifecycleCursor {
                    source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                    sequence: 2,
                },
                kind: botster_core_daemon::SessionLifecycleChangeKind::Removed {
                    session_id: SessionId("gone".into()),
                },
            },
        );
        {
            let job = state
                .session_family
                .pending_fanout
                .front_mut()
                .expect("job");
            job.bytes = HOST_BRIDGE_MAX_BYTES / 2 + 1;
        }
        continue_family_fanout(&mut state, &mut HostBridgeBudget::new());
        let first = state
            .session_family
            .consumers
            .get("plugin.00")
            .expect("first");
        let second = state
            .session_family
            .consumers
            .get("plugin.01")
            .expect("second");
        assert_eq!(first.pending.len(), 1);
        assert!(second.pending.is_empty());
        assert_eq!(state.session_family.pending_fanout.len(), 1);
        assert_eq!(
            state
                .session_family
                .pending_fanout
                .front()
                .and_then(|job| job.after.as_deref()),
            Some("plugin.00")
        );
    }

    fn source_cursor(sequence: u64) -> SessionLifecycleCursor {
        SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence,
        }
    }

    fn sealed_maintenance(cursor: u64, watermark: Option<u64>) -> MaintenanceState {
        let mut projection = SessionProjection::default();
        projection.seal_baseline(source_cursor(cursor));
        MaintenanceState {
            projection,
            journal_source_watermark: watermark.map(source_cursor),
            journal_caught_up_confirmed: watermark.is_some(),
            ..MaintenanceState::default()
        }
    }

    fn pending_upsert(sequence: u64, id: &str) -> SessionLifecycleChange {
        SessionLifecycleChange {
            cursor: source_cursor(sequence),
            kind: botster_core_daemon::SessionLifecycleChangeKind::Upsert {
                record: test_record(id),
            },
        }
    }

    #[test]
    fn projection_caught_up_holds_while_pending_changes_remain() {
        let mut state = sealed_maintenance(0, Some(1));
        state
            .pending_changes
            .push_back(pending_upsert(1, "pending"));
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn projection_caught_up_holds_while_cursor_is_behind_the_watermark() {
        let state = sealed_maintenance(4, Some(12));
        assert!(state.pending_changes.is_empty());
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn projection_caught_up_when_baseline_matches_the_observed_watermark() {
        let state = sealed_maintenance(12, Some(12));
        assert!(state.projection_caught_up());
    }

    #[test]
    fn projection_caught_up_holds_until_acknowledged_spawns_are_projected() {
        let mut state = sealed_maintenance(12, Some(12));
        assert!(
            state.projection_caught_up(),
            "an empty pending set is caught-up before any Spawn is recorded"
        );
        state
            .acknowledged_spawn_ids
            .insert("assemble-session-00".to_string());
        assert!(!state.projection_caught_up());
        state.projection.rows.insert(
            "assemble-session-00".to_string(),
            crate::session_projection::SessionProjectionRow {
                record: test_record("assemble-session-00"),
                lifecycle_class: "current",
                live_ended: false,
                change_seq: 12,
            },
        );
        assert!(state.projection_caught_up());
    }

    #[test]
    fn projection_caught_up_after_pending_ack_is_retired_and_row_removed() {
        let data_directory = std::env::temp_dir().join(format!(
            "hub-retire-ack-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "retire-ack".to_string(),
                display_name: "Retire Ack".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = HubRuntime::new(config).expect("runtime");
        runtime.record_acknowledged_spawn("retire-session-00");
        let mut state = sealed_maintenance(12, Some(12));
        sync_acknowledged_spawns(&runtime, &mut state);
        assert!(state.acknowledged_spawn_ids.contains("retire-session-00"));
        assert!(!state.projection_caught_up());
        state.projection.rows.insert(
            "retire-session-00".to_string(),
            crate::session_projection::SessionProjectionRow {
                record: test_record("retire-session-00"),
                lifecycle_class: "current",
                live_ended: false,
                change_seq: 12,
            },
        );
        retire_projected_spawn_acks(&runtime, &mut state);
        assert!(state.acknowledged_spawn_ids.is_empty());
        assert!(runtime.acknowledged_spawn_ids().is_empty());
        state.projection.rows.remove("retire-session-00");
        assert!(
            state.projection_caught_up(),
            "a retired Spawn id must not hold later first snapshots after the row is removed"
        );
        sync_acknowledged_spawns(&runtime, &mut state);
        assert!(
            state.acknowledged_spawn_ids.is_empty(),
            "sync must not re-extend a retired Spawn id"
        );
        assert!(refresh_projection_if_inventory_ahead(&runtime, &mut state));
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn projection_caught_up_rejects_a_mismatched_source_id() {
        let mut state = sealed_maintenance(12, Some(12));
        state.journal_source_watermark = Some(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("other".into()),
            sequence: 12,
        });
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn empty_journal_pull_does_not_confirm_when_a_journal_wake_arrives() {
        assert!(journal_caught_up_after_pull(false, true, false));
        assert!(
            !journal_caught_up_after_pull(false, true, true),
            "an empty pull at the watermark is stale when a journal-advanced wake arrives"
        );
        assert!(!journal_caught_up_after_pull(true, true, false));
        assert!(!journal_caught_up_after_pull(false, false, false));
    }

    #[test]
    fn authoritative_mutation_clears_journal_catch_up_confirmation() {
        let mut state = sealed_maintenance(12, Some(12));
        assert!(state.projection_caught_up());
        state.note_authoritative_mutation();
        assert!(!state.journal_caught_up_confirmed);
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn start_baseline_recovery_clears_the_journal_watermark() {
        let mut state = sealed_maintenance(12, Some(12));
        assert!(state.projection_caught_up());
        start_baseline_recovery(&mut state);
        assert!(state.journal_source_watermark.is_none());
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn missing_pending_acks_at_the_watermark_start_one_omitted_row_recover() {
        let data_directory = std::env::temp_dir().join(format!(
            "hub-omitted-recover-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "omitted-recover".to_string(),
                display_name: "Omitted Recover".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = HubRuntime::new(config).expect("runtime");
        runtime.record_acknowledged_spawn("runtime-session");
        let mut state = sealed_maintenance(12, Some(12));
        assert!(
            state.projection_caught_up(),
            "an empty pending set is caught-up before any Spawn is recorded"
        );
        state
            .acknowledged_spawn_ids
            .insert("control-path-session".to_string());
        assert!(!state.projection_caught_up());
        assert!(
            !refresh_projection_if_inventory_ahead(&runtime, &mut state),
            "a projection that omits pending Spawn ids must start one omitted-row recover"
        );
        assert!(
            state
                .acknowledged_spawn_ids
                .contains("control-path-session"),
            "sync must union, not replace, control-path acknowledgements"
        );
        assert!(state.acknowledged_spawn_ids.contains("runtime-session"));
        assert_eq!(
            state
                .projection
                .cursor
                .as_ref()
                .map(|cursor| cursor.sequence),
            Some(0)
        );
        assert!(state.projection.baseline_complete);
        assert!(state.baseline.is_none());
        assert!(!state.journal_caught_up_confirmed);
        assert!(!state.projection_caught_up());
        if let Some(cursor) = state.projection.cursor.as_mut() {
            cursor.sequence = 12;
        }
        state.journal_caught_up_confirmed = true;
        assert!(
            !refresh_projection_if_inventory_ahead(&runtime, &mut state),
            "a second recover at the same watermark must not rewind again"
        );
        assert_eq!(
            state
                .projection
                .cursor
                .as_ref()
                .map(|cursor| cursor.sequence),
            Some(12),
            "bounded recover must not loop at the same watermark"
        );
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn cursor_expired_starts_fresh_baseline_recovery() {
        let mut state = sealed_maintenance(0, Some(12));
        state.projection.rows.insert(
            "stale-session".to_string(),
            crate::session_projection::SessionProjectionRow {
                record: test_record("stale-session"),
                lifecycle_class: "current",
                live_ended: false,
                change_seq: 0,
            },
        );
        handle_resync_reason(
            &mut state,
            SessionLifecycleResyncReason::CursorExpired {
                oldest_available_sequence: 8,
            },
        );
        assert_eq!(
            state.resync_reads, 1,
            "CursorExpired must increment lifecycle resync reads"
        );
        assert!(
            state.projection.gap,
            "CursorExpired must discard the stale projection"
        );
        assert!(!state.projection.baseline_complete);
        assert!(state.projection.rows.is_empty());
        assert!(state.projection.cursor.is_none());
        assert!(state.journal_source_watermark.is_none());
        assert!(state.baseline.is_none());
        assert!(!state.projection_caught_up());
    }

    #[test]
    fn cursor_expired_fresh_baseline_reconstructs_membership_after_discarded_prefix() {
        crate::runtime::with_test_lifecycle_journal_capacity(2, || {
            let data_directory = std::env::temp_dir().join(format!(
                "hub-expired-prefix-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            let config = crate::HubStartupOptions {
                host: crate::HostIdentityOptions {
                    id: "expired-prefix".to_string(),
                    display_name: "Expired Prefix".to_string(),
                    fingerprint: None,
                },
                data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
                session_defaults: crate::SessionDefaults {
                    shell: "/bin/sh".to_string(),
                    working_directory: Some(".".into()),
                    initial_rows: 24,
                    initial_cols: 80,
                },
                transports: crate::TransportBindings::default(),
                core_engine: crate::config::CoreEngineOptions {
                    session_worker_path: Some(
                        std::env::var_os("BOTSTER_SESSION_WORKER_BIN")
                            .map(std::path::PathBuf::from)
                            .unwrap_or_else(|| {
                                panic!(
                                    "missing required candidate path in BOTSTER_SESSION_WORKER_BIN"
                                )
                            }),
                    ),
                    ..crate::config::CoreEngineOptions::default()
                },
                ..crate::HubStartupOptions::default()
            }
            .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
            .expect("config");
            let runtime = HubRuntime::new(config).expect("runtime");
            runtime
                .spawn_session_for_test(
                    botster_core::SessionSpawnRequest {
                        request_id: RequestId("expired-gone-spawn".to_string()),
                        session_id: SessionId("gone-session".to_string()),
                        executable: "/bin/sleep".to_string(),
                        arguments: vec!["8".to_string()],
                        working_directory: botster_core::SpawnWorkingDirectory {
                            path: ".".to_string(),
                        },
                        environment: botster_core::SpawnEnvironment::default(),
                        initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                    },
                    botster_core::CoreSessionMetadata::new(),
                )
                .expect("spawn gone session");
            runtime
                .shutdown_session_for_test(SessionId("gone-session".to_string()))
                .expect("remove gone session");
            runtime
                .spawn_session_for_test(
                    botster_core::SessionSpawnRequest {
                        request_id: RequestId("expired-keep-spawn".to_string()),
                        session_id: SessionId("keep-session".to_string()),
                        executable: "/bin/sleep".to_string(),
                        arguments: vec!["8".to_string()],
                        working_directory: botster_core::SpawnWorkingDirectory {
                            path: ".".to_string(),
                        },
                        environment: botster_core::SpawnEnvironment::default(),
                        initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                    },
                    botster_core::CoreSessionMetadata::new(),
                )
                .expect("spawn keep session");
            let mut observe_state = MaintenanceState::default();
            for _ in 0..16 {
                run_maintenance_kind_to_completion(
                    &runtime,
                    &mut observe_state,
                    MaintenanceSliceKind::Observe,
                );
            }
            let current = runtime
                .lifecycle_baseline_page_for_test(None, None, BASELINE_PAGE_BUDGET)
                .expect("current source cursor");
            let mut projection = SessionProjection::default();
            projection.seal_baseline(SessionLifecycleCursor {
                source_id: current.snapshot_sequence.source_id.clone(),
                sequence: 0,
            });
            let mut state = MaintenanceState {
                projection,
                journal_source_watermark: Some(current.snapshot_sequence.clone()),
                journal_caught_up_confirmed: true,
                ..MaintenanceState::default()
            };
            state.projection.rows.insert(
                "gone-session".to_string(),
                crate::session_projection::SessionProjectionRow {
                    record: test_record("gone-session"),
                    lifecycle_class: "current",
                    live_ended: false,
                    change_seq: 0,
                },
            );
            state
                .acknowledged_spawn_ids
                .insert("keep-session".to_string());
            assert!(!state.projection_caught_up());
            run_maintenance_kind_to_completion(
                &runtime,
                &mut state,
                MaintenanceSliceKind::JournalPull,
            );
            assert!(
                state.projection.gap,
                "a discarded prefix must remint a fresh baseline, not replay the retained suffix"
            );
            assert!(!state.projection.rows.contains_key("gone-session"));
            for _ in 0..8 {
                run_maintenance_kind_to_completion(
                    &runtime,
                    &mut state,
                    MaintenanceSliceKind::HostBridge,
                );
                if state.baseline.is_some() {
                    break;
                }
            }
            assert!(
                state.baseline.is_some(),
                "CursorExpired remint must arm baseline recovery"
            );
            for _ in 0..8 {
                run_maintenance_kind_to_completion(
                    &runtime,
                    &mut state,
                    MaintenanceSliceKind::Baseline,
                );
                if state.projection.baseline_complete && state.baseline.is_none() {
                    break;
                }
            }
            assert!(
                state.projection.baseline_complete,
                "fresh baseline must seal"
            );
            assert!(
                state.projection.rows.contains_key("keep-session"),
                "fresh baseline must reconstruct the live Spawn"
            );
            let gone_still_live = state
                .projection
                .rows
                .get("gone-session")
                .is_some_and(|row| !row.live_ended && row.lifecycle_class == "current");
            assert!(
                !gone_still_live,
                "fresh baseline must not keep a discarded removal as a live current row"
            );
            assert!(
                !state.acknowledged_spawn_ids.contains("keep-session"),
                "projecting the live Spawn must release the pending hold"
            );
            for _ in 0..8 {
                run_maintenance_kind_to_completion(
                    &runtime,
                    &mut state,
                    MaintenanceSliceKind::JournalPull,
                );
                run_maintenance_kind_to_completion(
                    &runtime,
                    &mut state,
                    MaintenanceSliceKind::ProjectionApply,
                );
                if state.projection_caught_up() {
                    break;
                }
            }
            assert!(
                state.projection_caught_up(),
                "fresh baseline plus journal confirm must release first-snapshot hold"
            );
            let _ = runtime.shutdown_session_for_test(SessionId("keep-session".to_string()));
            let _ = std::fs::remove_dir_all(data_directory);
        });
    }

    #[test]
    fn projection_apply_wakes_subscribers_and_retained_family_fanout() {
        let mut state = sealed_maintenance(0, Some(1));
        for kind in MaintenanceSliceKind::ALL {
            let _ = state.wakes.take(kind);
        }
        state.pending_changes.push_back(pending_upsert(1, "new"));
        run_projection_apply_slice(None, &mut state);
        assert!(state.wakes.take(MaintenanceSliceKind::SubscriberDelivery));
        assert!(state.wakes.take(MaintenanceSliceKind::HostBridge));
        assert_eq!(state.session_family.pending_fanout.len(), 1);
        assert!(state.projection.rows.contains_key("new"));
    }

    #[test]
    fn twenty_four_pending_changes_reach_completion_across_unchanged_apply_slices() {
        assert_eq!(APPLY_MAX_CHANGES, 16);
        assert_eq!(JOURNAL_PAGE_MAX_CHANGES, 16);
        let mut state = sealed_maintenance(0, Some(24));
        state.journal_caught_up_confirmed = false;
        for index in 0..24 {
            state
                .pending_changes
                .push_back(pending_upsert(index as u64 + 1, &format!("row-{index:02}")));
        }
        assert!(!state.projection_caught_up());
        run_projection_apply_slice(None, &mut state);
        assert_eq!(state.pending_changes.len(), 8);
        assert_eq!(
            state
                .projection
                .cursor
                .as_ref()
                .map(|cursor| cursor.sequence),
            Some(16)
        );
        assert!(!state.projection_caught_up());
        run_projection_apply_slice(None, &mut state);
        assert!(state.pending_changes.is_empty());
        assert_eq!(state.projection.rows.len(), 24);
        assert!(
            !state.projection_caught_up(),
            "apply to the watermark still requires a confirming empty journal pull"
        );
        state.journal_caught_up_confirmed = true;
        assert!(state.projection_caught_up());
    }

    #[test]
    fn late_projection_recovers_more_than_one_baseline_page_to_the_watermark() {
        let data_directory = std::env::temp_dir().join(format!(
            "hub-late-projection-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "late-projection".to_string(),
                display_name: "Late Projection".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let runtime = HubRuntime::new(config).expect("runtime");
        let expected = (0..20)
            .map(|index| format!("recover-session-{index:02}"))
            .collect::<BTreeSet<_>>();
        for id in &expected {
            runtime
                .spawn_session_for_test(
                    botster_core::SessionSpawnRequest {
                        request_id: RequestId(format!("recover-spawn-{id}")),
                        session_id: SessionId(id.clone()),
                        executable: "/bin/sleep".to_string(),
                        arguments: vec!["8".to_string()],
                        working_directory: botster_core::SpawnWorkingDirectory {
                            path: ".".to_string(),
                        },
                        environment: botster_core::SpawnEnvironment::default(),
                        initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                    },
                    botster_core::CoreSessionMetadata::new(),
                )
                .expect("spawn recovery session");
        }
        let mut state = MaintenanceState::default();
        start_baseline_recovery(&mut state);
        for _ in 0..8 {
            run_maintenance_kind_to_completion(
                &runtime,
                &mut state,
                MaintenanceSliceKind::HostBridge,
            );
            if state.baseline.is_some() {
                break;
            }
        }
        assert!(
            state.baseline.is_some(),
            "late projection must arm baseline recovery after the gap pass"
        );
        for _ in 0..8 {
            run_maintenance_kind_to_completion(
                &runtime,
                &mut state,
                MaintenanceSliceKind::Baseline,
            );
            if state.projection.baseline_complete && state.baseline.is_none() {
                break;
            }
        }
        assert!(
            state.projection.baseline_complete,
            "late projection must seal baseline pages"
        );
        assert!(
            state.projection.cursor.is_some(),
            "seal must keep a snapshot cursor"
        );
        assert!(
            !state.projection_caught_up(),
            "seal must not complete the journal consume"
        );
        assert!(
            state.projection.rows.len() > BASELINE_PAGE_BUDGET.max_rows,
            "recovery fixture must exceed one baseline page"
        );
        for _ in 0..16 {
            run_maintenance_kind_to_completion(
                &runtime,
                &mut state,
                MaintenanceSliceKind::JournalPull,
            );
            run_maintenance_kind_to_completion(
                &runtime,
                &mut state,
                MaintenanceSliceKind::ProjectionApply,
            );
            if state.projection_caught_up() {
                break;
            }
        }
        assert!(
            state.projection_caught_up(),
            "late projection must consume the journal to the source watermark"
        );
        let recovered = state
            .projection
            .rows
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(
            expected.is_subset(&recovered),
            "late projection missing {:?}; recovered={}",
            expected.difference(&recovered).cloned().collect::<Vec<_>>(),
            recovered.len()
        );
        for id in &expected {
            let _ = runtime.shutdown_session_for_test(SessionId(id.clone()));
        }
        let _ = std::fs::remove_dir_all(data_directory);
    }

    fn projection_dirty_alone_does_not_keep_maintenance_pending() {
        let mut state = sealed_maintenance(1, Some(1));
        state.projection_dirty = true;
        for kind in MaintenanceSliceKind::ALL {
            let _ = state.wakes.take(kind);
        }
        assert!(
            !state.needs_work(),
            "dirty is a delivery hint, not a self-rearming maintenance wake"
        );
    }

    #[test]
    fn observe_resume_alone_does_not_keep_maintenance_pending() {
        let mut state = sealed_maintenance(1, Some(1));
        for kind in MaintenanceSliceKind::ALL {
            let _ = state.wakes.take(kind);
        }
        state.observe_resume = Some(ObserveLifecycleCursor {
            pass_id: ObserveLifecyclePassId("1".into()),
            last_visited: None,
        });
        assert!(
            !state.needs_work(),
            "observe_resume is Observe continuation, not a nine-kind self-wake"
        );
    }

    #[test]
    fn sealed_baseline_unavailable_observe_clears_cursor_without_remint_or_spin() {
        let mut state = MaintenanceState::default();
        state
            .projection
            .seal_baseline(botster_core_daemon::SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 1,
            });
        state.observe_resume = Some(ObserveLifecycleCursor {
            pass_id: ObserveLifecyclePassId("1".into()),
            last_visited: None,
        });
        state.journal_caught_up_confirmed = true;
        for kind in MaintenanceSliceKind::ALL {
            let _ = state.wakes.take(kind);
        }
        handle_unavailable_observe_pass(&mut state);
        assert!(state.observe_resume.is_none());
        assert!(state.baseline.is_none());
        assert_eq!(state.baseline_page_reads, 0);
        assert!(
            !state.needs_work(),
            "sealed unavailable observe must not spin maintenance"
        );
    }

    #[test]
    fn incomplete_baseline_unavailable_observe_starts_recovery() {
        let mut state = MaintenanceState::default();
        assert!(!state.projection.baseline_complete);
        state.observe_resume = Some(ObserveLifecycleCursor {
            pass_id: ObserveLifecyclePassId("1".into()),
            last_visited: None,
        });
        handle_unavailable_observe_pass(&mut state);
        assert!(state.observe_resume.is_none());
        assert!(
            state.session_family.need_gap_pass,
            "incomplete baseline must start recovery, which begins with the gap pass"
        );
        assert!(!state.journal_caught_up_confirmed);
        assert_eq!(state.baseline_page_reads, 0);
        assert!(state.needs_work());
    }

    fn event_delivery_runtime(name: &str) -> (HubRuntime, std::path::PathBuf) {
        let data_directory = std::env::temp_dir().join(format!(
            "hub-event-delivery-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: format!("event-delivery-{name}"),
                display_name: "Event Delivery".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        (HubRuntime::new(config).expect("runtime"), data_directory)
    }

    fn subscribe_worktree_consumer(runtime: &HubRuntime, plugin_key: &str) {
        assert_eq!(
            runtime.package_event_router().try_subscribe(
                crate::package_event_router::EventSubscription {
                    plugin_key: plugin_key.to_string(),
                    owner: crate::package_event_router::HUB_EVENT_OWNER.to_string(),
                    name: "worktree_created".to_string(),
                    handler_id: "worktree_created".to_string(),
                    generation: 1,
                    ..crate::package_event_router::EventSubscription::default()
                }
            ),
            crate::package_event_router::EventPlaneStatus::Accepted
        );
    }

    fn ingress_worktree_created(runtime: &HubRuntime) {
        assert_eq!(
            runtime.package_event_router().try_ingress(
                crate::package_event_router::HUB_EVENT_OWNER,
                "worktree_created",
                &serde_json::json!({ "event": "worktree_created" }),
                Instant::now()
            ),
            crate::package_event_router::EventPlaneStatus::Accepted
        );
    }

    #[test]
    fn package_event_slice_consumes_pull_token_when_handler_is_missing() {
        let (runtime, data_directory) = event_delivery_runtime("missing-handler");
        subscribe_worktree_consumer(&runtime, "consumer");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        assert!(state.event_in_flight.is_empty());
        assert!(state.pending_retirements.is_empty());
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn package_event_slice_consumes_pull_token_on_admission_rejection() {
        let (runtime, data_directory) = event_delivery_runtime("admit-reject");
        subscribe_worktree_consumer(&runtime, "consumer");
        runtime.insert_test_event_handler("consumer", "worktree_created");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        assert!(state.event_in_flight.is_empty());
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn package_event_slice_consumes_pull_token_when_causal_mint_fails() {
        let (runtime, data_directory) = event_delivery_runtime("causal-mint");
        subscribe_worktree_consumer(&runtime, "consumer");
        runtime.insert_test_event_handler("consumer", "worktree_created");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        runtime.causal_scopes().test_with_inner_held(|| {
            run_package_event_delivery_slice(&runtime, &mut state);
        });
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        assert!(state.event_in_flight.is_empty());
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn queued_event_flight_retirement_consumes_the_pull_token() {
        let (runtime, data_directory) = event_delivery_runtime("queued-complete");
        subscribe_worktree_consumer(&runtime, "consumer");
        ingress_worktree_created(&runtime);
        let mut batch = runtime
            .package_event_router()
            .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
            .expect("pull");
        let delivery = batch.pop().expect("one pulled copy");
        runtime
            .package_event_router()
            .note_admitted(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
            .expect("admit");
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 1);
        let mut flight = event_flight(&delivery, Some(1), "package-event-queued".to_string());
        let busy = runtime
            .package_event_router()
            .test_with_inner_held(|| retire_event_holder(&runtime, &mut flight));
        assert!(!busy);
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 1);
        assert!(flight.pull_id.is_some());
        assert!(retire_event_holder(&runtime, &mut flight));
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        assert!(flight.pull_id.is_none());
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn package_event_fanout_keeps_per_holder_request_ids_and_occupancy() {
        let (runtime, data_directory) = event_delivery_runtime("fanout");
        subscribe_worktree_consumer(&runtime, "consumer-a");
        subscribe_worktree_consumer(&runtime, "consumer-b");
        runtime.insert_test_event_handler("consumer-a", "worktree_created");
        runtime.insert_test_event_handler("consumer-b", "worktree_created");
        ingress_worktree_created(&runtime);
        let mut batch = runtime
            .package_event_router()
            .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
            .expect("pull");
        assert_eq!(batch.len(), 2);
        let mut state = MaintenanceState::default();
        let mut scopes = Vec::new();
        let mut request_ids = BTreeSet::new();
        for delivery in batch.drain(..) {
            let request_id = package_event_request_id(&delivery);
            assert!(
                request_ids.insert(request_id.0.clone()),
                "each pulled copy must keep a unique request id: {}",
                request_id.0
            );
            runtime
                .package_event_router()
                .note_admitted(
                    delivery.envelope_id,
                    &delivery.holder.plugin_key,
                    delivery.holder.generation,
                )
                .expect("admit");
            let identity = crate::package_event_router::LeaseIdentity::EventInFlight;
            let scope_id = runtime
                .causal_scopes()
                .mint_with_lease(Some(identity))
                .expect("mint");
            scopes.push(scope_id);
            state.event_in_flight.insert(
                request_id.0.clone(),
                event_flight(&delivery, Some(scope_id), request_id.0),
            );
        }
        assert_eq!(state.event_in_flight.len(), 2);
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 2);
        let ids: Vec<String> = state.event_in_flight.keys().cloned().collect();
        for request_id in ids {
            let mut flight = state
                .event_in_flight
                .remove(&request_id)
                .expect("flight stays until its completion");
            assert!(retire_event_holder(&runtime, &mut flight));
        }
        assert!(state.event_in_flight.is_empty());
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
            assert!(
                Instant::now() < deadline,
                "the owner must apply admitted releases"
            );
            std::thread::yield_now();
        }
        for scope_id in scopes {
            assert!(
                !runtime.causal_scopes().is_live(scope_id),
                "completion must release the holder causal scope"
            );
        }
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.admitted_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn package_event_slice_retires_both_fanout_holders() {
        let (runtime, data_directory) = event_delivery_runtime("fanout-slice");
        subscribe_worktree_consumer(&runtime, "consumer-a");
        subscribe_worktree_consumer(&runtime, "consumer-b");
        runtime.insert_test_event_handler("consumer-a", "worktree_created");
        runtime.insert_test_event_handler("consumer-b", "worktree_created");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        assert!(state.event_in_flight.is_empty());
        assert!(state.pending_retirements.is_empty());
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.admitted_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    fn install_event_probe_registry(name: &str) -> (crate::packages::PackageRegistry, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "hub-event-probe-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create event probe package root");
        std::fs::write(
            root.join("plugin.lua"),
            r#"
events.on("hub", "worktree_created", function(event)
  return {
    received = event.event,
    worktree_id = event.worktree_id,
    target_id = event.target_id,
  }
end)

events.on("hub", "worktree_created", function(event)
  return {
    received = event.event,
    observer = "second",
    worktree_id = event.worktree_id,
  }
end)

return botster.register({})
"#,
        )
        .expect("write event probe plugin");
        std::fs::write(
            root.join("botster-package.json"),
            serde_json::json!({
                "name": "event-probe.plugin",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": root.display().to_string() },
                "capabilities": [],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            })
            .to_string(),
        )
        .expect("write event probe manifest");
        let mut policy = crate::default_package_policy();
        policy
            .install_local_path(&root, "install event probe lua package")
            .expect("install event probe package");
        policy
            .enable("event-probe.plugin", "enable event probe package")
            .expect("enable event probe package");
        (policy.registry().clone(), root)
    }

    #[test]
    fn owner_loop_queues_and_completes_two_fanout_plugin_handlers() {
        let (registry, package_root) = install_event_probe_registry("owner-loop-fanout");
        let (mut runtime, data_directory) = event_delivery_runtime("owner-loop-fanout");
        runtime
            .load_lua_plugin_package(&registry, "event-probe.plugin")
            .expect("load event probe plugin");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        let queue_deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < queue_deadline && state.event_in_flight.len() < 2 {
            run_package_event_delivery_slice(&runtime, &mut state);
            if state.event_in_flight.len() < 2 {
                runtime.package_event_router().set_delivery_wake();
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        assert_eq!(
            state.event_in_flight.len(),
            2,
            "both plugin handlers must queue through Core: {state:?}"
        );
        let request_ids: Vec<String> = state.event_in_flight.keys().cloned().collect();
        assert_ne!(request_ids[0], request_ids[1]);
        assert!(
            request_ids
                .iter()
                .all(|id| id.starts_with("package-event-hub-worktree_created-")),
            "production request ids must stay on the owner-loop format: {request_ids:?}"
        );
        let scopes: Vec<u64> = state
            .event_in_flight
            .values()
            .filter_map(|flight| flight.scope_id)
            .collect();
        assert_eq!(scopes.len(), 2);
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 2);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline
            && (!state.event_in_flight.is_empty() || !state.pending_retirements.is_empty())
        {
            run_completion_drain_slice(&runtime, &mut state);
            if !state.event_in_flight.is_empty() || !state.pending_retirements.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        assert!(
            state.event_in_flight.is_empty(),
            "both Core completions must retire their flights: {state:?}"
        );
        assert!(state.pending_retirements.is_empty());
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        for scope_id in scopes {
            assert!(
                !runtime.causal_scopes().is_live(scope_id),
                "completion must release causal scope {scope_id}"
            );
        }
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(snapshot.queued_holders, 0);
        assert_eq!(snapshot.admitted_holders, 0);
        assert_eq!(snapshot.global_in_flight_bytes, 0);
        let _ = std::fs::remove_dir_all(package_root);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn insufficient_nonzero_result_capacity_waits_for_release_before_drain_progress() {
        let (registry, package_root) = install_lua_event_plugin(
            "capacity-block",
            r#"
events.on("hub", "worktree_created", function(event)
  return { received = event.event, body = string.rep("x", 4096) }
end)
return botster.register({})
"#,
        );
        let (mut runtime, data_directory) = event_delivery_runtime("capacity-block");
        runtime
            .load_lua_plugin_package(&registry, "event-probe.plugin")
            .expect("load capacity-block plugin");
        ingress_worktree_created(&runtime);
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        assert_eq!(state.event_in_flight.len(), 1);

        let publish_deadline = Instant::now() + Duration::from_secs(2);
        while !runtime
            .drain_plugin_completions(0, usize::MAX)
            .has_remaining
        {
            assert!(
                Instant::now() < publish_deadline,
                "Core did not publish the capacity-block completion"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let budget = crate::daemon::control::reply::RetainedPluginResultBudget::new();
        let blocker = budget
            .try_reserve(crate::daemon::control::reply::RETAINED_PLUGIN_RESULT_BYTE_CAPACITY - 128)
            .expect("reserve all but a nonzero tail");
        let blocked = run_completion_drain_slice_with_controls(&runtime, &mut state, &budget);
        assert_eq!(blocked.item_count, 0);
        assert!(blocked.has_remaining);
        assert_eq!(budget.available_bytes(), 128);
        assert!(
            !budget.take_release_notification(),
            "unchanged capacity must not create a retry wake"
        );
        assert_eq!(
            state.event_in_flight.len(),
            1,
            "a zero-progress drain must leave the completion pending"
        );

        drop(blocker);
        assert!(
            budget.take_release_notification(),
            "dropping retained storage must publish the resume wake"
        );
        let resumed = run_completion_drain_slice_with_controls(&runtime, &mut state, &budget);
        assert_eq!(resumed.item_count, 1);
        assert!(!resumed.has_remaining);
        assert!(state.event_in_flight.is_empty());

        let _ = std::fs::remove_dir_all(package_root);
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn backpressured_event_admission_retains_release_when_causal_table_is_full() {
        use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
        use crate::runtime::CAUSAL_OWNER_CAPACITY;

        let (runtime, data_directory) = event_delivery_runtime("backpressure-causal-release");
        subscribe_worktree_consumer(&runtime, "consumer");
        runtime.insert_test_event_handler("consumer", "worktree_created");
        ingress_worktree_created(&runtime);
        runtime.set_test_plugin_admit_backpressure(true);
        let scopes = runtime.causal_scopes();
        for _ in 0..CAUSAL_OWNER_CAPACITY {
            assert_eq!(
                runtime.admit_causal_op(CausalOp::Release {
                    scope_id: u64::MAX,
                    identity: LeaseIdentity::EventInFlight,
                }),
                CausalAdmitResult::Applied
            );
        }
        let before = scopes.mint().expect("scope before event admission");
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        let after = scopes.mint().expect("scope after refused event admission");
        assert_eq!(
            after - before,
            1,
            "full capacity must prevent event lease acquisition"
        );
        assert!(state.event_causal_blocked);
        assert_eq!(
            runtime
                .package_event_router()
                .snapshot()
                .unwrap()
                .queued_holders,
            1
        );
        assert!(state.pending_retirements.is_empty());
        runtime.apply_causal_owner_ops();
        assert!(runtime.take_causal_capacity_notification());
        state.note_causal_capacity_progress();
        run_package_event_delivery_slice(&runtime, &mut state);
        let resumed = scopes.mint().expect("scope after resumed event admission");
        assert_eq!(
            resumed - after,
            2,
            "resumed admission acquires one event scope"
        );
        assert_eq!(runtime.causal_operation_count(), CAUSAL_OWNER_CAPACITY);
        let old_scope = after + 1;
        runtime.apply_causal_owner_ops();
        assert!(runtime.take_causal_capacity_notification());
        state.note_causal_capacity_progress();
        run_package_event_delivery_slice(&runtime, &mut state);
        let new_scope = resumed + 1;
        assert_eq!(scopes.lease_count(old_scope), Some(1));
        assert_eq!(scopes.lease_count(new_scope), Some(1));
        assert_eq!(runtime.causal_operation_count(), CAUSAL_OWNER_CAPACITY);
        while runtime.causal_operation_count() > 2 {
            runtime.apply_causal_owner_ops();
        }
        runtime.apply_causal_owner_ops();
        assert!(!scopes.is_live(old_scope));
        assert!(
            scopes.is_live(new_scope),
            "the old event release must preserve the scope from the next admission"
        );
        assert_eq!(
            runtime
                .package_event_router()
                .snapshot()
                .unwrap()
                .queued_holders,
            1
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while runtime.causal_owner_ops_pending() || !state.pending_retirements.is_empty() {
            assert!(Instant::now() < deadline, "the event release must complete");
            let _ = runtime.apply_event_plane_owner_ops();
            if runtime.take_causal_capacity_notification() {
                state.note_causal_capacity_progress();
            }
            flush_pending_event_retirements(&runtime, &mut state);
        }
        assert_eq!(
            scopes.identities(after + 1),
            None,
            "the refused event must release its scope"
        );
        assert!(!scopes.is_live(new_scope));
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn backpressured_admission_requeues_holder_instead_of_retiring() {
        let (runtime, data_directory) = event_delivery_runtime("backpressure-requeue");
        subscribe_worktree_consumer(&runtime, "consumer");
        runtime.insert_test_event_handler("consumer", "worktree_created");
        ingress_worktree_created(&runtime);
        runtime.set_test_plugin_admit_backpressure(true);
        let mut state = MaintenanceState::default();
        run_package_event_delivery_slice(&runtime, &mut state);
        assert!(
            state.event_in_flight.is_empty(),
            "Backpressured admission must not mark the delivery in-flight: {state:?}"
        );
        assert_eq!(runtime.package_event_router().test_outstanding_pulls(), 0);
        let snapshot = runtime.package_event_router().snapshot().expect("snapshot");
        assert_eq!(
            snapshot.queued_holders, 1,
            "transient Backpressured must requeue the holder; retiring it (the catch-all arm) makes this 0"
        );
        assert_eq!(snapshot.admitted_holders, 0);
        assert!(
            snapshot.global_in_flight_bytes > 0,
            "requeued occupancy must remain until a later slice admits or expires the copy"
        );
        runtime.set_test_plugin_admit_backpressure(false);
        runtime.package_event_router().set_delivery_wake();
        run_package_event_delivery_slice(&runtime, &mut state);
        let after = runtime.package_event_router().snapshot().expect("after");
        assert_eq!(
            after.queued_holders, 0,
            "a later slice must consume the requeued holder rather than lose it"
        );
        let _ = std::fs::remove_dir_all(data_directory);
    }

    fn install_lua_event_plugin(
        name: &str,
        lua: &str,
    ) -> (crate::packages::PackageRegistry, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "hub-event-lua-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create lua event plugin root");
        std::fs::write(root.join("plugin.lua"), lua).expect("write lua event plugin");
        std::fs::write(
            root.join("botster-package.json"),
            serde_json::json!({
                "name": "event-probe.plugin",
                "version": "1.0.0",
                "kind": "plugin",
                "botster": ">=0.1.0",
                "source": { "type": "path", "path": root.display().to_string() },
                "capabilities": [],
                "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
            })
            .to_string(),
        )
        .expect("write lua event manifest");
        let mut policy = crate::default_package_policy();
        policy
            .install_local_path(&root, "install lua event package")
            .expect("install lua event package");
        policy
            .enable("event-probe.plugin", "enable lua event package")
            .expect("enable lua event package");
        (policy.registry().clone(), root)
    }

    fn drain_event_flights(runtime: &HubRuntime, state: &mut MaintenanceState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline
            && (!state.event_in_flight.is_empty() || !state.pending_retirements.is_empty())
        {
            run_completion_drain_slice(runtime, state);
            if !state.event_in_flight.is_empty() || !state.pending_retirements.is_empty() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[test]
    fn t1_hold_seam_times_out_distinct_from_handler_failure() {
        let hold_lua = r#"
events.on("hub", "worktree_created", function(event)
  return { received = event.event }
end)
return botster.register({})
"#;
        let fail_lua = r#"
events.on("hub", "worktree_created", function(event)
  error("handler boom")
end)
return botster.register({})
"#;

        let (timeout_registry, timeout_root) = install_lua_event_plugin("t1-hold", hold_lua);
        let (mut timeout_runtime, timeout_dir) = event_delivery_runtime("t1-hold");
        timeout_runtime
            .load_lua_plugin_package(&timeout_registry, "event-probe.plugin")
            .expect("load hold plugin");
        ingress_worktree_created(&timeout_runtime);
        // Hold the handler past the invocation bound so the classification is
        // a timeout, not a handler failure.
        crate::lua_runtime::TEST_EVENT_HANDLER_HOLD_MS.store(
            EVENT_INVOCATION_TIMEOUT_MS + 500,
            std::sync::atomic::Ordering::Relaxed,
        );
        let mut timeout_state = MaintenanceState::default();
        run_package_event_delivery_slice(&timeout_runtime, &mut timeout_state);
        assert_eq!(timeout_state.event_in_flight.len(), 1);
        drain_event_flights(&timeout_runtime, &mut timeout_state);
        crate::lua_runtime::TEST_EVENT_HANDLER_HOLD_MS
            .store(0, std::sync::atomic::Ordering::Relaxed);
        let timeout_snap = timeout_runtime.event_plane_counters_snapshot();
        assert_eq!(timeout_snap.event_handler_timed_out, 1);
        assert_eq!(timeout_snap.event_handler_failed, 0);
        assert_eq!(timeout_snap.event_handler_cancelled, 0);
        assert_eq!(timeout_snap.event_handler_backpressured, 0);
        assert_eq!(timeout_snap.event_handler_worker_stopped, 0);

        let (fail_registry, fail_root) = install_lua_event_plugin("t1-fail", fail_lua);
        let (mut fail_runtime, fail_dir) = event_delivery_runtime("t1-fail");
        fail_runtime
            .load_lua_plugin_package(&fail_registry, "event-probe.plugin")
            .expect("load fail plugin");
        ingress_worktree_created(&fail_runtime);
        let mut fail_state = MaintenanceState::default();
        run_package_event_delivery_slice(&fail_runtime, &mut fail_state);
        assert_eq!(fail_state.event_in_flight.len(), 1);
        drain_event_flights(&fail_runtime, &mut fail_state);
        let fail_snap = fail_runtime.event_plane_counters_snapshot();
        assert_eq!(fail_snap.event_handler_timed_out, 0);
        assert_eq!(fail_snap.event_handler_failed, 1);
        assert_eq!(fail_snap.event_handler_cancelled, 0);

        let _ = std::fs::remove_dir_all(timeout_root);
        let _ = std::fs::remove_dir_all(timeout_dir);
        let _ = std::fs::remove_dir_all(fail_root);
        let _ = std::fs::remove_dir_all(fail_dir);
    }

    fn event_failure_completion(
        request_id: &str,
        kind: PluginInvocationFailureKind,
    ) -> botster_core::PluginCompletion {
        let reason = format!("{kind:?}");
        botster_core::PluginCompletion {
            class: PluginInvocationClass::Background,
            result: PluginInvocationResult::Failed(botster_core::PluginInvocationFailure {
                request_id: RequestId(request_id.to_string()),
                handler: PluginHandlerRef {
                    plugin_key: botster_core::PluginKey("consumer".into()),
                    kind: PluginHandlerKind::Event,
                    handler_id: "worktree_created".into(),
                },
                kind,
                timeout_ms: Some(1),
                reason,
            }),
        }
    }

    #[test]
    fn t1_timed_out_is_distinct_from_handler_failed() {
        let (runtime, data_directory) = event_delivery_runtime("t1-kind");
        subscribe_worktree_consumer(&runtime, "consumer");
        ingress_worktree_created(&runtime);
        let mut batch = runtime
            .package_event_router()
            .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
            .expect("pull");
        let delivery = batch.pop().expect("one pulled copy");
        runtime
            .package_event_router()
            .note_admitted(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
            .expect("admit");
        let request_id = package_event_request_id(&delivery);
        let mut state = MaintenanceState::default();
        state.event_in_flight.insert(
            request_id.0.clone(),
            event_flight(&delivery, None, request_id.0.clone()),
        );
        apply_plugin_completion(
            &runtime,
            &mut state,
            &event_failure_completion(&request_id.0, PluginInvocationFailureKind::TimedOut),
        );
        let snap = runtime.event_plane_counters_snapshot();
        assert_eq!(snap.event_handler_timed_out, 1);
        assert_eq!(snap.event_handler_failed, 0);
        assert_eq!(snap.event_handler_cancelled, 0);
        assert_eq!(snap.event_handler_backpressured, 0);
        assert_eq!(snap.event_handler_worker_stopped, 0);
        assert!(state.event_in_flight.is_empty());

        ingress_worktree_created(&runtime);
        let mut batch = runtime
            .package_event_router()
            .pull_ready_batch(8, 64 * 1024, Instant::now(), Duration::from_millis(8))
            .expect("pull failed case");
        let delivery = batch.pop().expect("failed-case copy");
        runtime
            .package_event_router()
            .note_admitted(
                delivery.envelope_id,
                &delivery.holder.plugin_key,
                delivery.holder.generation,
            )
            .expect("admit failed case");
        let request_id = package_event_request_id(&delivery);
        state.event_in_flight.insert(
            request_id.0.clone(),
            event_flight(&delivery, None, request_id.0.clone()),
        );
        apply_plugin_completion(
            &runtime,
            &mut state,
            &event_failure_completion(&request_id.0, PluginInvocationFailureKind::HandlerFailed),
        );
        let snap = runtime.event_plane_counters_snapshot();
        assert_eq!(snap.event_handler_timed_out, 1);
        assert_eq!(snap.event_handler_failed, 1);
        assert_eq!(snap.event_handler_cancelled, 0);
        let _ = std::fs::remove_dir_all(data_directory);
    }
}
