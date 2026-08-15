//! Bounded owner-loop maintenance slices for Hub session projection.
//!
//! One owner turn runs at most one slice, then yields. This module does not
//! import terminal semantic bodies and does not name package-owned product
//! policy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use botster_core::SessionId;
use botster_core::{
    BoundaryJson, PluginAdmissionResult, PluginHandlerKind, PluginHandlerRef,
    PluginInvocationClass, PluginInvocationContext, PluginInvocationRequest,
    PluginInvocationResult, RequestId,
};
use botster_core_daemon::{
    LifecycleBaselineBudget, ObserveLifecycleBudget, ObserveLifecycleCursor,
    SessionLifecycleChange, SessionLifecycleCursor, SessionLifecyclePageError,
    SessionLifecycleResyncReason,
};

use crate::HubRuntime;
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
const SESSION_CHUNK_MAX_ITEMS: usize = 8;
const SESSION_CHUNK_MAX_BYTES: usize = 32 * 1024;
const COMPLETION_DRAIN_MAX_ITEMS: usize = 8;
const COMPLETION_DRAIN_MAX_BYTES: usize = 32 * 1024;
const FAMILY_QUEUE_MAX_ITEMS: usize = 32;
const FAMILY_QUEUE_MAX_BYTES: usize = 64 * 1024;
const CONSUMER_REFRESH_MAX: usize = 8;
const CONSUMER_REFRESH_MAX_ELAPSED: Duration = Duration::from_millis(8);

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceSliceKind {
    Observe,
    JournalPull,
    ProjectionApply,
    Baseline,
    HostBridge,
    SubscriberDelivery,
    CompletionDrain,
    ProviderResync,
}

impl MaintenanceSliceKind {
    fn next(self) -> Self {
        match self {
            Self::Observe => Self::JournalPull,
            Self::JournalPull => Self::ProjectionApply,
            Self::ProjectionApply => Self::Baseline,
            Self::Baseline => Self::HostBridge,
            Self::HostBridge => Self::SubscriberDelivery,
            Self::SubscriberDelivery => Self::CompletionDrain,
            Self::CompletionDrain => Self::ProviderResync,
            Self::ProviderResync => Self::Observe,
        }
    }
}

/// Coalesced wake plus the next slice to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceScheduler {
    wake: bool,
    next: MaintenanceSliceKind,
}

impl Default for MaintenanceScheduler {
    fn default() -> Self {
        Self {
            wake: true,
            next: MaintenanceSliceKind::Observe,
        }
    }
}

impl MaintenanceScheduler {
    /// Set the coalesced wake bit. O(1).
    pub fn try_wake(&mut self) {
        self.wake = true;
    }

    /// After an authoritative mutation, pull journal changes next.
    pub fn prefer_journal_pull(&mut self) {
        self.wake = true;
        self.next = MaintenanceSliceKind::JournalPull;
    }

    /// After a session subscriber registers, deliver a bounded page next.
    pub fn prefer_subscriber_delivery(&mut self) {
        self.wake = true;
        self.next = MaintenanceSliceKind::SubscriberDelivery;
    }

    /// True when an idle owner turn should run one slice.
    #[must_use]
    pub fn has_wake(&self) -> bool {
        self.wake
    }

    /// Consume the current slice and advance the round-robin pointer.
    pub fn take_slice(&mut self) -> MaintenanceSliceKind {
        let kind = self.next;
        self.next = kind.next();
        self.wake = false;
        kind
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
    gap_after: Option<String>,
    need_gap_pass: bool,
    snapshot_start_after: Option<String>,
    snapshot_start_sequence: Option<u64>,
    admit_after: Option<String>,
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
    pub scheduler: MaintenanceScheduler,
    pub projection: SessionProjection,
    pub observe_resume: Option<ObserveLifecycleCursor>,
    pub pending_changes: VecDeque<SessionLifecycleChange>,
    pub baseline: Option<BaselineRecovery>,
    pub session_family: SessionFamilyBridge,
    pub last_owner_turn: Duration,
    pub journal_page_reads: u64,
    pub baseline_page_reads: u64,
    pub projection_dirty: bool,
}

impl MaintenanceState {
    /// Coalesce one O(1) wake after an authoritative mutation.
    pub fn try_wake(&mut self) {
        self.scheduler.try_wake();
    }

    /// After an authoritative mutation, pull the journal on the next idle turn.
    pub fn note_authoritative_mutation(&mut self) {
        self.scheduler.prefer_journal_pull();
    }

    pub fn needs_work(&self) -> bool {
        self.scheduler.has_wake()
            || self.baseline.is_some()
            || !self.pending_changes.is_empty()
            || self.observe_resume.is_some()
            || self.session_family.has_work()
    }
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
    kind: MaintenanceSliceKind,
) {
    match kind {
        MaintenanceSliceKind::Observe => run_observe_slice(runtime, state),
        MaintenanceSliceKind::JournalPull => run_journal_pull_slice(runtime, state),
        MaintenanceSliceKind::ProjectionApply => run_projection_apply_slice(state),
        MaintenanceSliceKind::Baseline => run_baseline_slice(runtime, state),
        MaintenanceSliceKind::HostBridge => run_host_bridge_slice(runtime, state),
        MaintenanceSliceKind::CompletionDrain => run_completion_drain_slice(runtime, state),
        MaintenanceSliceKind::SubscriberDelivery | MaintenanceSliceKind::ProviderResync => {}
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn run_observe_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let resume = state.observe_resume.clone();
    match runtime.observe_lifecycle_slice(now_seconds(), resume.as_ref(), OBSERVE_SLICE_BUDGET) {
        Ok(slice) => {
            if let Some(reason) = slice.resync_required {
                if matches!(
                    reason,
                    SessionLifecycleResyncReason::ObservePassUnavailable
                        | SessionLifecycleResyncReason::SourceChanged
                ) {
                    state.observe_resume = None;
                    start_baseline_recovery(state);
                }
                return;
            }
            if slice.complete {
                state.observe_resume = None;
            } else {
                state.observe_resume = Some(ObserveLifecycleCursor {
                    pass_id: slice.pass_id,
                    last_visited: slice.last_visited,
                });
                state.scheduler.try_wake();
            }
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.scheduler.try_wake();
        }
        Err(_) => state.scheduler.try_wake(),
    }
}

fn run_journal_pull_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let woke = runtime.take_journal_advanced_wake();
    if state.baseline.is_some() || !state.projection.baseline_complete {
        if woke && state.baseline.is_none() {
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
    match runtime.lifecycle_changes_page(&cursor, JOURNAL_PAGE_MAX_CHANGES, JOURNAL_PAGE_MAX_BYTES)
    {
        Ok(page) => {
            state.journal_page_reads = state.journal_page_reads.saturating_add(1);
            if let Some(reason) = page.resync_required {
                handle_resync_reason(state, reason);
                return;
            }
            let received = !page.changes.is_empty();
            state.pending_changes.extend(page.changes);
            if received || page.next != page.source_watermark {
                state.scheduler.try_wake();
            }
            if runtime.take_journal_advanced_wake() {
                state.scheduler.try_wake();
            }
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.scheduler.try_wake();
        }
        Err(_) => start_baseline_recovery(state),
    }
}

fn run_projection_apply_slice(state: &mut MaintenanceState) {
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
    if applied > 0 {
        state.projection_dirty = true;
    }
    if applied > 0 || !state.pending_changes.is_empty() {
        state.scheduler.try_wake();
    }
}

fn run_baseline_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let Some(recovery) = state.baseline.as_ref() else {
        return;
    };
    let snapshot_ref = recovery.snapshot.clone();
    let after_ref = recovery.after.clone();
    match runtime.lifecycle_baseline_page(
        snapshot_ref.as_ref(),
        after_ref.as_ref(),
        BASELINE_PAGE_BUDGET,
    ) {
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
                state.projection_dirty = true;
                begin_family_snapshots(state, snapshot.sequence);
            } else {
                state.scheduler.try_wake();
            }
        }
        Err(SessionLifecyclePageError::BudgetTooSmall { .. }) => {
            state.scheduler.try_wake();
        }
        Err(_) => start_baseline_recovery(state),
    }
}

fn run_host_bridge_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    refresh_session_family_consumers(runtime, state);
    continue_gap_pass(state);
    continue_snapshot_starts(state);
    continue_family_fanout(state);
    let Some((plugin_key, handler, payload)) = next_session_family_admission(state) else {
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
            timeout_ms: 1_000,
            context: PluginInvocationContext {
                client_id: None,
                session_id: None,
                subscription_id: None,
                surface_id: None,
                origin: Some("session-family".to_string()),
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

fn run_completion_drain_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let drain =
        runtime.drain_plugin_completions(COMPLETION_DRAIN_MAX_ITEMS, COMPLETION_DRAIN_MAX_BYTES);
    for completion in drain.completions {
        let request_id = match &completion.result {
            PluginInvocationResult::Completed(success) => success.request_id.clone(),
            PluginInvocationResult::Failed(failure) => failure.request_id.clone(),
        };
        let Some(plugin_key) = state
            .session_family
            .in_flight_by_request
            .remove(&request_id.0)
        else {
            continue;
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
            continue;
        }
        state.scheduler.try_wake();
    }
}

/// Start a paged baseline recovery. Incomplete pages are not ended evidence.
pub fn start_baseline_recovery(state: &mut MaintenanceState) {
    state.projection.begin_baseline_recovery();
    state.pending_changes.clear();
    state.observe_resume = None;
    state.session_family.need_gap_pass = true;
    state.session_family.gap_after = None;
    state.session_family.pending_fanout.clear();
    state.session_family.snapshot_start_sequence = None;
    state.session_family.snapshot_start_after = None;
    state.baseline = None;
    state.scheduler.try_wake();
}

fn continue_gap_pass(state: &mut MaintenanceState) {
    if !state.session_family.need_gap_pass {
        return;
    }
    let started = Instant::now();
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.gap_after.as_deref(),
        CONSUMER_REFRESH_MAX,
    );
    let mut last_key = state.session_family.gap_after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if started.elapsed() >= CONSUMER_REFRESH_MAX_ELAPSED {
            break;
        }
        state.session_family.mark_gap(plugin_key);
        last_key = Some(plugin_key.clone());
        visited += 1;
    }
    if visited < keys.len() || keys.len() == CONSUMER_REFRESH_MAX {
        state.session_family.gap_after = last_key;
        state.scheduler.try_wake();
        return;
    }
    state.session_family.need_gap_pass = false;
    state.session_family.gap_after = None;
    state.baseline = Some(BaselineRecovery {
        snapshot: None,
        after: None,
    });
    state.scheduler.try_wake();
}

fn continue_snapshot_starts(state: &mut MaintenanceState) {
    let Some(sequence) = state.session_family.snapshot_start_sequence else {
        return;
    };
    let started = Instant::now();
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.snapshot_start_after.as_deref(),
        CONSUMER_REFRESH_MAX,
    );
    let mut last_key = state.session_family.snapshot_start_after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if started.elapsed() >= CONSUMER_REFRESH_MAX_ELAPSED {
            break;
        }
        state.session_family.begin_snapshot(plugin_key, sequence);
        last_key = Some(plugin_key.clone());
        visited += 1;
    }
    if visited < keys.len() || keys.len() == CONSUMER_REFRESH_MAX {
        state.session_family.snapshot_start_after = last_key;
        state.scheduler.try_wake();
        return;
    }
    state.session_family.snapshot_start_sequence = None;
    state.session_family.snapshot_start_after = None;
}

fn continue_family_fanout(state: &mut MaintenanceState) {
    let Some(mut job) = state.session_family.pending_fanout.pop_front() else {
        return;
    };
    fanout_family_frame(state, &mut job);
    if job.after.is_some() {
        state.session_family.pending_fanout.push_front(job);
        state.scheduler.try_wake();
    }
}

fn handle_resync_reason(state: &mut MaintenanceState, reason: SessionLifecycleResyncReason) {
    match reason {
        SessionLifecycleResyncReason::SnapshotUnavailable
        | SessionLifecycleResyncReason::ObservePassUnavailable
        | SessionLifecycleResyncReason::SourceChanged
        | SessionLifecycleResyncReason::CursorExpired { .. }
        | SessionLifecycleResyncReason::CursorAhead => {
            start_baseline_recovery(state);
        }
        _ => start_baseline_recovery(state),
    }
}

fn refresh_session_family_consumers(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let started = Instant::now();
    let after = state.session_family.refresh_after.clone();
    let (handlers, more) =
        runtime.session_family_event_handlers_page(after.as_deref(), CONSUMER_REFRESH_MAX);
    let mut last_key = after;
    for handler in handlers {
        if started.elapsed() >= CONSUMER_REFRESH_MAX_ELAPSED {
            state.session_family.refresh_after = last_key;
            state.scheduler.try_wake();
            return;
        }
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
        last_key = Some(plugin_key);
    }
    if more {
        state.session_family.refresh_after = last_key;
        state.scheduler.try_wake();
        return;
    }
    let seen = std::mem::take(&mut state.session_family.refresh_seen);
    let mut dropped_busy = 0u32;
    let mut dropped_requests = Vec::new();
    state.session_family.consumers.retain(|key, consumer| {
        if seen.contains(key) {
            return true;
        }
        if consumer_busy(consumer) {
            dropped_busy = dropped_busy.saturating_add(1);
        }
        if let Some(flight) = &consumer.in_flight {
            dropped_requests.push(flight.request_id.0.clone());
        }
        false
    });
    for request_id in dropped_requests {
        state
            .session_family
            .in_flight_by_request
            .remove(&request_id);
    }
    state.session_family.busy_count = state.session_family.busy_count.saturating_sub(dropped_busy);
    state.session_family.refresh_after = None;
}

fn next_session_family_admission(
    state: &mut MaintenanceState,
) -> Option<(String, PluginHandlerRef, serde_json::Value)> {
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        state.session_family.admit_after.as_deref(),
        CONSUMER_REFRESH_MAX,
    );
    if keys.is_empty() {
        state.session_family.admit_after = None;
        return None;
    }
    for plugin_key in &keys {
        if let Some(payload) = next_session_family_payload(state, plugin_key) {
            let handler = state
                .session_family
                .consumers
                .get(plugin_key)
                .and_then(|consumer| consumer.handler.clone())?;
            state.session_family.admit_after = Some(plugin_key.clone());
            return Some((plugin_key.clone(), handler, payload));
        }
    }
    if keys.len() == CONSUMER_REFRESH_MAX {
        state.session_family.admit_after = keys.last().cloned();
        state.scheduler.try_wake();
    } else {
        state.session_family.admit_after = None;
    }
    None
}

fn next_session_family_payload(
    state: &mut MaintenanceState,
    plugin_key: &str,
) -> Option<serde_json::Value> {
    let mut queued = None;
    let mut blocked = false;
    let mut need_chunks = false;
    let mut sequence = 0;
    let mut after = None;
    state
        .session_family
        .touch_existing_consumer(plugin_key, |consumer| {
            if consumer.in_flight.is_some() || consumer.handler.is_none() {
                blocked = true;
                return;
            }
            if let Some(payload) = consumer.pending.pop_front() {
                queued = Some(payload);
                return;
            }
            need_chunks = consumer.need_snapshot_chunks;
            sequence = consumer.snapshot_sequence;
            after = consumer.snapshot_after.clone();
        });
    if blocked {
        return None;
    }
    if queued.is_some() {
        return queued;
    }
    if !need_chunks {
        return None;
    }
    match next_projection_chunk(&state.projection, after.as_deref()) {
        Ok(Some((chunk, last_id))) => {
            state
                .session_family
                .touch_existing_consumer(plugin_key, |consumer| {
                    consumer.snapshot_after = Some(last_id);
                });
            Some(session_chunk_frame(sequence, &chunk))
        }
        Ok(None) => {
            state
                .session_family
                .touch_existing_consumer(plugin_key, |consumer| {
                    consumer.need_snapshot_chunks = false;
                });
            Some(session_end_frame(sequence, true))
        }
        Err(()) => {
            state.session_family.mark_gap(plugin_key);
            None
        }
    }
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
    state
        .session_family
        .pending_fanout
        .push_back(FanoutJob { frame, after: None });
    state.scheduler.try_wake();
}

fn fanout_family_frame(state: &mut MaintenanceState, job: &mut FanoutJob) {
    let started = Instant::now();
    let keys = consumer_keys_page(
        &state.session_family.consumers,
        job.after.as_deref(),
        CONSUMER_REFRESH_MAX,
    );
    let mut last_key = job.after.clone();
    let mut visited = 0;
    for plugin_key in &keys {
        if started.elapsed() >= CONSUMER_REFRESH_MAX_ELAPSED {
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
    if visited < keys.len() || keys.len() == CONSUMER_REFRESH_MAX {
        job.after = last_key;
    } else {
        job.after = None;
    }
}

fn begin_family_snapshots(state: &mut MaintenanceState, sequence: u64) {
    state.session_family.snapshot_start_sequence = Some(sequence);
    state.session_family.snapshot_start_after = None;
    state.scheduler.try_wake();
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
    use std::path::Path;

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
        let first = next_session_family_admission(&mut state).expect("begin");
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
            next_session_family_admission(&mut state).is_none(),
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
    fn scheduler_round_robins_and_coalesces_wake() {
        let mut scheduler = MaintenanceScheduler::default();
        scheduler.try_wake();
        scheduler.try_wake();
        assert!(scheduler.has_wake());
        assert_eq!(scheduler.take_slice(), MaintenanceSliceKind::Observe);
        assert!(!scheduler.has_wake());
        assert_eq!(scheduler.take_slice(), MaintenanceSliceKind::JournalPull);
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
            continue_family_fanout(&mut state);
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
            continue_family_fanout(state);
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
            continue_gap_pass(&mut state);
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
            continue_snapshot_starts(&mut state);
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
        continue_family_fanout(&mut state);
        start_baseline_recovery(&mut state);
        continue_gap_pass(&mut state);
        assert!(started.elapsed() < Duration::from_millis(MAX_OWNER_TURN_MS));
        assert!(state.session_family.has_work());
        assert!(state.session_family.need_gap_pass);
        assert_eq!(state.session_family.pending_fanout.len(), 0);
    }
}
