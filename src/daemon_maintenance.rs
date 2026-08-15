//! Bounded owner-loop maintenance slices for Hub session projection.
//!
//! One owner turn runs at most one slice, then yields. This module does not
//! import terminal semantic bodies and does not name package-owned product
//! policy.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::SessionId;
use botster_core::{
    BoundaryJson, PluginAdmissionResult, PluginHandlerKind, PluginHandlerRef,
    PluginInvocationClass, PluginInvocationContext, PluginInvocationRequest,
    PluginInvocationResult, RequestId,
};
use botster_core_daemon::{
    LifecycleBaselineBudget, ObserveLifecycleBudget, ObserveLifecycleCursor,
    SessionLifecycleChange, SessionLifecycleCursor, SessionLifecyclePageError,
    SessionLifecycleRecord, SessionLifecycleResyncReason,
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
    assembled: Vec<SessionLifecycleRecord>,
}

/// One in-flight session-family frame waiting for a matching completion.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InFlightSessionFamily {
    request_id: RequestId,
    snapshot_sequence: u64,
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

/// Hub-owned `/session` delivery with one in-flight frame per plugin.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionFamilyBridge {
    consumers: BTreeMap<String, SessionFamilyConsumer>,
    next_request_serial: u64,
}

impl SessionFamilyBridge {
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
        let consumer = self.consumer_mut(plugin_key);
        consumer.gap = true;
        consumer.snapshot_complete = false;
        consumer.need_snapshot_chunks = false;
        consumer.snapshot_after = None;
        consumer.pending.clear();
        consumer.held_deltas.clear();
        consumer.in_flight = None;
    }

    fn begin_snapshot(&mut self, plugin_key: &str, sequence: u64) {
        let consumer = self.consumer_mut(plugin_key);
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

    fn queue_delta(&mut self, plugin_key: &str, frame: serde_json::Value) {
        let consumer = self.consumer_mut(plugin_key);
        if consumer.gap {
            return;
        }
        if !consumer.snapshot_complete {
            consumer.held_deltas.push_back(frame);
            return;
        }
        consumer.pending.push_back(frame);
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
            || self.session_family.consumers.values().any(|consumer| {
                consumer.in_flight.is_some()
                    || !consumer.pending.is_empty()
                    || !consumer.held_deltas.is_empty()
                    || consumer.need_snapshot_chunks
            })
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
    if applied > 0 || !state.pending_changes.is_empty() {
        state.scheduler.try_wake();
    }
}

fn run_baseline_slice(runtime: &HubRuntime, state: &mut MaintenanceState) {
    let Some(recovery) = state.baseline.as_mut() else {
        return;
    };
    match runtime.lifecycle_baseline_page(
        recovery.snapshot.as_ref(),
        recovery.after.as_ref(),
        BASELINE_PAGE_BUDGET,
    ) {
        Ok(page) => {
            state.baseline_page_reads = state.baseline_page_reads.saturating_add(1);
            if let Some(reason) = page.resync_required {
                handle_resync_reason(state, reason);
                return;
            }
            recovery.snapshot = Some(page.snapshot_sequence.clone());
            recovery.after = page.next.clone();
            recovery.assembled.extend(page.sessions);
            if page.complete {
                let snapshot = page.snapshot_sequence;
                let assembled = state
                    .baseline
                    .take()
                    .map(|recovery| recovery.assembled)
                    .unwrap_or_default();
                let sequence = snapshot.sequence;
                state
                    .projection
                    .replace_complete_baseline(snapshot, assembled);
                begin_family_snapshots(state, sequence);
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
    let Some((plugin_key, handler, payload)) = next_session_family_admission(state) else {
        return;
    };
    let sequence = payload
        .get("snapshot_sequence")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
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
            if let Some(consumer) = state.session_family.consumers.get_mut(&plugin_key) {
                consumer.in_flight = Some(InFlightSessionFamily {
                    request_id,
                    snapshot_sequence: consumer.snapshot_sequence,
                });
            }
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
            .consumers
            .iter()
            .find_map(|(key, consumer)| {
                consumer
                    .in_flight
                    .as_ref()
                    .is_some_and(|flight| flight.request_id == request_id)
                    .then(|| key.clone())
            })
        else {
            continue;
        };
        let success = matches!(completion.result, PluginInvocationResult::Completed(_));
        if let Some(consumer) = state.session_family.consumers.get_mut(&plugin_key) {
            let ended_snapshot = consumer
                .in_flight
                .as_ref()
                .is_some_and(|_| consumer.pending.front().is_none());
            consumer.in_flight = None;
            if !success {
                state.session_family.mark_gap(&plugin_key);
                start_baseline_recovery(state);
                continue;
            }
            if ended_snapshot {
                consumer.snapshot_complete = true;
                consumer.pending.extend(consumer.held_deltas.drain(..));
            }
        }
        state.scheduler.try_wake();
    }
}

/// Start a paged baseline recovery. Incomplete pages are not ended evidence.
pub fn start_baseline_recovery(state: &mut MaintenanceState) {
    state.projection.begin_baseline_recovery();
    state.pending_changes.clear();
    state.observe_resume = None;
    let keys = state
        .session_family
        .consumers
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plugin_key in keys {
        state.session_family.mark_gap(&plugin_key);
    }
    state.baseline = Some(BaselineRecovery {
        snapshot: None,
        after: None,
        assembled: Vec::new(),
    });
    state.scheduler.try_wake();
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
    let handlers = runtime.session_family_event_handlers();
    let mut live = BTreeMap::new();
    for handler in handlers {
        if handler.handler.kind != PluginHandlerKind::Event {
            continue;
        }
        let plugin_key = handler.handler.plugin_key.0.clone();
        live.insert(plugin_key.clone(), handler.handler.clone());
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
    state
        .session_family
        .consumers
        .retain(|key, _| live.contains_key(key));
}

fn next_session_family_admission(
    state: &mut MaintenanceState,
) -> Option<(String, PluginHandlerRef, serde_json::Value)> {
    let keys = state
        .session_family
        .consumers
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plugin_key in keys {
        if let Some(payload) = next_session_family_payload(state, &plugin_key) {
            let handler = state
                .session_family
                .consumers
                .get(&plugin_key)
                .and_then(|consumer| consumer.handler.clone())?;
            return Some((plugin_key, handler, payload));
        }
    }
    None
}

fn next_session_family_payload(
    state: &mut MaintenanceState,
    plugin_key: &str,
) -> Option<serde_json::Value> {
    let consumer = state.session_family.consumers.get_mut(plugin_key)?;
    if consumer.in_flight.is_some() || consumer.handler.is_none() {
        return None;
    }
    if let Some(payload) = consumer.pending.pop_front() {
        return Some(payload);
    }
    if !consumer.need_snapshot_chunks {
        return None;
    }
    let sequence = consumer.snapshot_sequence;
    let after = consumer.snapshot_after.clone();
    match next_projection_chunk(&state.projection, after.as_deref()) {
        Ok(Some((chunk, last_id))) => {
            let consumer = state.session_family.consumers.get_mut(plugin_key)?;
            consumer.snapshot_after = Some(last_id);
            Some(session_chunk_frame(sequence, &chunk))
        }
        Ok(None) => {
            let consumer = state.session_family.consumers.get_mut(plugin_key)?;
            consumer.need_snapshot_chunks = false;
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
    for (id, row) in &projection.rows {
        if after.is_some_and(|after| id.as_str() <= after) {
            continue;
        }
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
    let keys = state
        .session_family
        .consumers
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plugin_key in keys {
        state.session_family.queue_delta(&plugin_key, frame.clone());
    }
}

fn begin_family_snapshots(state: &mut MaintenanceState, sequence: u64) {
    let keys = state
        .session_family
        .consumers
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for plugin_key in keys {
        state.session_family.begin_snapshot(&plugin_key, sequence);
    }
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
}
