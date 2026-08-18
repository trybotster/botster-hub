//! Entity subscription registration, fanout, overflow, and resync.
//!
//! This module owns one subscription lifecycle: register, snapshot, patch,
//! overflow, resync, and fanout. The daemon transport owns the accept loop,
//! connection cleanup, and control dispatch.

use std::collections::BTreeMap;
use std::ops::Bound;
#[cfg(test)]
use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, Instant};

#[cfg(test)]
use botster_core::SessionLifecycleState;
use botster_core_daemon::SessionLifecycleCursor;
#[cfg(test)]
use botster_core_daemon::{RegistrySessionState, SessionLifecycleBaseline, SessionLifecycleRecord};
use botster_hub_client::{
    DaemonDiagnostic, DaemonEntityFrame, DaemonLifecycleCounters, DaemonOperatorError,
    DaemonResponse, DaemonResponseKind, DaemonSessionEntity,
};
use serde_json::Value;

use super::{
    DAEMON_MAX_FRAME_BYTES, DaemonControlState, DaemonTransportError, DaemonTransportResult,
    HubDaemon, daemon_response_base, daemon_session_type_from_client, session_type_entity_snapshot,
};

const SESSION_DELIVERY_MAX_ITEMS: usize = 16;
const SESSION_DELIVERY_MAX_BYTES: usize = 64 * 1024;
const SESSION_DELIVERY_MAX_ELAPSED: Duration = Duration::from_millis(8);

struct DeliveryPage {
    items: usize,
    bytes: usize,
    more: bool,
}

#[derive(Debug)]
pub(crate) enum EntityFrameSender {
    #[cfg(test)]
    Blocking(SyncSender<DaemonEntityFrame>),
    Async(tokio::sync::mpsc::Sender<DaemonEntityFrame>),
}

#[derive(Debug)]
enum EntityFrameTrySendError {
    Full(DaemonEntityFrame),
    Disconnected,
}

impl EntityFrameSender {
    fn try_send_kind(&self, frame: DaemonEntityFrame) -> Result<(), EntityFrameTrySendError> {
        match self {
            #[cfg(test)]
            Self::Blocking(sender) => sender.try_send(frame).map_err(|error| match error {
                mpsc::TrySendError::Full(frame) => EntityFrameTrySendError::Full(frame),
                mpsc::TrySendError::Disconnected(_) => EntityFrameTrySendError::Disconnected,
            }),
            Self::Async(sender) => sender.try_send(frame).map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(frame) => {
                    EntityFrameTrySendError::Full(frame)
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    EntityFrameTrySendError::Disconnected
                }
            }),
        }
    }

    pub(crate) fn try_send(&self, frame: DaemonEntityFrame) -> Result<(), ()> {
        self.try_send_kind(frame).map_err(|_| ())
    }
}

fn entity_frame_exceeds_limit(frame: &DaemonEntityFrame) -> bool {
    serde_json::to_vec(frame)
        .expect("daemon entity frame values always serialize")
        .len()
        > DAEMON_MAX_FRAME_BYTES
}

#[derive(Debug)]
pub(crate) struct EntitySubscriptionState {
    sender: EntityFrameSender,
    entity_type: String,
    #[allow(dead_code)]
    cursor: Option<SessionLifecycleCursor>,
    entities: BTreeMap<String, DaemonSessionEntity>,
    definition_generation: u64,
    definition_entities: BTreeMap<String, Value>,
    resync_reason: Option<String>,
    /// Local WebRTC grant that owns this subscription, when registered over DataChannel.
    /// Used so PeerClosed can sweep rows that arrived after cleanup_once's id snapshot.
    pub(crate) owner_grant_id: Option<String>,
    /// Highest package-entity snapshot/delta seq successfully applied to this stream.
    /// Built-in `session` / `session_type` families leave this `None`.
    package_last_applied_seq: Option<u64>,
    /// Package-entity subscriber is gated to targeted snapshots until caught up.
    package_catching_up: bool,
    /// Resume key for one bounded session-delivery page.
    delivery_after: Option<String>,
    /// Removes first, then projection rows. Prevents a high remove id from
    /// skipping a later lower upsert id.
    delivery_phase: DeliveryPhase,
    /// Per-subscriber monotonic snapshot sequence.
    next_seq: u64,
    /// JSON values accumulated while assembling a complete first snapshot.
    assembled_items: Vec<Value>,
    /// Encoded item bytes plus JSON array separators for `assembled_items`.
    assembled_item_bytes: usize,
    /// True until a delivery page reports no remaining work.
    needs_delivery: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryPhase {
    Assembling { source_seq: u64 },
    Removes,
    Rows,
}

pub(super) fn register_entity_subscription(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
    entity_type: String,
    subscription_id: String,
    sender: EntityFrameSender,
    owner_grant_id: Option<String>,
) -> DaemonTransportResult<DaemonResponse> {
    if state.entity_subscriptions.contains_key(&subscription_id) {
        return Ok(entity_subscription_error(
            "duplicate_entity_subscription",
            &subscription_id,
            "entity subscription id is already active",
        ));
    }
    if entity_type == "session_type" {
        let (snapshot_seq, entities) = match session_type_entity_snapshot(daemon) {
            Ok(snapshot) => snapshot,
            Err(DaemonTransportError::Client(crate::HubClientError::SessionType {
                kind,
                message,
                ..
            })) => {
                // Keep entity-subscription operator frames on the subscribe_entities
                // convention (request_id = subscription_id), not list_session_types.
                return Ok(entity_subscription_error(kind, &subscription_id, &message));
            }
            Err(error) => return Err(error),
        };
        let snapshot = DaemonEntityFrame::Snapshot {
            subscription_id: subscription_id.clone(),
            entity_type: entity_type.clone(),
            snapshot_seq,
            items: entities.values().cloned().collect(),
            resync_reason: None,
        };
        if entity_frame_exceeds_limit(&snapshot) {
            return Ok(entity_subscription_error(
                "entity_provider_frame_too_large",
                &subscription_id,
                "session type snapshot exceeds daemon frame limit",
            ));
        }
        sender
            .try_send(snapshot)
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: snapshot_seq,
                definition_entities: entities,
                resync_reason: None,
                owner_grant_id,
                package_last_applied_seq: None,
                package_catching_up: false,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Removes,
                next_seq: snapshot_seq,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            },
        );
        state.lifecycle_counters.live_entity_subscriptions =
            state.entity_subscriptions.len() as u64;
        state.lifecycle_counters.high_water_entity_subscriptions = state
            .lifecycle_counters
            .high_water_entity_subscriptions
            .max(state.lifecycle_counters.live_entity_subscriptions);
        return Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed));
    }
    if entity_type != "session" {
        let (snapshot_seq, items, catching_up) = {
            let runtime = daemon
                .runtime_mut()
                .ok_or(DaemonTransportError::DaemonNotRunning)?;
            let (snapshot_seq, items) =
                match runtime.plugin_entity_snapshot(&entity_type, &subscription_id) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return Ok(entity_subscription_error(
                            &error.code,
                            &subscription_id,
                            &error.message,
                        ));
                    }
                };
            // Advance monotonic family floor from provider; never lower it.
            let _ = runtime.apply_package_entity_provider_snapshot(&entity_type, snapshot_seq);
            let family_floor = runtime
                .package_entity_family_state(&entity_type)
                .map(|family| family.last_accepted_seq)
                .unwrap_or(snapshot_seq);
            // New subscriber may receive this snapshot (never-applied). Behind
            // relative to family floor means catching_up; advanced peers are not
            // touched here because we only deliver to this subscription.
            let catching_up = snapshot_seq < family_floor;
            if catching_up {
                // New catching-up subscriber re-arms even after degraded.
                runtime.rearm_package_entity_resync(&entity_type);
            }
            (snapshot_seq, items, catching_up)
        };
        let snapshot = DaemonEntityFrame::Snapshot {
            subscription_id: subscription_id.clone(),
            entity_type: entity_type.clone(),
            snapshot_seq,
            items,
            resync_reason: None,
        };
        if entity_frame_exceeds_limit(&snapshot) {
            return Ok(entity_subscription_error(
                "entity_provider_frame_too_large",
                &subscription_id,
                "entity provider snapshot exceeds daemon frame limit",
            ));
        }
        sender
            .try_send(snapshot)
            .map_err(|_| DaemonTransportError::ControlThreadStopped)?;
        state.entity_subscriptions.insert(
            subscription_id.clone(),
            EntitySubscriptionState {
                sender,
                entity_type,
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 0,
                definition_entities: BTreeMap::new(),
                resync_reason: None,
                owner_grant_id,
                package_last_applied_seq: Some(snapshot_seq),
                package_catching_up: catching_up,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Removes,
                next_seq: snapshot_seq,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            },
        );
        state.lifecycle_counters.live_entity_subscriptions =
            state.entity_subscriptions.len() as u64;
        state.lifecycle_counters.high_water_entity_subscriptions = state
            .lifecycle_counters
            .high_water_entity_subscriptions
            .max(state.lifecycle_counters.live_entity_subscriptions);
        // Fanout any pending mutations unlocked by the floor advance.
        drive_package_entity_fanout(daemon, state);
        return Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed));
    }
    if let Some(runtime) = daemon.runtime() {
        crate::daemon_maintenance::run_maintenance_kind(
            runtime,
            &mut state.maintenance,
            crate::daemon_maintenance::MaintenanceSliceKind::JournalPull,
        );
        crate::daemon_maintenance::run_maintenance_kind(
            runtime,
            &mut state.maintenance,
            crate::daemon_maintenance::MaintenanceSliceKind::ProjectionApply,
        );
    }
    let cursor = state.maintenance.projection.cursor.clone();
    let snapshot_seq = cursor.as_ref().map(|cursor| cursor.sequence).unwrap_or(0);
    let subscription = EntitySubscriptionState {
        sender,
        entity_type: "session".to_string(),
        cursor,
        entities: BTreeMap::new(),
        definition_generation: 0,
        definition_entities: BTreeMap::new(),
        resync_reason: None,
        owner_grant_id,
        package_last_applied_seq: None,
        package_catching_up: false,
        delivery_after: None,
        delivery_phase: DeliveryPhase::Assembling {
            source_seq: snapshot_seq,
        },
        next_seq: snapshot_seq,
        assembled_items: Vec::new(),
        assembled_item_bytes: 0,
        needs_delivery: true,
    };
    state.maintenance.journal_caught_up_confirmed = false;
    state
        .entity_subscriptions
        .insert(subscription_id.clone(), subscription);
    state.lifecycle_counters.live_entity_subscriptions = state
        .lifecycle_counters
        .live_entity_subscriptions
        .saturating_add(1);
    state.lifecycle_counters.high_water_entity_subscriptions = state
        .lifecycle_counters
        .high_water_entity_subscriptions
        .max(state.lifecycle_counters.live_entity_subscriptions);
    if state.released_entity_generations > 0 {
        state.released_entity_generations -= 1;
        state.lifecycle_counters.reconnect_registrations = state
            .lifecycle_counters
            .reconnect_registrations
            .saturating_add(1);
    }
    state.maintenance.scheduler.prefer_journal_pull();
    Ok(daemon_response_base(DaemonResponseKind::EntitySubscribed))
}

pub(super) fn seed_lifecycle_reconciliation(
    daemon: &mut HubDaemon,
    state: &mut DaemonControlState,
) {
    if daemon.runtime().is_none() {
        return;
    };
    crate::daemon_maintenance::start_baseline_recovery(&mut state.maintenance);
}

fn drive_session_type_subscriptions(
    subscriptions: &mut BTreeMap<String, EntitySubscriptionState>,
    generation: u64,
    entities: &BTreeMap<String, Value>,
) {
    subscriptions.retain(|subscription_id, subscription| {
        if subscription.entity_type != "session_type" {
            return true;
        }

        if let Some(reason) = subscription.resync_reason.clone() {
            let snapshot_seq = subscription.next_seq.saturating_add(1);
            let frame = DaemonEntityFrame::Snapshot {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq,
                items: entities.values().cloned().collect(),
                resync_reason: Some(reason),
            };
            if entity_frame_exceeds_limit(&frame) {
                let error = DaemonEntityFrame::Error {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    code: "entity_provider_frame_too_large".to_string(),
                    message: "session type snapshot exceeds daemon frame limit".to_string(),
                };
                return match subscription.sender.try_send_kind(error) {
                    Ok(()) | Err(EntityFrameTrySendError::Disconnected) => false,
                    Err(EntityFrameTrySendError::Full(_)) => true,
                };
            }
            return match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    subscription.next_seq = snapshot_seq;
                    subscription.definition_generation = generation;
                    subscription.definition_entities = entities.clone();
                    subscription.resync_reason = None;
                    true
                }
                Err(EntityFrameTrySendError::Full(_)) => true,
                Err(EntityFrameTrySendError::Disconnected) => false,
            };
        }

        if subscription.definition_generation == generation {
            return true;
        }

        let mut frames = subscription
            .definition_entities
            .keys()
            .filter(|id| !entities.contains_key(*id))
            .map(|id| DaemonEntityFrame::Remove {
                subscription_id: subscription_id.clone(),
                entity_type: "session_type".to_string(),
                snapshot_seq: 0,
                id: id.clone(),
            })
            .collect::<Vec<_>>();
        frames.extend(
            entities
                .iter()
                .filter(|(id, entity)| subscription.definition_entities.get(*id) != Some(*entity))
                .map(|(id, entity)| DaemonEntityFrame::Upsert {
                    subscription_id: subscription_id.clone(),
                    entity_type: "session_type".to_string(),
                    snapshot_seq: 0,
                    id: id.clone(),
                    entity: entity.clone(),
                }),
        );
        for frame in frames {
            let snapshot_seq = subscription.next_seq.saturating_add(1);
            let frame = with_session_type_snapshot_seq(frame, snapshot_seq);
            match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    subscription.next_seq = snapshot_seq;
                }
                Err(EntityFrameTrySendError::Full(_)) => {
                    subscription.resync_reason = Some("subscriber_overflow".to_string());
                    return true;
                }
                Err(EntityFrameTrySendError::Disconnected) => return false,
            }
        }
        subscription.definition_generation = generation;
        subscription.definition_entities = entities.clone();
        true
    });
}

fn with_session_type_snapshot_seq(
    frame: DaemonEntityFrame,
    snapshot_seq: u64,
) -> DaemonEntityFrame {
    match frame {
        DaemonEntityFrame::Remove {
            subscription_id,
            entity_type,
            id,
            ..
        } => DaemonEntityFrame::Remove {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
        },
        DaemonEntityFrame::Upsert {
            subscription_id,
            entity_type,
            id,
            entity,
            ..
        } => DaemonEntityFrame::Upsert {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
            entity,
        },
        DaemonEntityFrame::Patch {
            subscription_id,
            entity_type,
            id,
            patch,
            ..
        } => DaemonEntityFrame::Patch {
            subscription_id,
            entity_type,
            snapshot_seq,
            id,
            patch,
        },
        DaemonEntityFrame::Snapshot {
            subscription_id,
            entity_type,
            items,
            resync_reason,
            ..
        } => DaemonEntityFrame::Snapshot {
            subscription_id,
            entity_type,
            snapshot_seq,
            items,
            resync_reason,
        },
        other => other,
    }
}

pub(super) fn drive_entity_subscriptions(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    if state.entity_subscriptions.is_empty() {
        return;
    }
    let packages = daemon.package_registry().clone();
    let Some(runtime) = daemon.runtime_mut() else {
        state.entity_subscriptions.clear();
        state.lifecycle_counters.live_entity_subscriptions = 0;
        return;
    };
    state.entity_subscriptions.retain(|_, subscription| {
        subscription.entity_type == "session"
            || subscription.entity_type == "session_type"
            || runtime.has_plugin_entity_provider_family(&subscription.entity_type)
    });
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;

    state.lifecycle_counters.reconciliation_wakes = state
        .lifecycle_counters
        .reconciliation_wakes
        .saturating_add(1);

    if state
        .entity_subscriptions
        .values()
        .any(|subscription| subscription.entity_type == "session_type")
    {
        let records = packages.packages();
        let runtime_state = runtime.state();
        let generation = runtime_state.session_type_generation;
        if let Ok(session_types) =
            crate::session_types::list_session_types(&records, &runtime_state)
        {
            let entities = session_types
                .into_iter()
                .map(daemon_session_type_from_client)
                .filter_map(|session_type| {
                    let id = session_type.session_type_id.clone();
                    serde_json::to_value(session_type)
                        .ok()
                        .map(|value| (id, value))
                })
                .collect::<BTreeMap<_, _>>();
            drive_session_type_subscriptions(
                &mut state.entity_subscriptions,
                generation,
                &entities,
            );
        }
    }

    let started = Instant::now();
    let mut delivered = 0usize;
    let mut delivered_bytes = 0usize;
    let caught_up = crate::daemon_maintenance::refresh_projection_if_inventory_ahead(
        runtime,
        &mut state.maintenance,
    );
    let complete =
        state.maintenance.projection.baseline_complete && !state.maintenance.projection.gap;
    if complete {
        let resync_ids = state
            .entity_subscriptions
            .iter()
            .filter(|(_, subscription)| {
                subscription.entity_type == "session"
                    && subscription.resync_reason.is_some()
                    && !matches!(
                        subscription.delivery_phase,
                        DeliveryPhase::Assembling { .. }
                    )
            })
            .map(|(id, _)| id.clone())
            .take(1)
            .collect::<Vec<_>>();
        if let Some(subscription_id) = resync_ids.first() {
            let reason = state
                .entity_subscriptions
                .get(subscription_id)
                .and_then(|subscription| subscription.resync_reason.clone())
                .unwrap_or_else(|| "projection_gap".to_string());
            let before = state.entity_subscriptions.len();
            state.entity_subscriptions.retain(|id, subscription| {
                if id != subscription_id {
                    return true;
                }
                try_resync_from_projection(
                    id,
                    subscription,
                    &state.maintenance.projection,
                    caught_up,
                    reason.clone(),
                    &mut state.lifecycle_counters,
                )
            });
            note_released_entity_generations(state, before);
            state.maintenance.try_wake();
        } else {
            let mut more = false;
            let before = state.entity_subscriptions.len();
            state
                .entity_subscriptions
                .retain(|subscription_id, subscription| {
                    if subscription.entity_type != "session" {
                        return true;
                    }
                    if started.elapsed() >= SESSION_DELIVERY_MAX_ELAPSED
                        || delivered >= SESSION_DELIVERY_MAX_ITEMS
                        || delivered_bytes >= SESSION_DELIVERY_MAX_BYTES
                    {
                        more = true;
                        return true;
                    }
                    let (alive, page) = if matches!(
                        subscription.delivery_phase,
                        DeliveryPhase::Assembling { .. }
                    ) {
                        match continue_session_snapshot_assembly(
                            subscription_id,
                            subscription,
                            &state.maintenance.projection,
                            caught_up,
                            &mut state.lifecycle_counters,
                            SESSION_DELIVERY_MAX_ITEMS.saturating_sub(delivered),
                            SESSION_DELIVERY_MAX_BYTES.saturating_sub(delivered_bytes),
                            SESSION_DELIVERY_MAX_ELAPSED.saturating_sub(started.elapsed()),
                        ) {
                            SnapshotAssemble::Continue { page } => (true, page),
                            SnapshotAssemble::Closed {
                                frame_too_large: true,
                            }
                            | SnapshotAssemble::Closed {
                                frame_too_large: false,
                            } => (
                                false,
                                DeliveryPage {
                                    items: 0,
                                    bytes: 0,
                                    more: false,
                                },
                            ),
                        }
                    } else {
                        deliver_projection_delta_page(
                            subscription_id,
                            subscription,
                            &state.maintenance.projection,
                            &mut state.lifecycle_counters,
                            SESSION_DELIVERY_MAX_ITEMS.saturating_sub(delivered),
                            SESSION_DELIVERY_MAX_BYTES.saturating_sub(delivered_bytes),
                            SESSION_DELIVERY_MAX_ELAPSED.saturating_sub(started.elapsed()),
                        )
                    };
                    delivered = delivered.saturating_add(page.items);
                    delivered_bytes = delivered_bytes.saturating_add(page.bytes);
                    more |= page.more;
                    alive
                });
            note_released_entity_generations(state, before);
            if more {
                state.maintenance.try_wake();
            } else {
                state.maintenance.projection_dirty = false;
            }
        }
    }

    if complete {
        state
            .pending_runtime
            .retain_sessions_present_in(|session_id| {
                state.maintenance.projection.rows.contains_key(session_id)
            });
    }
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

fn note_released_entity_generations(state: &mut DaemonControlState, before: usize) {
    let released = before.saturating_sub(state.entity_subscriptions.len()) as u64;
    if released == 0 {
        return;
    }
    state.released_entity_generations = state.released_entity_generations.saturating_add(released);
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

pub(super) fn session_subscribers_need_delivery(state: &DaemonControlState) -> bool {
    let has_session_subscriber = state
        .entity_subscriptions
        .values()
        .any(|subscription| subscription.entity_type == "session");
    (state.maintenance.projection_dirty && has_session_subscriber)
        || state.entity_subscriptions.values().any(|subscription| {
            subscription.entity_type == "session"
                && (subscription.needs_delivery || subscription.resync_reason.is_some())
        })
}

pub(super) fn drive_package_entity_fanout(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let mutations = runtime.take_leased_package_entity_fanout();
    if mutations.is_empty() {
        return;
    }
    for item in mutations {
        if item.finish_only {
            runtime.finish_package_entity_mutation_fanout(&item, item.scheduled_resync);
            continue;
        }
        let mutation = &item.mutation;
        state.lifecycle_counters.package_entity_publish_accepted = state
            .lifecycle_counters
            .package_entity_publish_accepted
            .saturating_add(1);
        let entity_type = mutation.entity_type().to_string();
        let seq = mutation.snapshot_seq();
        let mut scheduled_resync = false;
        let mut dead = Vec::new();
        for (subscription_id, subscription) in state.entity_subscriptions.iter_mut() {
            if subscription.entity_type != entity_type {
                continue;
            }
            // Catching_up subscribers receive only targeted resync snapshots.
            // Do not treat applied < family_floor alone as catching_up here: the
            // sequential next delta always has applied == floor-1 before delivery.
            if subscription.package_catching_up {
                runtime.mark_package_entity_resync_needed(&entity_type);
                scheduled_resync = true;
                continue;
            }
            let Some(applied) = subscription.package_last_applied_seq else {
                subscription.package_catching_up = true;
                runtime.mark_package_entity_resync_needed(&entity_type);
                scheduled_resync = true;
                continue;
            };
            if seq < applied + 1 {
                // Already applied or behind relative to this subscriber.
                continue;
            }
            if seq > applied + 1 {
                // Gap relative to this subscriber — schedule targeted resync.
                subscription.package_catching_up = true;
                runtime.mark_package_entity_resync_needed(&entity_type);
                scheduled_resync = true;
                continue;
            }
            // seq == applied + 1
            let frame = package_mutation_to_daemon_frame(subscription_id, mutation);
            if entity_frame_exceeds_limit(&frame) {
                state.lifecycle_counters.entity_delivery_attempts = state
                    .lifecycle_counters
                    .entity_delivery_attempts
                    .saturating_add(1);
                let error = DaemonEntityFrame::Error {
                    subscription_id: subscription_id.clone(),
                    entity_type: entity_type.clone(),
                    code: "entity_provider_frame_too_large".to_string(),
                    message: "package entity mutation exceeds daemon frame limit".to_string(),
                };
                match subscription.sender.try_send_kind(error) {
                    Ok(()) => {
                        state.lifecycle_counters.entity_delivery_successes = state
                            .lifecycle_counters
                            .entity_delivery_successes
                            .saturating_add(1);
                        // Do not advance applied seq; schedule resync for recovery.
                        subscription.package_catching_up = true;
                        subscription.resync_reason =
                            Some("entity_provider_frame_too_large".to_string());
                        runtime.mark_package_entity_resync_needed(&entity_type);
                        scheduled_resync = true;
                    }
                    Err(EntityFrameTrySendError::Full(_)) => {
                        state.lifecycle_counters.entity_delivery_overflows = state
                            .lifecycle_counters
                            .entity_delivery_overflows
                            .saturating_add(1);
                        subscription.package_catching_up = true;
                        subscription.resync_reason = Some("subscriber_overflow".to_string());
                        runtime.mark_package_entity_resync_needed(&entity_type);
                        scheduled_resync = true;
                    }
                    Err(EntityFrameTrySendError::Disconnected) => {
                        state.lifecycle_counters.entity_delivery_failures = state
                            .lifecycle_counters
                            .entity_delivery_failures
                            .saturating_add(1);
                        dead.push(subscription_id.clone());
                    }
                }
                continue;
            }
            state.lifecycle_counters.entity_delivery_attempts = state
                .lifecycle_counters
                .entity_delivery_attempts
                .saturating_add(1);
            match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    state.lifecycle_counters.entity_delivery_successes = state
                        .lifecycle_counters
                        .entity_delivery_successes
                        .saturating_add(1);
                    subscription.package_last_applied_seq = Some(seq);
                    subscription.package_catching_up = false;
                }
                Err(EntityFrameTrySendError::Full(_)) => {
                    state.lifecycle_counters.entity_delivery_overflows = state
                        .lifecycle_counters
                        .entity_delivery_overflows
                        .saturating_add(1);
                    subscription.package_catching_up = true;
                    subscription.resync_reason = Some("subscriber_overflow".to_string());
                    runtime.mark_package_entity_resync_needed(&entity_type);
                    scheduled_resync = true;
                }
                Err(EntityFrameTrySendError::Disconnected) => {
                    state.lifecycle_counters.entity_delivery_failures = state
                        .lifecycle_counters
                        .entity_delivery_failures
                        .saturating_add(1);
                    dead.push(subscription_id.clone());
                }
            }
        }
        runtime.finish_package_entity_mutation_fanout(&item, scheduled_resync);
        for subscription_id in dead {
            state.entity_subscriptions.remove(&subscription_id);
        }
    }
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

fn package_mutation_to_daemon_frame(
    subscription_id: &str,
    mutation: &crate::package_entity_fanout::PackageEntityMutation,
) -> DaemonEntityFrame {
    match mutation {
        crate::package_entity_fanout::PackageEntityMutation::Upsert {
            entity_type,
            snapshot_seq,
            id,
            entity,
        } => DaemonEntityFrame::Upsert {
            subscription_id: subscription_id.to_string(),
            entity_type: entity_type.clone(),
            snapshot_seq: *snapshot_seq,
            id: id.clone(),
            entity: entity.clone(),
        },
        crate::package_entity_fanout::PackageEntityMutation::Patch {
            entity_type,
            snapshot_seq,
            id,
            patch,
        } => DaemonEntityFrame::Patch {
            subscription_id: subscription_id.to_string(),
            entity_type: entity_type.clone(),
            snapshot_seq: *snapshot_seq,
            id: id.clone(),
            patch: patch.clone(),
        },
        crate::package_entity_fanout::PackageEntityMutation::Remove {
            entity_type,
            snapshot_seq,
            id,
        } => DaemonEntityFrame::Remove {
            subscription_id: subscription_id.to_string(),
            entity_type: entity_type.clone(),
            snapshot_seq: *snapshot_seq,
            id: id.clone(),
        },
    }
}

pub(super) fn drive_package_entity_resync(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    // Stagnant catching_up / overflow may keep `needed` set when not degraded.
    // Do not rearm degraded families here — only a new publish or a newly
    // catching-up subscribe may clear degradation and start another cycle.
    if let Some(runtime) = daemon.runtime() {
        for subscription in state.entity_subscriptions.values() {
            if subscription.package_catching_up || subscription.resync_reason.is_some() {
                runtime.mark_package_entity_resync_needed(&subscription.entity_type);
            }
        }
    }
    let eligible = {
        let Some(runtime) = daemon.runtime() else {
            return;
        };
        runtime.package_entity_resync_eligible_families()
    };
    if eligible.is_empty() {
        return;
    }
    for entity_type in eligible {
        // Also resync when any subscriber is catching_up even without a gap.
        let has_catching_up = state.entity_subscriptions.values().any(|subscription| {
            subscription.entity_type == entity_type && subscription.package_catching_up
        });
        let family_needs = daemon
            .runtime()
            .and_then(|runtime| runtime.package_entity_family_state(&entity_type))
            .is_some_and(|family| {
                family.resync.needed
                    || family.last_accepted_seq < family.high_water_seq
                    || !family.pending_by_seq.is_empty()
            });
        if !has_catching_up && !family_needs {
            continue;
        }

        let degraded = {
            let Some(runtime) = daemon.runtime() else {
                return;
            };
            state.lifecycle_counters.package_entity_resync_attempts = state
                .lifecycle_counters
                .package_entity_resync_attempts
                .saturating_add(1);
            runtime.record_package_entity_resync_attempt(&entity_type)
        };
        if degraded {
            state.lifecycle_counters.package_entity_resync_degraded = state
                .lifecycle_counters
                .package_entity_resync_degraded
                .saturating_add(1);
            continue;
        }

        let subscription_id_for_provider = state
            .entity_subscriptions
            .iter()
            .find(|(_, subscription)| subscription.entity_type == entity_type)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| format!("package-entity-resync-{entity_type}"));

        let provider_result = {
            let Some(runtime) = daemon.runtime_mut() else {
                return;
            };
            runtime.plugin_entity_snapshot(&entity_type, &subscription_id_for_provider)
        };
        let Ok((snapshot_seq, items)) = provider_result else {
            // Failed provider: keep schedule (attempt already recorded with backoff).
            continue;
        };

        let ready = {
            let Some(runtime) = daemon.runtime() else {
                return;
            };
            runtime.apply_package_entity_provider_snapshot(&entity_type, snapshot_seq)
        };
        if !ready.is_empty() {
            // Queue drained pending for the shared fanout path.
            // apply_package_entity_provider_snapshot already enqueued them.
        }

        let family_floor = daemon
            .runtime()
            .and_then(|runtime| runtime.package_entity_family_state(&entity_type))
            .map(|family| family.last_accepted_seq)
            .unwrap_or(snapshot_seq);

        let mut dead = Vec::new();
        for (subscription_id, subscription) in state.entity_subscriptions.iter_mut() {
            if subscription.entity_type != entity_type {
                continue;
            }
            // Targeted delivery: only catching_up / overflow / never-applied, and never
            // roll an advanced subscriber backward.
            let applied = subscription.package_last_applied_seq;
            let needs_snapshot = subscription.package_catching_up
                || subscription.resync_reason.is_some()
                || applied.is_none()
                || applied.is_some_and(|seq| seq < family_floor);
            if !needs_snapshot {
                continue;
            }
            if applied.is_some_and(|seq| snapshot_seq < seq) {
                // Behind for this advanced subscriber — skip.
                continue;
            }
            let frame = DaemonEntityFrame::Snapshot {
                subscription_id: subscription_id.clone(),
                entity_type: entity_type.clone(),
                snapshot_seq,
                items: items.clone(),
                resync_reason: subscription
                    .resync_reason
                    .clone()
                    .or_else(|| Some("package_entity_resync".to_string())),
            };
            if entity_frame_exceeds_limit(&frame) {
                continue;
            }
            state.lifecycle_counters.entity_delivery_attempts = state
                .lifecycle_counters
                .entity_delivery_attempts
                .saturating_add(1);
            match subscription.sender.try_send_kind(frame) {
                Ok(()) => {
                    state.lifecycle_counters.entity_delivery_successes = state
                        .lifecycle_counters
                        .entity_delivery_successes
                        .saturating_add(1);
                    subscription.package_last_applied_seq = Some(snapshot_seq);
                    subscription.package_catching_up = snapshot_seq < family_floor;
                    subscription.resync_reason = None;
                }
                Err(EntityFrameTrySendError::Full(_)) => {
                    state.lifecycle_counters.entity_delivery_overflows = state
                        .lifecycle_counters
                        .entity_delivery_overflows
                        .saturating_add(1);
                    subscription.package_catching_up = true;
                    subscription.resync_reason = Some("subscriber_overflow".to_string());
                    if let Some(runtime) = daemon.runtime() {
                        runtime.mark_package_entity_resync_needed(&entity_type);
                    }
                }
                Err(EntityFrameTrySendError::Disconnected) => {
                    state.lifecycle_counters.entity_delivery_failures = state
                        .lifecycle_counters
                        .entity_delivery_failures
                        .saturating_add(1);
                    dead.push(subscription_id.clone());
                }
            }
        }
        for subscription_id in dead {
            state.entity_subscriptions.remove(&subscription_id);
        }

        // After snapshot floor advance, fanout any drained pending deltas.
        drive_package_entity_fanout(daemon, state);

        // Clear need when floor matches high water and no catching_up remains.
        if let Some(runtime) = daemon.runtime() {
            let still_catching_up = state.entity_subscriptions.values().any(|subscription| {
                subscription.entity_type == entity_type && subscription.package_catching_up
            });
            if !still_catching_up {
                runtime.recompute_package_entity_resync(&entity_type);
            } else {
                runtime.mark_package_entity_resync_needed(&entity_type);
            }
        }
    }
    state.lifecycle_counters.live_entity_subscriptions = state.entity_subscriptions.len() as u64;
}

fn take_snapshot_item_page(
    projection: &crate::session_projection::SessionProjection,
    after: Option<&str>,
    max_items: usize,
    max_bytes: usize,
    max_elapsed: Duration,
) -> (Vec<Value>, Option<String>, bool) {
    let started = Instant::now();
    let mut items = Vec::new();
    let mut last_id = after.map(str::to_string);
    let mut more = false;
    for (id, row) in rows_after(&projection.rows, after) {
        if items.len() >= max_items || started.elapsed() >= max_elapsed {
            more = true;
            break;
        }
        let entity = crate::session_projection::SessionProjection::project_entity(&row.record);
        let value = serde_json::to_value(&entity).expect("serialize session entity");
        items.push(value);
        let trial = DaemonEntityFrame::Snapshot {
            subscription_id: String::new(),
            entity_type: "session".to_string(),
            snapshot_seq: 0,
            items: items.clone(),
            resync_reason: None,
        };
        let encoded = serde_json::to_vec(&trial)
            .map(|body| body.len())
            .unwrap_or(0);
        if encoded > max_bytes {
            items.pop();
            more = true;
            break;
        }
        last_id = Some(id.clone());
    }
    (items, last_id, more)
}

fn store_snapshot_items(state: &mut EntitySubscriptionState, items: &[Value]) {
    for value in items {
        if let Ok(entity) = serde_json::from_value::<DaemonSessionEntity>(value.clone()) {
            state.entities.insert(entity.session_uuid.clone(), entity);
        }
    }
}

enum SnapshotAssemble {
    Continue { page: DeliveryPage },
    Closed { frame_too_large: bool },
}

#[allow(clippy::too_many_arguments)]
fn continue_session_snapshot_assembly(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    caught_up: bool,
    counters: &mut DaemonLifecycleCounters,
    max_items: usize,
    max_bytes: usize,
    max_elapsed: Duration,
) -> SnapshotAssemble {
    if !caught_up || !projection.baseline_complete || projection.gap {
        state.needs_delivery = true;
        return SnapshotAssemble::Continue {
            page: DeliveryPage {
                items: 0,
                bytes: 0,
                more: true,
            },
        };
    }
    let live_seq = projection
        .cursor
        .as_ref()
        .map(|cursor| cursor.sequence)
        .unwrap_or(0);
    let source_seq = match state.delivery_phase {
        DeliveryPhase::Assembling { source_seq } => source_seq,
        _ => live_seq,
    };
    if source_seq != live_seq || !matches!(state.delivery_phase, DeliveryPhase::Assembling { .. }) {
        reset_snapshot_assembly(state, live_seq);
    }
    let (items, last_id, more) = take_snapshot_item_page(
        projection,
        state.delivery_after.as_deref(),
        max_items.max(1),
        max_bytes.max(1),
        max_elapsed,
    );
    if items.is_empty() && more {
        return close_oversized_session_snapshot(subscription_id, state, counters);
    }
    let page_item_bytes = encoded_item_bytes(&items);
    let page_bytes = page_item_bytes.saturating_add(snapshot_separator_bytes(
        state.assembled_items.len(),
        items.len(),
    ));
    let envelope = snapshot_envelope_bytes(subscription_id, state);
    if state
        .assembled_item_bytes
        .saturating_add(page_bytes)
        .saturating_add(envelope)
        > DAEMON_MAX_FRAME_BYTES
    {
        return close_oversized_session_snapshot(subscription_id, state, counters);
    }
    let page_item_count = items.len();
    store_snapshot_items(state, &items);
    state.assembled_items.extend(items);
    state.assembled_item_bytes = state.assembled_item_bytes.saturating_add(page_bytes);
    state.delivery_after = last_id;
    if more {
        state.needs_delivery = true;
        return SnapshotAssemble::Continue {
            page: DeliveryPage {
                items: page_item_count,
                bytes: page_bytes,
                more: true,
            },
        };
    }
    let snapshot = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: state.next_seq,
        items: std::mem::take(&mut state.assembled_items),
        resync_reason: state.resync_reason.clone(),
    };
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    match state.sender.try_send_kind(snapshot) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.resync_reason = None;
            state.delivery_phase = DeliveryPhase::Removes;
            state.delivery_after = None;
            state.assembled_item_bytes = 0;
            state.needs_delivery = false;
            SnapshotAssemble::Continue {
                page: DeliveryPage {
                    items: page_item_count,
                    bytes: page_bytes,
                    more: false,
                },
            }
        }
        Err(EntityFrameTrySendError::Full(frame)) => {
            if let DaemonEntityFrame::Snapshot { items: queued, .. } = frame {
                state.assembled_items = queued;
            }
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            state.needs_delivery = true;
            SnapshotAssemble::Continue {
                page: DeliveryPage {
                    items: 0,
                    bytes: 0,
                    more: true,
                },
            }
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            SnapshotAssemble::Closed {
                frame_too_large: false,
            }
        }
    }
}

fn reset_snapshot_assembly(state: &mut EntitySubscriptionState, source_seq: u64) {
    state.entities.clear();
    state.assembled_items.clear();
    state.assembled_item_bytes = 0;
    state.delivery_after = None;
    state.delivery_phase = DeliveryPhase::Assembling { source_seq };
}

fn encoded_item_bytes(items: &[Value]) -> usize {
    items
        .iter()
        .map(|item| serde_json::to_vec(item).map(|body| body.len()).unwrap_or(0))
        .sum()
}

fn snapshot_separator_bytes(existing_items: usize, new_items: usize) -> usize {
    if new_items == 0 {
        return 0;
    }
    new_items
        .saturating_sub(1)
        .saturating_add(usize::from(existing_items > 0))
}

fn snapshot_envelope_bytes(subscription_id: &str, state: &EntitySubscriptionState) -> usize {
    let empty = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: state.next_seq,
        items: Vec::new(),
        resync_reason: state.resync_reason.clone(),
    };
    serde_json::to_vec(&empty)
        .map(|body| body.len())
        .unwrap_or(256)
}

fn close_oversized_session_snapshot(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    counters: &mut DaemonLifecycleCounters,
) -> SnapshotAssemble {
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    let error = DaemonEntityFrame::Error {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        code: "entity_provider_frame_too_large".to_string(),
        message: "session snapshot exceeds daemon frame limit".to_string(),
    };
    match state.sender.try_send_kind(error) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
        }
    }
    state.needs_delivery = false;
    state.resync_reason = None;
    state.delivery_phase = DeliveryPhase::Removes;
    state.delivery_after = None;
    state.assembled_items.clear();
    state.assembled_item_bytes = 0;
    SnapshotAssemble::Closed {
        frame_too_large: true,
    }
}

fn rows_after<'a, V>(
    rows: &'a BTreeMap<String, V>,
    after: Option<&str>,
) -> impl Iterator<Item = (&'a String, &'a V)> + 'a {
    let start = match after {
        Some(after) => Bound::Excluded(after),
        None => Bound::Unbounded,
    };
    rows.range::<str, _>((start, Bound::Unbounded))
}

fn encoded_frame_len(frame: &DaemonEntityFrame) -> usize {
    serde_json::to_vec(frame)
        .map(|body| body.len())
        .unwrap_or(0)
}

fn try_resync_from_projection(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    caught_up: bool,
    reason: String,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    state.next_seq = state.next_seq.saturating_add(1);
    state.entities.clear();
    state.delivery_after = None;
    state.delivery_phase = DeliveryPhase::Assembling {
        source_seq: projection
            .cursor
            .as_ref()
            .map(|cursor| cursor.sequence)
            .unwrap_or(0),
    };
    state.resync_reason = Some(reason);
    state.needs_delivery = true;
    match continue_session_snapshot_assembly(
        subscription_id,
        state,
        projection,
        caught_up,
        counters,
        SESSION_DELIVERY_MAX_ITEMS,
        SESSION_DELIVERY_MAX_BYTES,
        SESSION_DELIVERY_MAX_ELAPSED,
    ) {
        SnapshotAssemble::Continue { page } => {
            if page.more {
                state.needs_delivery = true;
            }
            true
        }
        SnapshotAssemble::Closed {
            frame_too_large: true,
        }
        | SnapshotAssemble::Closed {
            frame_too_large: false,
        } => false,
    }
}

fn deliver_projection_delta_page(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    projection: &crate::session_projection::SessionProjection,
    counters: &mut DaemonLifecycleCounters,
    max_items: usize,
    max_bytes: usize,
    max_elapsed: Duration,
) -> (bool, DeliveryPage) {
    let started = Instant::now();
    let mut page = DeliveryPage {
        items: 0,
        bytes: 0,
        more: false,
    };
    let mut last_id = state.delivery_after.clone();
    if state.delivery_phase == DeliveryPhase::Removes {
        let after = state.delivery_after.clone();
        let mut visited = Vec::new();
        for (id, _) in rows_after(&state.entities, after.as_deref()) {
            if started.elapsed() >= max_elapsed || visited.len() >= max_items.saturating_add(1) {
                page.more = true;
                break;
            }
            visited.push(id.clone());
        }
        for id in visited {
            if page.items >= max_items || started.elapsed() >= max_elapsed {
                page.more = true;
                break;
            }
            last_id = Some(id.clone());
            if projection.rows.contains_key(&id) {
                continue;
            }
            state.next_seq = state.next_seq.saturating_add(1);
            let frame = DaemonEntityFrame::Remove {
                subscription_id: subscription_id.to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: state.next_seq,
                id: id.clone(),
            };
            let bytes = encoded_frame_len(&frame);
            if bytes > max_bytes.saturating_sub(page.bytes) {
                state.next_seq = state.next_seq.saturating_sub(1);
                if page.items == 0 {
                    counters.entity_delivery_overflows =
                        counters.entity_delivery_overflows.saturating_add(1);
                    state.resync_reason = Some("subscriber_overflow".to_string());
                    return (true, page);
                }
                page.more = true;
                break;
            }
            match send_session_delta(state, counters, frame, projection) {
                SendDelta::Alive { bytes } => {
                    page.items += 1;
                    page.bytes = page.bytes.saturating_add(bytes);
                    last_id = Some(id);
                }
                SendDelta::Overflow => return (true, page),
                SendDelta::Dead => return (false, page),
            }
        }
        if !page.more {
            state.delivery_phase = DeliveryPhase::Rows;
            state.delivery_after = None;
            last_id = None;
        }
    }
    if !page.more && state.delivery_phase == DeliveryPhase::Rows {
        let after = state.delivery_after.clone();
        for (id, row) in rows_after(&projection.rows, after.as_deref()) {
            if page.items >= max_items || started.elapsed() >= max_elapsed {
                page.more = true;
                break;
            }
            let entity = crate::session_projection::SessionProjection::project_entity(&row.record);
            let frame = match state.entities.get(id) {
                None => {
                    state.next_seq = state.next_seq.saturating_add(1);
                    DaemonEntityFrame::Upsert {
                        subscription_id: subscription_id.to_string(),
                        entity_type: "session".to_string(),
                        snapshot_seq: state.next_seq,
                        id: id.clone(),
                        entity: serde_json::to_value(&entity).expect("serialize session entity"),
                    }
                }
                Some(previous) if previous != &entity => {
                    state.next_seq = state.next_seq.saturating_add(1);
                    DaemonEntityFrame::Patch {
                        subscription_id: subscription_id.to_string(),
                        entity_type: "session".to_string(),
                        snapshot_seq: state.next_seq,
                        id: id.clone(),
                        patch: crate::session_projection::SessionProjection::entity_patch(
                            previous, &entity,
                        ),
                    }
                }
                Some(_) => {
                    last_id = Some(id.clone());
                    continue;
                }
            };
            let bytes = encoded_frame_len(&frame);
            if bytes > max_bytes.saturating_sub(page.bytes) {
                state.next_seq = state.next_seq.saturating_sub(1);
                if page.items == 0 {
                    counters.entity_delivery_overflows =
                        counters.entity_delivery_overflows.saturating_add(1);
                    state.resync_reason = Some("subscriber_overflow".to_string());
                    return (true, page);
                }
                page.more = true;
                break;
            }
            match send_session_delta(state, counters, frame, projection) {
                SendDelta::Alive { bytes } => {
                    page.items += 1;
                    page.bytes = page.bytes.saturating_add(bytes);
                    last_id = Some(id.clone());
                }
                SendDelta::Overflow => return (true, page),
                SendDelta::Dead => return (false, page),
            }
        }
        if !page.more {
            state.delivery_phase = DeliveryPhase::Removes;
            state.delivery_after = None;
            state.needs_delivery = false;
            return (true, page);
        }
    }
    state.delivery_after = last_id;
    (true, page)
}

enum SendDelta {
    Alive { bytes: usize },
    Overflow,
    Dead,
}

fn send_session_delta(
    state: &mut EntitySubscriptionState,
    counters: &mut DaemonLifecycleCounters,
    frame: DaemonEntityFrame,
    projection: &crate::session_projection::SessionProjection,
) -> SendDelta {
    let bytes = serde_json::to_vec(&frame)
        .map(|body| body.len())
        .unwrap_or(0);
    counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
    match state.sender.try_send_kind(frame.clone()) {
        Ok(()) => {
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            match frame {
                DaemonEntityFrame::Remove { id, .. } => {
                    state.entities.remove(&id);
                }
                DaemonEntityFrame::Upsert { id, entity, .. } => {
                    if let Ok(parsed) = serde_json::from_value(entity) {
                        state.entities.insert(id, parsed);
                    }
                }
                DaemonEntityFrame::Patch { id, .. } => {
                    if let Some(row) = projection.rows.get(&id) {
                        state.entities.insert(
                            id,
                            crate::session_projection::SessionProjection::project_entity(
                                &row.record,
                            ),
                        );
                    }
                }
                _ => {}
            }
            SendDelta::Alive { bytes }
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            state.resync_reason = Some("subscriber_overflow".to_string());
            SendDelta::Overflow
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            SendDelta::Dead
        }
    }
}

#[cfg(test)]
fn try_resync_subscription(
    subscription_id: &str,
    state: &mut EntitySubscriptionState,
    baseline: SessionLifecycleBaseline,
    reason: String,
    counters: &mut DaemonLifecycleCounters,
) -> bool {
    let cursor = baseline.cursor.clone();
    let (entities, snapshot) = entity_snapshot(subscription_id, baseline, Some(reason));
    match state.sender.try_send_kind(snapshot) {
        Ok(()) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_successes =
                counters.entity_delivery_successes.saturating_add(1);
            state.cursor = Some(cursor);
            state.entities = entities;
            state.resync_reason = None;
            true
        }
        Err(EntityFrameTrySendError::Full(_)) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_overflows =
                counters.entity_delivery_overflows.saturating_add(1);
            true
        }
        Err(EntityFrameTrySendError::Disconnected) => {
            counters.entity_delivery_attempts = counters.entity_delivery_attempts.saturating_add(1);
            counters.entity_delivery_failures = counters.entity_delivery_failures.saturating_add(1);
            false
        }
    }
}

#[cfg(test)]
fn entity_snapshot(
    subscription_id: &str,
    baseline: SessionLifecycleBaseline,
    resync_reason: Option<String>,
) -> (BTreeMap<String, DaemonSessionEntity>, DaemonEntityFrame) {
    let entities = baseline
        .sessions
        .iter()
        .map(project_session_entity)
        .map(|entity| (entity.session_uuid.clone(), entity))
        .collect::<BTreeMap<_, _>>();
    let frame = DaemonEntityFrame::Snapshot {
        subscription_id: subscription_id.to_string(),
        entity_type: "session".to_string(),
        snapshot_seq: baseline.cursor.sequence,
        items: entities
            .values()
            .map(|entity| serde_json::to_value(entity).expect("serialize session entity"))
            .collect(),
        resync_reason,
    };
    (entities, frame)
}

#[cfg(test)]
fn project_session_entity(record: &SessionLifecycleRecord) -> DaemonSessionEntity {
    let (lifecycle, exit_code, failure_reason) = match &record.lifecycle {
        Some(SessionLifecycleState::Starting) => (Some("starting".to_string()), None, None),
        Some(SessionLifecycleState::Running) => (Some("running".to_string()), None, None),
        Some(SessionLifecycleState::Stopping) => (Some("stopping".to_string()), None, None),
        Some(SessionLifecycleState::Exited { code }) => (Some("exited".to_string()), *code, None),
        Some(SessionLifecycleState::Failed { reason }) => {
            (Some("failed".to_string()), None, Some(reason.clone()))
        }
        None => (None, None, None),
    };
    let lifecycle_class =
        session_lifecycle_class(&record.session.registry_state, record.lifecycle.as_ref());
    let metadata = &record.metadata.entries;
    let traits = metadata
        .get("botster.session_type.traits")
        .and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .unwrap_or_default();
    DaemonSessionEntity {
        session_uuid: record.session.session_id.0.clone(),
        registry_state: match record.session.registry_state {
            RegistrySessionState::Running => "running",
            RegistrySessionState::Stopping => "stopping",
            RegistrySessionState::Exited => "exited",
            RegistrySessionState::Stale => "stale",
        }
        .to_string(),
        lifecycle,
        lifecycle_class: lifecycle_class.to_string(),
        rows: record.session.size.rows,
        cols: record.session.size.cols,
        updated_at: record.session.updated_at,
        exit_code,
        failure_reason,
        session_type_id: metadata.get("botster.session_type.id").cloned(),
        session_type_source: metadata.get("botster.session_type.source").cloned(),
        role: metadata.get("botster.session_type.role").cloned(),
        traits,
        interaction: metadata.get("botster.session_type.interaction").cloned(),
        session_type_lifecycle: metadata.get("botster.session_type.lifecycle").cloned(),
    }
}

#[cfg(test)]
fn session_lifecycle_class(
    registry_state: &RegistrySessionState,
    lifecycle: Option<&SessionLifecycleState>,
) -> &'static str {
    if registry_state == &RegistrySessionState::Stale {
        "indeterminate"
    } else {
        match lifecycle {
            Some(
                SessionLifecycleState::Starting
                | SessionLifecycleState::Running
                | SessionLifecycleState::Stopping,
            ) => "current",
            Some(SessionLifecycleState::Exited { .. } | SessionLifecycleState::Failed { .. }) => {
                "ended"
            }
            None => "indeterminate",
        }
    }
}

#[cfg(test)]
fn session_entity_patch(previous: &DaemonSessionEntity, current: &DaemonSessionEntity) -> Value {
    let previous = serde_json::to_value(previous).expect("serialize previous session entity");
    let current = serde_json::to_value(current).expect("serialize current session entity");
    let previous = previous.as_object().expect("session entity object");
    let current = current.as_object().expect("session entity object");
    Value::Object(
        current
            .iter()
            .filter(|(key, value)| previous.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

pub(super) fn entity_subscription_error(
    code: &str,
    subscription_id: &str,
    message: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: code.to_string(),
        request_id: subscription_id.to_string(),
        operation: "subscribe_entities".to_string(),
        message: message.to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(
            "subscribe_entities",
            message,
        )],
    });
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::mpsc;

    use botster_core::{RequestId, SessionId};
    use botster_core_daemon::{
        RegistrySessionState, SessionLifecycleBaseline, SessionLifecycleCursor,
    };
    use botster_hub_client::{
        DaemonEntityFrame, DaemonLifecycleCounters, DaemonResponseKind, DaemonSessionEntity,
    };
    use serde_json::Value;

    use crate::HubDaemon;
    use crate::daemon_transport::DaemonControlState;

    #[test]
    fn session_lifecycle_class_is_total_and_stale_first() {
        let concrete = [
            (SessionLifecycleState::Starting, "current"),
            (SessionLifecycleState::Running, "current"),
            (SessionLifecycleState::Stopping, "current"),
            (SessionLifecycleState::Exited { code: Some(0) }, "ended"),
            (
                SessionLifecycleState::Failed {
                    reason: "failed".to_string(),
                },
                "ended",
            ),
        ];
        for (lifecycle, expected) in &concrete {
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Running, Some(lifecycle)),
                *expected
            );
            assert_eq!(
                session_lifecycle_class(&RegistrySessionState::Stale, Some(lifecycle)),
                "indeterminate"
            );
        }
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Running, None),
            "indeterminate"
        );
        assert_eq!(
            session_lifecycle_class(&RegistrySessionState::Stale, None),
            "indeterminate"
        );
    }

    #[test]
    fn session_entity_patch_explicitly_updates_required_lifecycle_class() {
        let entity = |registry_state: &str, lifecycle: Option<&str>, lifecycle_class: &str| {
            DaemonSessionEntity {
                session_uuid: "session-1".to_string(),
                registry_state: registry_state.to_string(),
                lifecycle: lifecycle.map(str::to_string),
                lifecycle_class: lifecycle_class.to_string(),
                rows: 24,
                cols: 80,
                updated_at: 1,
                exit_code: None,
                failure_reason: None,
                session_type_id: None,
                session_type_source: None,
                role: None,
                traits: Vec::new(),
                interaction: None,
                session_type_lifecycle: None,
            }
        };
        let current = entity("running", Some("running"), "current");
        let ended = entity("exited", Some("exited"), "ended");
        let no_lifecycle = entity("running", None, "indeterminate");
        let stale = entity("stale", Some("running"), "indeterminate");

        assert_eq!(
            session_entity_patch(&current, &ended)["lifecycle_class"],
            "ended"
        );
        assert_eq!(
            session_entity_patch(&current, &no_lifecycle)["lifecycle_class"],
            "indeterminate"
        );
        assert_eq!(
            session_entity_patch(&current, &stale)["lifecycle_class"],
            "indeterminate"
        );
    }

    #[test]
    fn live_session_entity_subscription_emits_exact_stale_transition_patch() {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-stale-transition-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "stale-transition-test".to_string(),
                display_name: "Stale Transition Test".to_string(),
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
        .expect("build stale transition config");
        let mut daemon = HubDaemon::start(config).expect("start stale transition daemon");
        let session_id = SessionId("stale-transition-session".to_string());
        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .spawn_session(
                botster_core::SessionSpawnRequest {
                    request_id: RequestId("stale-transition-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sh".to_string(),
                    arguments: vec![
                        "-c".to_string(),
                        "while IFS= read -r line; do printf '%s\\n' \"$line\"; done".to_string(),
                    ],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
                1,
            )
            .expect("spawn worker-backed session");

        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        for _ in 0..16 {
            let kind = state.maintenance.scheduler.take_slice();
            if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
        }
        let (sender, receiver) = mpsc::sync_channel(4);
        let response = register_entity_subscription(
            &mut daemon,
            &mut state,
            "session".to_string(),
            "stale-transition-subscription".to_string(),
            EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register entity subscription");
        assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
        let mut first = receiver.try_recv().ok();
        for _ in 0..32 {
            if first.is_some() {
                break;
            }
            let kind = state.maintenance.scheduler.take_slice();
            if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
            drive_entity_subscriptions(&mut daemon, &mut state);
            first = receiver.try_recv().ok();
        }
        let first = first.expect("initial authoritative snapshot");
        match first {
            DaemonEntityFrame::Snapshot { ref items, .. } => {
                assert!(
                    items.iter().any(|entity| {
                        entity.get("session_uuid").and_then(Value::as_str) == Some(&session_id.0)
                            && entity.get("lifecycle_class").and_then(Value::as_str)
                                == Some("current")
                    }),
                    "first snapshot must contain the live session"
                );
            }
            other => panic!("expected populated snapshot, got {other:?}"),
        }

        daemon
            .runtime()
            .expect("runtime initialized")
            .mark_session_stale(&session_id, 2)
            .expect("mark live session stale through core daemon");
        for _ in 0..16 {
            let kind = state.maintenance.scheduler.take_slice();
            if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
            drive_entity_subscriptions(&mut daemon, &mut state);
        }
        assert!(matches!(
            receiver.recv().expect("stale transition patch"),
            DaemonEntityFrame::Patch {
                ref id,
                ref patch,
                ..
            } if id == &session_id.0
                && patch == &serde_json::json!({
                    "registry_state": "stale",
                    "lifecycle_class": "indeterminate",
                    "updated_at": 2
                })
        ));

        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .shutdown_session(session_id, 3)
            .expect("stop worker-backed test session");
        daemon.stop();
        let _ = fs::remove_dir_all(data_directory);
    }

    #[test]
    fn existing_session_subscriber_receives_spawn_upsert_without_another_request() {
        let data_directory = std::env::temp_dir().join(format!(
            "botster-hub-existing-sub-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "existing-sub-test".to_string(),
                display_name: "Existing Subscriber Test".to_string(),
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
        .expect("build existing subscriber config");
        let mut daemon = HubDaemon::start(config).expect("start existing subscriber daemon");
        let mut state = DaemonControlState::default();
        seed_lifecycle_reconciliation(&mut daemon, &mut state);
        for _ in 0..16 {
            let kind = state.maintenance.scheduler.take_slice();
            if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
        }
        let (sender, receiver) = mpsc::sync_channel(8);
        let response = register_entity_subscription(
            &mut daemon,
            &mut state,
            "session".to_string(),
            "existing-sub".to_string(),
            EntityFrameSender::Blocking(sender),
            None,
        )
        .expect("register idle session subscription");
        assert_eq!(response.kind, DaemonResponseKind::EntitySubscribed);
        let mut first = receiver.try_recv().ok();
        for _ in 0..32 {
            if first.is_some() {
                break;
            }
            let kind = state.maintenance.scheduler.take_slice();
            if kind == crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery {
                drive_entity_subscriptions(&mut daemon, &mut state);
            } else if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
            first = receiver.try_recv().ok();
        }
        match first.expect("first snapshot before spawn") {
            DaemonEntityFrame::Snapshot { .. } => {}
            other => panic!("expected first snapshot, got {other:?}"),
        }
        let session_id = SessionId("assemble-ready-spawn".to_string());
        daemon
            .runtime_mut()
            .expect("runtime initialized")
            .spawn_session(
                botster_core::SessionSpawnRequest {
                    request_id: RequestId("existing-sub-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sleep".to_string(),
                    arguments: vec!["8".to_string()],
                    working_directory: botster_core::SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: botster_core::SpawnEnvironment::default(),
                    initial_pty_size: Some(botster_core::ResizePayload { rows: 24, cols: 80 }),
                },
                botster_core::CoreSessionMetadata::new(),
                1,
            )
            .expect("spawn after first snapshot");
        state.maintenance.note_authoritative_mutation();
        let mut saw_ready = false;
        for _ in 0..16 {
            let kind = state.maintenance.scheduler.take_slice();
            if kind == crate::daemon_maintenance::MaintenanceSliceKind::SubscriberDelivery {
                drive_entity_subscriptions(&mut daemon, &mut state);
            } else if let Some(runtime) = daemon.runtime() {
                crate::daemon_maintenance::run_maintenance_kind(
                    runtime,
                    &mut state.maintenance,
                    kind,
                );
            }
            while let Ok(frame) = receiver.try_recv() {
                match frame {
                    DaemonEntityFrame::Upsert { id, .. } | DaemonEntityFrame::Patch { id, .. }
                        if id == session_id.0 =>
                    {
                        saw_ready = true;
                    }
                    DaemonEntityFrame::Snapshot { items, .. }
                        if items.iter().any(|entity| {
                            entity.get("session_uuid").and_then(Value::as_str)
                                == Some(session_id.0.as_str())
                        }) =>
                    {
                        saw_ready = true;
                    }
                    DaemonEntityFrame::Error { code, message, .. } => {
                        panic!("existing subscriber error: {code}: {message}");
                    }
                    _ => {}
                }
            }
            if saw_ready {
                break;
            }
        }
        assert!(
            saw_ready,
            "existing subscriber must receive assemble-ready-spawn without another client request"
        );
        let _ = daemon
            .runtime_mut()
            .expect("runtime initialized")
            .shutdown_session(session_id, 2);
        daemon.stop();
        let _ = fs::remove_dir_all(data_directory);
    }

    #[test]
    fn entity_overflow_requires_empty_snapshot_resync_and_failed_delivery_disconnects() {
        let fixture =
            botster_hub_test_support::session_lifecycle_subscription_conformance_scenario();
        let overflow_reason = fixture.overflow.resync_reason.clone();
        assert!(fixture.overflow.empty_snapshot_valid);
        assert!(fixture.overflow.snapshot_precedes_later_deltas);
        assert!(
            fixture
                .overflow
                .failed_snapshot_delivery_closes_subscription
        );
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, receiver) = mpsc::sync_channel(1);
        sender
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            })
            .expect("fill bounded subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: Some(overflow_reason.clone()),
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str())
        );
        let _ = receiver.recv().expect("drain stale queued frame");
        assert!(try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.recv().expect("receive empty resync snapshot"),
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }

    #[test]
    fn session_type_resync_replaces_oversized_snapshot_with_typed_error() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut subscriptions = BTreeMap::from([(
            "oversized-session-types".to_string(),
            EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session_type".to_string(),
                cursor: None,
                entities: BTreeMap::new(),
                definition_generation: 1,
                definition_entities: BTreeMap::new(),
                resync_reason: Some("subscriber_overflow".to_string()),
                owner_grant_id: None,
                package_last_applied_seq: None,
                package_catching_up: false,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Removes,
                next_seq: 0,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            },
        )]);
        let entities = BTreeMap::from([(
            "device/oversized".to_string(),
            serde_json::json!({ "description": "x".repeat(DAEMON_MAX_FRAME_BYTES) }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 2, &entities);

        assert!(
            subscriptions.is_empty(),
            "typed error closes the subscription"
        );
        assert!(matches!(
            receiver.recv().expect("receive bounded typed error"),
            DaemonEntityFrame::Error {
                ref subscription_id,
                ref entity_type,
                ref code,
                ..
            } if subscription_id == "oversized-session-types"
                && entity_type == "session_type"
                && code == "entity_provider_frame_too_large"
        ));
    }

    fn session_type_subscription_state(
        sender: mpsc::SyncSender<DaemonEntityFrame>,
        definition_generation: u64,
        next_seq: u64,
        definition_entities: BTreeMap<String, Value>,
        resync_reason: Option<String>,
    ) -> EntitySubscriptionState {
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session_type".to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation,
            definition_entities,
            resync_reason,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        }
    }

    fn session_type_delta_seqs(
        receiver: &mpsc::Receiver<DaemonEntityFrame>,
    ) -> Vec<(String, u64, String)> {
        receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert {
                    subscription_id,
                    snapshot_seq,
                    id,
                    ..
                }
                | DaemonEntityFrame::Remove {
                    subscription_id,
                    snapshot_seq,
                    id,
                    ..
                } => Some((subscription_id, snapshot_seq, id)),
                DaemonEntityFrame::Snapshot {
                    subscription_id,
                    snapshot_seq,
                    ..
                } => Some((subscription_id, snapshot_seq, "snapshot".to_string())),
                DaemonEntityFrame::Error { .. } | DaemonEntityFrame::Patch { .. } => None,
            })
            .collect()
    }

    #[test]
    fn session_type_same_generation_multi_row_uses_contiguous_subscriber_seq() {
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                1,
                1,
                BTreeMap::from([
                    (
                        "device/alpha".to_string(),
                        serde_json::json!({ "label": "Alpha" }),
                    ),
                    (
                        "device/beta".to_string(),
                        serde_json::json!({ "label": "Beta" }),
                    ),
                ]),
                None,
            ),
        )]);
        let entities = BTreeMap::from([
            (
                "device/alpha".to_string(),
                serde_json::json!({ "label": "Alpha 2" }),
            ),
            (
                "device/beta".to_string(),
                serde_json::json!({ "label": "Beta 2" }),
            ),
        ]);

        drive_session_type_subscriptions(&mut subscriptions, 2, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![
                (
                    "held-session-types".to_string(),
                    2,
                    "device/alpha".to_string()
                ),
                (
                    "held-session-types".to_string(),
                    3,
                    "device/beta".to_string()
                ),
            ],
            "one generation with two published diffs must deliver N+1 then N+2 on the held subscription"
        );
        assert_eq!(
            subscriptions
                .get("held-session-types")
                .map(|subscription| subscription.next_seq),
            Some(3)
        );
    }

    #[test]
    fn session_type_skipped_generation_uses_contiguous_subscriber_seq() {
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                1,
                1,
                BTreeMap::from([(
                    "device/alpha".to_string(),
                    serde_json::json!({ "label": "Alpha" }),
                )]),
                None,
            ),
        )]);
        let entities = BTreeMap::from([(
            "device/alpha".to_string(),
            serde_json::json!({ "label": "Alpha 3" }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 3, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![(
                "held-session-types".to_string(),
                2,
                "device/alpha".to_string()
            )],
            "a skipped dirty generation must still deliver the next contiguous subscriber seq, not the generation number"
        );
        assert_eq!(
            subscriptions
                .get("held-session-types")
                .map(|subscription| subscription.next_seq),
            Some(2)
        );
    }

    #[test]
    fn session_type_overflow_resync_advances_subscriber_seq_not_generation() {
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut subscriptions = BTreeMap::from([(
            "held-session-types".to_string(),
            session_type_subscription_state(
                sender,
                7,
                7,
                BTreeMap::from([(
                    "device/alpha".to_string(),
                    serde_json::json!({ "label": "Alpha" }),
                )]),
                Some("subscriber_overflow".to_string()),
            ),
        )]);
        let entities = BTreeMap::from([(
            "device/alpha".to_string(),
            serde_json::json!({ "label": "Alpha recovered" }),
        )]);

        drive_session_type_subscriptions(&mut subscriptions, 3, &entities);

        let frames = session_type_delta_seqs(&receiver);
        assert_eq!(
            frames,
            vec![("held-session-types".to_string(), 8, "snapshot".to_string())],
            "overflow resync must send next_seq+1 and must not move snapshot_seq backwards to the generation"
        );
        let subscription = subscriptions
            .get("held-session-types")
            .expect("held subscription remains open");
        assert_eq!(subscription.next_seq, 8);
        assert_eq!(subscription.definition_generation, 3);
        assert!(subscription.resync_reason.is_none());
    }

    #[test]
    fn async_entity_overflow_requires_empty_snapshot_resync_and_closed_delivery_disconnects() {
        let overflow_reason = "subscriber_overflow".to_string();
        let cursor = SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
            sequence: 9,
        };
        let baseline = || SessionLifecycleBaseline {
            cursor: cursor.clone(),
            sessions: Vec::new(),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(DaemonEntityFrame::Snapshot {
                subscription_id: "async-subscription".to_string(),
                entity_type: "session".to_string(),
                snapshot_seq: 8,
                items: Vec::new(),
                resync_reason: None,
            })
            .expect("fill bounded async subscriber queue");
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Async(sender),
            entity_type: "session".to_string(),
            cursor: Some(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("source".to_string()),
                sequence: 8,
            }),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: Some(overflow_reason.clone()),
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();

        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert_eq!(
            state.resync_reason.as_deref(),
            Some(overflow_reason.as_str()),
            "a full production WebRTC queue must retain its pending resync"
        );
        let _ = receiver.try_recv().expect("drain stale async frame");
        assert!(try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason.clone(),
            &mut counters,
        ));
        assert!(state.resync_reason.is_none());
        assert!(matches!(
            receiver.try_recv().expect("receive async resync snapshot"),
            DaemonEntityFrame::Snapshot {
                snapshot_seq: 9,
                ref items,
                resync_reason: Some(ref reason),
                ..
            } if items.is_empty() && reason == &overflow_reason
        ));

        drop(receiver);
        state.resync_reason = Some(overflow_reason.clone());
        assert!(!try_resync_subscription(
            "async-subscription",
            &mut state,
            baseline(),
            overflow_reason,
            &mut counters,
        ));
        assert_eq!(counters.entity_delivery_attempts, 3);
        assert_eq!(counters.entity_delivery_successes, 1);
        assert_eq!(counters.entity_delivery_overflows, 1);
        assert_eq!(counters.entity_delivery_failures, 1);
    }

    #[test]
    fn delivery_page_does_not_skip_a_low_id_after_a_high_remove() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.replace_complete_baseline(
            SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 2,
            },
            Vec::new(),
        );
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        projection.ingest_baseline_rows(2, [record("a")]);
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::from([(
                "z".to_string(),
                crate::session_projection::SessionProjection::project_entity(&record("z")),
            )]),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let (alive, first) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        assert!(alive);
        assert!(first.more);
        let (alive, second) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        assert!(alive);
        assert!(!second.more);
        let frames: Vec<_> = receiver.try_iter().collect();
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DaemonEntityFrame::Remove { id, .. } if id == "z"
        )));
        assert!(frames.iter().any(|frame| matches!(
            frame,
            DaemonEntityFrame::Upsert { id, .. } if id == "a"
        )));
    }

    #[test]
    fn delivery_page_keeps_snapshot_seq_monotonic_when_id_order_reverses_journal_order() {
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(1, [record("z")]);
        projection.ingest_baseline_rows(2, [record("a")]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 2,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Rows,
            next_seq: 0,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: false,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        let seqs: Vec<u64> = receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert { snapshot_seq, .. }
                | DaemonEntityFrame::Patch { snapshot_seq, .. } => Some(snapshot_seq),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, vec![1, 2]);
    }

    #[test]
    fn overflow_resync_does_not_move_snapshot_seq_backwards() {
        let record = |id: &str| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(11, [record("a"), record("b"), record("c")]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 11,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Rows,
            next_seq: 110,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let _ = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            1,
            usize::MAX,
            Duration::from_secs(1),
        );
        state.resync_reason = Some("subscriber_overflow".to_string());
        assert!(try_resync_from_projection(
            "sub",
            &mut state,
            &projection,
            true,
            "subscriber_overflow".to_string(),
            &mut counters,
        ));
        let seqs: Vec<u64> = receiver
            .try_iter()
            .filter_map(|frame| match frame {
                DaemonEntityFrame::Upsert { snapshot_seq, .. }
                | DaemonEntityFrame::Patch { snapshot_seq, .. }
                | DaemonEntityFrame::Snapshot { snapshot_seq, .. }
                | DaemonEntityFrame::Remove { snapshot_seq, .. } => Some(snapshot_seq),
                DaemonEntityFrame::Error { .. } => None,
            })
            .collect();
        assert!(seqs.len() >= 2);
        for window in seqs.windows(2) {
            assert!(window[0] < window[1], "sequences moved backwards: {seqs:?}");
        }
        assert!(seqs[0] > 110 || seqs.iter().any(|seq| *seq > 110));
    }

    #[test]
    fn paged_delivery_stays_within_owner_turn_for_a_large_registry() {
        let record = |id: String| botster_core_daemon::SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..256).map(|index| record(format!("session-{index:03}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let mut delivered = 0;
        for subscriber in 0..2 {
            let (sender, receiver) = mpsc::sync_channel(256);
            let mut state = EntitySubscriptionState {
                sender: EntityFrameSender::Blocking(sender),
                entity_type: "session".to_string(),
                cursor: projection.cursor.clone(),
                entities: BTreeMap::new(),
                definition_generation: 0,
                definition_entities: BTreeMap::new(),
                resync_reason: None,
                owner_grant_id: None,
                package_last_applied_seq: None,
                package_catching_up: false,
                delivery_after: None,
                delivery_phase: DeliveryPhase::Rows,
                next_seq: 0,
                assembled_items: Vec::new(),
                assembled_item_bytes: 0,
                needs_delivery: false,
            };
            let mut counters = DaemonLifecycleCounters::default();
            loop {
                let started = Instant::now();
                let (alive, page) = deliver_projection_delta_page(
                    &format!("sub-{subscriber}"),
                    &mut state,
                    &projection,
                    &mut counters,
                    SESSION_DELIVERY_MAX_ITEMS,
                    SESSION_DELIVERY_MAX_BYTES,
                    SESSION_DELIVERY_MAX_ELAPSED,
                );
                assert!(alive);
                assert!(started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS));
                if !page.more {
                    break;
                }
            }
            delivered += receiver.try_iter().count();
        }
        assert_eq!(delivered, 512);
    }

    #[test]
    fn first_session_snapshot_is_complete_and_assembled_in_pages() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(32);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let mut pages = 0;
        loop {
            let started = Instant::now();
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_ELAPSED,
            ) else {
                panic!("assembly must stay alive");
            };
            assert!(started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS));
            pages += 1;
            if !page.more {
                break;
            }
            assert!(
                matches!(state.delivery_phase, DeliveryPhase::Assembling { .. }),
                "must keep assembling until the complete snapshot"
            );
            assert!(receiver.try_iter().next().is_none());
            assert!(pages < 8);
        }
        assert!(pages > 1);
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot {
                items,
                resync_reason,
                ..
            } => {
                assert_eq!(items.len(), 24);
                assert_eq!(resync_reason, &None);
            }
            other => panic!("expected one complete snapshot, got {other:?}"),
        }
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }

    #[test]
    fn catch_up_restarts_when_a_prefix_id_changes() {
        let record = |id: &str, updated_at: u64| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id.to_string()),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| record(&format!("session-{index:02}"), 1)),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        ) else {
            panic!("first page");
        };
        assert!(page.more);
        assert!(receiver.try_iter().next().is_none());
        projection.ingest_baseline_rows(2, [record("session-00", 9)]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 2,
        });
        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_ELAPSED,
            ) else {
                panic!("restarted assembly");
            };
            if !page.more {
                break;
            }
        }
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot { items, .. } => {
                let first = items
                    .iter()
                    .find(|item| {
                        item.get("session_uuid").and_then(Value::as_str) == Some("session-00")
                    })
                    .expect("prefix row");
                assert_eq!(first.get("updated_at").and_then(Value::as_u64), Some(9));
                assert_eq!(items.len(), 24);
            }
            other => panic!("expected complete snapshot, got {other:?}"),
        }
    }

    #[test]
    fn oversized_first_snapshot_closes_the_subscription() {
        let huge = SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId("x".repeat(DAEMON_MAX_FRAME_BYTES)),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(1, [huge]);
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let first = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            true,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        );
        assert!(matches!(
            first,
            SnapshotAssemble::Closed {
                frame_too_large: true
            }
        ));
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            DaemonEntityFrame::Error { code, .. } if code == "entity_provider_frame_too_large"
        ));
        assert!(!state.needs_delivery);
        assert!(state.resync_reason.is_none());
    }

    #[test]
    fn no_removal_scan_stays_within_owner_turn() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..256).map(|index| record(format!("session-{index:03}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, _receiver) = mpsc::sync_channel(16);
        let mut entities = BTreeMap::new();
        for (id, row) in &projection.rows {
            entities.insert(
                id.clone(),
                crate::session_projection::SessionProjection::project_entity(&row.record),
            );
        }
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities,
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Removes,
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        let started = Instant::now();
        let (alive, page) = deliver_projection_delta_page(
            "sub",
            &mut state,
            &projection,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        );
        assert!(alive);
        assert!(started.elapsed() < Duration::from_millis(crate::MAX_OWNER_TURN_MS));
        assert!(page.more);
        assert!(state.delivery_after.is_some());
    }

    #[test]
    fn near_limit_snapshot_assembly_stays_within_owner_turn() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..20).map(|index| record(format!("s{index:02}-{}", "x".repeat(40 * 1024)))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        // This case proves bounded per-call assembly work, not wall-clock latency.
        // Duration::MAX cannot cut a page, so page cuts are byte-driven: one item per page.
        const MAX_NEAR_LIMIT_PAGES: usize = 21;
        for page_index in 0..MAX_NEAR_LIMIT_PAGES {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                Duration::MAX,
            ) else {
                panic!("near-limit assembly");
            };
            assert!(page.items >= 1);
            assert!(page.items <= SESSION_DELIVERY_MAX_ITEMS);
            assert!(page.bytes <= SESSION_DELIVERY_MAX_BYTES);
            if page.more {
                assert_eq!(receiver.try_iter().count(), 0);
                assert!(!state.assembled_items.is_empty());
                assert!(
                    page_index + 1 < MAX_NEAR_LIMIT_PAGES,
                    "twenty one-item pages cannot need more than twenty useful calls"
                );
            } else {
                break;
            }
        }
        assert!(state.assembled_items.is_empty());
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            DaemonEntityFrame::Snapshot { items, .. } => {
                assert_eq!(items.len(), 20);
                let encoded = serde_json::to_vec(frames.first().expect("frame"))
                    .expect("encode")
                    .len();
                assert!(encoded <= DAEMON_MAX_FRAME_BYTES);
            }
            other => panic!("expected complete snapshot, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_size_charges_json_array_separators() {
        assert_eq!(snapshot_separator_bytes(0, 0), 0);
        assert_eq!(snapshot_separator_bytes(0, 1), 0);
        assert_eq!(snapshot_separator_bytes(0, 3), 2);
        assert_eq!(snapshot_separator_bytes(4, 1), 1);
        assert_eq!(snapshot_separator_bytes(4, 2), 2);
    }

    #[test]
    fn separators_close_when_item_bytes_fit_but_commas_do_not() {
        let record = |id: String| SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        };
        let probe = serde_json::to_value(
            crate::session_projection::SessionProjection::project_entity(&record("sep-00".into())),
        )
        .expect("probe");
        let probe_len = serde_json::to_vec(&probe).expect("encode probe").len();
        let envelope = {
            let (sender, _receiver) = mpsc::sync_channel(1);
            snapshot_envelope_bytes(
                "sub",
                &EntitySubscriptionState {
                    sender: EntityFrameSender::Blocking(sender),
                    entity_type: "session".to_string(),
                    cursor: None,
                    entities: BTreeMap::new(),
                    definition_generation: 0,
                    definition_entities: BTreeMap::new(),
                    resync_reason: None,
                    owner_grant_id: None,
                    package_last_applied_seq: None,
                    package_catching_up: false,
                    delivery_after: None,
                    delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
                    next_seq: 1,
                    assembled_items: Vec::new(),
                    assembled_item_bytes: 0,
                    needs_delivery: true,
                },
            )
        };
        let mut pad = DAEMON_MAX_FRAME_BYTES
            .saturating_sub(envelope)
            .saturating_div(2)
            .saturating_sub(probe_len)
            .saturating_add(64);
        let projection = loop {
            let mut projection = crate::session_projection::SessionProjection::default();
            projection.ingest_baseline_rows(
                1,
                [
                    record(format!("sep-00-{}", "y".repeat(pad))),
                    record(format!("sep-01-{}", "y".repeat(pad))),
                ],
            );
            projection.seal_baseline(SessionLifecycleCursor {
                source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
                sequence: 1,
            });
            let encoded_items: Vec<_> = projection
                .rows
                .values()
                .map(|row| {
                    serde_json::to_value(
                        crate::session_projection::SessionProjection::project_entity(&row.record),
                    )
                    .expect("item")
                })
                .collect();
            let item_only = encoded_item_bytes(&encoded_items);
            let without_commas = item_only.saturating_add(envelope);
            let with_commas =
                without_commas.saturating_add(snapshot_separator_bytes(0, encoded_items.len()));
            if without_commas <= DAEMON_MAX_FRAME_BYTES && with_commas > DAEMON_MAX_FRAME_BYTES {
                break projection;
            }
            if without_commas > DAEMON_MAX_FRAME_BYTES {
                pad = pad.saturating_sub(8);
            } else {
                pad = pad.saturating_add(1);
            }
            assert!(pad > 32, "failed to find separator boundary pad");
        };
        let (sender, receiver) = mpsc::sync_channel(2);
        let mut state = EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: projection.cursor.clone(),
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq: 1 },
            next_seq: 1,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        };
        let mut counters = DaemonLifecycleCounters::default();
        // This case proves separator accounting, not owner-turn latency.
        // Duration::MAX cannot cut a page, so Closed cannot be elapsed-empty.
        const MAX_SEPARATOR_PAGES: usize = 3;
        let mut closed_too_large = false;
        for page_index in 0..MAX_SEPARATOR_PAGES {
            match continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                8,
                DAEMON_MAX_FRAME_BYTES,
                Duration::MAX,
            ) {
                SnapshotAssemble::Closed {
                    frame_too_large: true,
                } => {
                    closed_too_large = true;
                    break;
                }
                SnapshotAssemble::Closed {
                    frame_too_large: false,
                } => panic!("closed without frame_too_large"),
                SnapshotAssemble::Continue { page } => {
                    assert!(page.items > 0, "empty-item continue is not separator proof");
                    assert!(page.more, "completed snapshot without charging separators");
                    assert!(
                        page_index + 1 < MAX_SEPARATOR_PAGES,
                        "two items cannot need more than two useful pages"
                    );
                }
            }
        }
        assert!(closed_too_large, "separator close did not fire");
        let frames: Vec<_> = receiver.try_iter().collect();
        assert!(matches!(
            frames.first(),
            Some(DaemonEntityFrame::Error { code, .. })
                if code == "entity_provider_frame_too_large"
        ));
    }

    fn assemble_record(id: String) -> SessionLifecycleRecord {
        SessionLifecycleRecord {
            session: botster_core_daemon::DaemonSession {
                session_id: SessionId(id),
                registry_state: RegistrySessionState::Running,
                size: botster_core::ResizePayload { rows: 24, cols: 80 },
                process: None,
                updated_at: 1,
            },
            metadata: botster_core::CoreSessionMetadata::new(),
            lifecycle: Some(SessionLifecycleState::Running),
        }
    }

    fn assembling_subscription(
        sender: mpsc::SyncSender<DaemonEntityFrame>,
        source_seq: u64,
    ) -> EntitySubscriptionState {
        EntitySubscriptionState {
            sender: EntityFrameSender::Blocking(sender),
            entity_type: "session".to_string(),
            cursor: None,
            entities: BTreeMap::new(),
            definition_generation: 0,
            definition_entities: BTreeMap::new(),
            resync_reason: None,
            owner_grant_id: None,
            package_last_applied_seq: None,
            package_catching_up: false,
            delivery_after: None,
            delivery_phase: DeliveryPhase::Assembling { source_seq },
            next_seq: source_seq,
            assembled_items: Vec::new(),
            assembled_item_bytes: 0,
            needs_delivery: true,
        }
    }

    fn snapshot_item_ids(frame: &DaemonEntityFrame) -> Vec<String> {
        match frame {
            DaemonEntityFrame::Snapshot { items, .. } => items
                .iter()
                .filter_map(|item| {
                    item.get("session_uuid")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect(),
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    #[test]
    fn first_session_snapshot_holds_until_the_projection_is_caught_up() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(4);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
            "sub",
            &mut state,
            &projection,
            false,
            &mut counters,
            SESSION_DELIVERY_MAX_ITEMS,
            SESSION_DELIVERY_MAX_BYTES,
            SESSION_DELIVERY_MAX_ELAPSED,
        ) else {
            panic!("hold must stay alive");
        };
        assert_eq!(page.items, 0);
        assert!(page.more);
        assert!(state.needs_delivery);
        assert!(matches!(
            state.delivery_phase,
            DeliveryPhase::Assembling { .. }
        ));
        assert!(receiver.try_iter().next().is_none());
    }

    #[test]
    fn first_session_snapshot_completes_when_caught_up() {
        let mut projection = crate::session_projection::SessionProjection::default();
        projection.ingest_baseline_rows(
            1,
            (0..24).map(|index| assemble_record(format!("session-{index:02}"))),
        );
        projection.seal_baseline(SessionLifecycleCursor {
            source_id: botster_core_daemon::SessionLifecycleSourceId("s".into()),
            sequence: 1,
        });
        let (sender, receiver) = mpsc::sync_channel(8);
        let mut state = assembling_subscription(sender, 1);
        let mut counters = DaemonLifecycleCounters::default();
        loop {
            let SnapshotAssemble::Continue { page } = continue_session_snapshot_assembly(
                "sub",
                &mut state,
                &projection,
                true,
                &mut counters,
                SESSION_DELIVERY_MAX_ITEMS,
                SESSION_DELIVERY_MAX_BYTES,
                SESSION_DELIVERY_MAX_ELAPSED,
            ) else {
                panic!("caught-up assembly must stay alive");
            };
            if !page.more {
                break;
            }
        }
        let frames: Vec<_> = receiver.try_iter().collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(snapshot_item_ids(&frames[0]).len(), 24);
        assert_eq!(state.delivery_phase, DeliveryPhase::Removes);
        assert!(!state.needs_delivery);
    }
}
