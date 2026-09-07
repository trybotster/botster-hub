//! Route cleanup for a connection or peer that left: detach every route it
//! owned in Core, release its captures, and apply the owner-side bookkeeping
//! only to the streams that still carry the identity captured at cleanup
//! start. Unix connection cleanup and WebRTC peer cleanup share this path.
//!
//! The candidate list is bounded at admission by
//! [`MAX_ATTACH_ROUTES_PER_OWNER`], and each Core turn detaches at most
//! [`MAX_CLEANUP_ROUTES_PER_TURN`] routes, so one obligation retains a bounded
//! vector and does bounded work per owner turn.

use botster_core::{ClientId, SessionId, SubscriptionId, TerminalSubscriptionGeneration};
use botster_core_daemon::{CaptureOwner, CoreDaemon, DetachTerminalSubscriptionResult};

use crate::daemon::owner_budget::{CoreWorkPoll, ObligationPoll, OwnerPermit, drive_core_slot};
use crate::daemon::owner_loop::DaemonControlState;
use crate::data_plane::driver::CoreTicket;
use crate::subscription::attach_routes::{
    AttachStreamOwner, AttachedSubscription, AttachedSubscriptionChange, AttachmentIdentity,
    RouteReservation, live_generation_for_route, record_attached_subscription_change,
};

/// Attach streams one owner (Unix client or WebRTC grant) may hold at once.
/// Attach refuses beyond it, which bounds every cleanup candidate vector.
pub(crate) const MAX_ATTACH_ROUTES_PER_OWNER: usize = 64;

/// Routes detached in one Core owner turn.
pub(crate) const MAX_CLEANUP_ROUTES_PER_TURN: usize = 16;

/// Operator error code when an owner holds the maximum attach streams.
pub(crate) const ATTACH_ROUTE_LIMIT: &str = "attach_route_limit";

/// One route the departed owner may still own in Core, captured before any
/// owner-side mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CleanupCandidate {
    /// Client id the route is owned under in Core.
    pub core_client_id: String,
    pub session_id: String,
    pub subscription_id: String,
    /// Attach stream identity at capture time; `None` when no stream existed.
    pub identity: Option<AttachmentIdentity>,
    /// Generation recorded on the stream at capture time, when bound.
    pub generation: Option<TerminalSubscriptionGeneration>,
}

/// Per-route result of one cleanup turn on the Core owner thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CleanupRouteOutcome {
    /// Another client owns the route in Core; leave it alone.
    Foreign,
    /// No live generation existed; record the detach without Core work.
    NoGeneration,
    /// Core detached the route (or it was already gone).
    Detached,
    /// Core refused the detach; cleanup is recorded as failed.
    DetachFailed,
}

#[derive(Debug, Clone)]
pub(crate) struct CleanupRouteReport {
    pub candidate: CleanupCandidate,
    pub outcome: CleanupRouteOutcome,
}

/// Build a candidate for a route the departing `owner` was told it
/// attached. The departing owner is preserved: the current stream's identity
/// and generation are adopted only when that stream still belongs to the
/// departing owner. A route whose current stream belongs to someone else is
/// a replacement; it is never targeted through the replacement's identity.
///
/// `core_client_id` names the Core owner of the departing route when no
/// stream of the departing owner exists any more. A Unix connection has a
/// unique Core client id, so its routes are always looked up under it. A
/// WebRTC peer shares its per-subscription Core client id with any
/// replacement peer on the same subscription; when a replacement stream
/// exists, Core already detached the departing generation on reattach and
/// the candidate is dropped (`None`).
pub(crate) fn candidate_for_departing_owner(
    registry: &crate::subscription::attach_routes::AttachStreamRegistry,
    owner: &AttachStreamOwner,
    core_client_id: &str,
    session_id: &str,
    subscription_id: &str,
) -> Option<CleanupCandidate> {
    let current = registry.stream_identity(session_id, subscription_id);
    let owned = registry.stream_owner_matches(session_id, subscription_id, owner);
    match (current, owned) {
        (Some(identity), true) => Some(CleanupCandidate {
            core_client_id: identity.client_id.clone(),
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
            generation: registry.recorded_generation(session_id, subscription_id),
            identity: Some(identity),
        }),
        (Some(_), false) if owner.grant_id.is_some() => None,
        _ => Some(CleanupCandidate {
            core_client_id: core_client_id.to_string(),
            session_id: session_id.to_string(),
            subscription_id: subscription_id.to_string(),
            generation: None,
            identity: None,
        }),
    }
}

/// One Core owner turn: release the owner's captures (first page only) and
/// detach each candidate under its exact generation when known.
fn cleanup_core_turn(
    page: Vec<CleanupCandidate>,
    capture_owner: Option<CaptureOwner>,
    now: u64,
) -> impl FnOnce(&mut CoreDaemon) -> Vec<CleanupRouteReport> + Send + 'static {
    move |daemon| {
        if let Some(owner) = capture_owner.as_ref() {
            let _ = daemon.release_owner_captures(owner);
        }
        let inventory = daemon.list_terminal_subscriptions();
        let mut reports = Vec::with_capacity(page.len());
        for candidate in page {
            let generation = candidate.generation.or_else(|| {
                live_generation_for_route(
                    &inventory,
                    &candidate.core_client_id,
                    &candidate.session_id,
                    &candidate.subscription_id,
                )
            });
            let foreign_core_owner = inventory.iter().any(|row| {
                row.session_id.0 == candidate.session_id
                    && row.subscription_id.0 == candidate.subscription_id
                    && row.client_id.0 != candidate.core_client_id
            });
            let outcome = match generation {
                None if foreign_core_owner => CleanupRouteOutcome::Foreign,
                None => CleanupRouteOutcome::NoGeneration,
                Some(generation) => match daemon.detach_terminal_subscription(
                    ClientId(candidate.core_client_id.clone()),
                    SessionId(candidate.session_id.clone()),
                    SubscriptionId(candidate.subscription_id.clone()),
                    generation,
                    now,
                ) {
                    Ok(
                        DetachTerminalSubscriptionResult::Detached { .. }
                        | DetachTerminalSubscriptionResult::AlreadyGone
                        | DetachTerminalSubscriptionResult::GenerationMismatch { .. },
                    ) => CleanupRouteOutcome::Detached,
                    Err(_) => CleanupRouteOutcome::DetachFailed,
                },
            };
            reports.push(CleanupRouteReport { candidate, outcome });
        }
        reports
    }
}

/// Owner-side bookkeeping totals for one cleanup.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CleanupApplied {
    pub failed: bool,
    pub bound_closes: u64,
}

/// Apply one page of reports. Every branch that mutates owner state checks
/// the captured identity first: a stream that no longer matches belongs to a
/// replacement and is left alone, including its live-attach bookkeeping.
pub(crate) fn apply_cleanup_reports(
    state: &mut DaemonControlState,
    reports: Vec<CleanupRouteReport>,
    applied: &mut CleanupApplied,
) {
    for report in reports {
        let CleanupCandidate {
            session_id,
            subscription_id,
            identity,
            ..
        } = report.candidate;
        let current = state
            .pending_runtime
            .stream_identity(&session_id, &subscription_id);
        let owned_stream = match (current.as_ref(), identity.as_ref()) {
            (Some(current), Some(captured)) => current == captured,
            _ => false,
        };
        // A stream exists that is not the captured one: a replacement owns
        // the key, whatever Core reported for the old owner.
        let replaced = current.is_some() && !owned_stream;
        match report.outcome {
            CleanupRouteOutcome::Foreign => continue,
            CleanupRouteOutcome::DetachFailed => {
                applied.failed = true;
                continue;
            }
            CleanupRouteOutcome::NoGeneration | CleanupRouteOutcome::Detached => {}
        }
        if replaced {
            continue;
        }
        if report.outcome == CleanupRouteOutcome::Detached {
            *state
                .lifecycle_counters
                .cleanup_by_reason
                .entry("cleanup_generation_detach".to_string())
                .or_insert(0) += 1;
        }
        if owned_stream {
            let identity = identity.as_ref().expect("owned stream has identity");
            let was_bound = state
                .pending_runtime
                .is_adapter_bound(&session_id, &subscription_id);
            let _ = state
                .pending_runtime
                .close_adapter_if(&session_id, &subscription_id, identity);
            let _ = state
                .pending_runtime
                .cancel_stream_if(&session_id, &subscription_id, identity);
            if was_bound {
                applied.bound_closes += 1;
            }
        }
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            Some(AttachedSubscriptionChange::Detach(AttachedSubscription {
                session_id,
                subscription_id,
            })),
            None,
        );
    }
}

/// Retain one route-cleanup obligation for a departed owner. The obligation
/// pages through `candidates`, keeps one Core ticket in flight, resubmits on
/// refusal, and calls `on_done` once with the totals.
pub(crate) fn retain_route_cleanup(
    state: &mut DaemonControlState,
    permit: OwnerPermit,
    label: &'static str,
    capture_owner: Option<CaptureOwner>,
    mut candidates: Vec<CleanupCandidate>,
    now: u64,
    mut on_done: impl FnMut(&mut DaemonControlState, CleanupApplied) + Send + 'static,
) {
    candidates.reverse();
    let mut capture_owner = capture_owner;
    let mut slot: Option<CoreTicket<Vec<CleanupRouteReport>>> = None;
    let mut applied = CleanupApplied::default();
    let mut page: Vec<CleanupCandidate> = Vec::new();
    state.budget.retain(permit, label, move |daemon, state| {
        if slot.is_none() {
            if candidates.is_empty() && capture_owner.is_none() && page.is_empty() {
                on_done(state, applied);
                return ObligationPoll::Done;
            }
            if page.is_empty() {
                let take = candidates.len().min(MAX_CLEANUP_ROUTES_PER_TURN);
                page = candidates.split_off(candidates.len() - take);
                page.reverse();
            }
        }
        let turn_page = page.clone();
        let turn_owner = capture_owner.clone();
        match drive_core_slot(&mut slot, daemon, state, |runtime, _| {
            runtime.submit_core(cleanup_core_turn(turn_page, turn_owner, now))
        }) {
            CoreWorkPoll::Pending => ObligationPoll::Pending,
            CoreWorkPoll::Lost => {
                applied.failed = true;
                on_done(state, applied);
                ObligationPoll::Done
            }
            CoreWorkPoll::Ready(reports) => {
                // The page and the capture release are committed in Core;
                // a refused resubmit never repeats them.
                page.clear();
                capture_owner = None;
                apply_cleanup_reports(state, reports, &mut applied);
                ObligationPoll::Pending
            }
        }
    });
}

/// Reserve the route key in the owner's route set before the attach starts.
/// The set is the union of pending, live, and acknowledged keys, capped at
/// [`MAX_ATTACH_ROUTES_PER_OWNER`], so concurrent attaches cannot exceed
/// it and cleanup candidate vectors stay bounded. `false` means refuse.
#[must_use]
pub(crate) fn reserve_attach_route(
    registry: &mut crate::subscription::attach_routes::AttachStreamRegistry,
    owner: &AttachStreamOwner,
    session_id: &str,
    subscription_id: &str,
) -> bool {
    registry.reserve_route(
        &owner.budget_key(),
        session_id,
        subscription_id,
        MAX_ATTACH_ROUTES_PER_OWNER,
    ) != RouteReservation::Full
}

/// Release a route key after an attach failed, unless a stream of this
/// owner still holds the key (a newer attach of the same route).
pub(crate) fn release_failed_attach_route(
    registry: &mut crate::subscription::attach_routes::AttachStreamRegistry,
    owner: &AttachStreamOwner,
    session_id: &str,
    subscription_id: &str,
) {
    if !registry.stream_owner_matches(session_id, subscription_id, owner) {
        registry.release_route(&owner.budget_key(), session_id, subscription_id);
    }
}

#[cfg(test)]
pub(crate) fn candidate_for_test(
    core_client_id: &str,
    session_id: &str,
    subscription_id: &str,
    identity: Option<AttachmentIdentity>,
    generation: Option<TerminalSubscriptionGeneration>,
) -> CleanupCandidate {
    CleanupCandidate {
        core_client_id: core_client_id.to_string(),
        session_id: session_id.to_string(),
        subscription_id: subscription_id.to_string(),
        identity,
        generation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::attach_routes::BoundAdapterHandle;
    use crate::transport::unix::UnixTerminalAdapter;

    fn owner(client: &str) -> AttachStreamOwner {
        AttachStreamOwner {
            client_id: client.to_string(),
            grant_id: None,
        }
    }

    /// H3: a report with no old generation arrives after a replacement took
    /// the route key. The replacement's live-attach and grant bookkeeping
    /// must survive.
    #[test]
    fn no_generation_report_after_replacement_leaves_replacement_bookkeeping() {
        let mut state = DaemonControlState::default();
        let old = state
            .pending_runtime
            .start_attach(owner("a"), "s".into(), "sub".into());
        let candidate = candidate_for_test("a", "s", "sub", Some(old.clone()), None);
        // Cleanup cancelled the unbound old stream; a replacement attached.
        assert!(state.pending_runtime.cancel_stream_if("s", "sub", &old));
        let replacement = state.pending_runtime.start_attach(
            AttachStreamOwner {
                client_id: "b".to_string(),
                grant_id: Some("grant-b".to_string()),
            },
            "s".into(),
            "sub".into(),
        );
        record_attached_subscription_change(
            &mut state.pending_runtime,
            &mut state.attach_close,
            &mut state.lifecycle_counters,
            Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                session_id: "s".into(),
                subscription_id: "sub".into(),
            })),
            Some("grant-b"),
        );
        let mut applied = CleanupApplied::default();
        apply_cleanup_reports(
            &mut state,
            vec![CleanupRouteReport {
                candidate,
                outcome: CleanupRouteOutcome::NoGeneration,
            }],
            &mut applied,
        );
        assert!(
            state
                .pending_runtime
                .stream_matches("s", "sub", &replacement)
        );
        assert!(
            state
                .pending_runtime
                .live_attach_routes
                .contains(&("s".to_string(), "sub".to_string())),
            "the replacement's live attach record survives"
        );
        assert_eq!(
            state
                .pending_runtime
                .attach_owner_grant_ids
                .get(&("s".to_string(), "sub".to_string()))
                .map(String::as_str),
            Some("grant-b")
        );
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 1);
        assert!(!applied.failed);
    }

    /// H3: a Detached report for the old owner after a replacement bound the
    /// same key closes nothing.
    #[test]
    fn detached_report_after_replacement_keeps_replacement_adapter() {
        let mut state = DaemonControlState::default();
        let old = state
            .pending_runtime
            .start_attach(owner("a"), "s".into(), "sub".into());
        let (_, old_handle) = UnixTerminalAdapter::pair();
        assert!(state.pending_runtime.mark_adapter_bound_if(
            "s",
            "sub",
            &old,
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(old_handle),
        ));
        let candidate =
            candidate_for_departing_owner(&state.pending_runtime, &owner("a"), "a", "s", "sub")
                .expect("owned candidate");
        assert_eq!(
            candidate.generation,
            Some(TerminalSubscriptionGeneration(1))
        );
        let replacement = state
            .pending_runtime
            .start_attach(owner("a"), "s".into(), "sub".into());
        let (_, new_handle) = UnixTerminalAdapter::pair();
        assert!(state.pending_runtime.mark_adapter_bound_if(
            "s",
            "sub",
            &replacement,
            TerminalSubscriptionGeneration(2),
            BoundAdapterHandle::Unix(new_handle.clone()),
        ));
        let mut applied = CleanupApplied::default();
        apply_cleanup_reports(
            &mut state,
            vec![CleanupRouteReport {
                candidate,
                outcome: CleanupRouteOutcome::Detached,
            }],
            &mut applied,
        );
        assert!(state.pending_runtime.is_adapter_bound("s", "sub"));
        assert!(!new_handle.is_closed());
        assert_eq!(applied.bound_closes, 0);
    }

    /// The owned stream is closed and cancelled exactly once, and a route
    /// with no stream at all still records its detach.
    #[test]
    fn owned_and_streamless_reports_apply_bookkeeping() {
        let mut state = DaemonControlState::default();
        let identity = state
            .pending_runtime
            .start_attach(owner("a"), "s".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        assert!(state.pending_runtime.mark_adapter_bound_if(
            "s",
            "sub",
            &identity,
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(handle.clone()),
        ));
        for key in ["sub", "gone"] {
            record_attached_subscription_change(
                &mut state.pending_runtime,
                &mut state.attach_close,
                &mut state.lifecycle_counters,
                Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                    session_id: "s".into(),
                    subscription_id: key.into(),
                })),
                None,
            );
        }
        let owned =
            candidate_for_departing_owner(&state.pending_runtime, &owner("a"), "a", "s", "sub")
                .expect("owned candidate");
        let streamless =
            candidate_for_departing_owner(&state.pending_runtime, &owner("a"), "a", "s", "gone")
                .expect("streamless candidate");
        assert!(streamless.identity.is_none());
        let mut applied = CleanupApplied::default();
        apply_cleanup_reports(
            &mut state,
            vec![
                CleanupRouteReport {
                    candidate: owned,
                    outcome: CleanupRouteOutcome::Detached,
                },
                CleanupRouteReport {
                    candidate: streamless,
                    outcome: CleanupRouteOutcome::NoGeneration,
                },
            ],
            &mut applied,
        );
        assert!(handle.is_closed());
        assert!(state.pending_runtime.stream_identity("s", "sub").is_none());
        assert!(state.pending_runtime.live_attach_routes.is_empty());
        assert_eq!(state.lifecycle_counters.live_attach_subscriptions, 0);
        assert_eq!(applied.bound_closes, 1);
    }

    /// H3: client B replaced client A's route before A's cleanup arrived.
    /// A's candidate keeps A as the Core owner and adopts nothing from B.
    #[test]
    fn departing_owner_candidate_never_targets_a_replacement() {
        let mut registry = crate::subscription::attach_routes::AttachStreamRegistry::default();
        registry.start_attach(owner("a"), "s".into(), "sub".into());
        let b = registry.start_attach(owner("b"), "s".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        assert!(registry.mark_adapter_bound_if(
            "s",
            "sub",
            &b,
            TerminalSubscriptionGeneration(9),
            BoundAdapterHandle::Unix(handle),
        ));
        let candidate = candidate_for_departing_owner(&registry, &owner("a"), "a", "s", "sub")
            .expect("unix departing candidate");
        assert_eq!(candidate.core_client_id, "a");
        assert_eq!(candidate.generation, None);
        assert_eq!(candidate.identity, None);
        // A WebRTC departing peer shares its Core client id with the
        // replacement, so the candidate is dropped instead.
        let peer = AttachStreamOwner {
            client_id: "botster-hub-daemon-subscription-sub".into(),
            grant_id: Some("grant-old".into()),
        };
        registry.start_attach(
            AttachStreamOwner {
                client_id: "botster-hub-daemon-subscription-sub".into(),
                grant_id: Some("grant-new".into()),
            },
            "s".into(),
            "sub".into(),
        );
        assert!(
            candidate_for_departing_owner(
                &registry,
                &peer,
                "botster-hub-daemon-subscription-sub",
                "s",
                "sub"
            )
            .is_none()
        );
    }

    /// Reserved (pending) keys count with live and acknowledged keys, so
    /// concurrent attaches cannot exceed the cap; a failed attach releases
    /// its key only when no stream of the owner still holds it.
    #[test]
    fn attach_reservation_is_a_bounded_union_per_owner() {
        let mut registry = crate::subscription::attach_routes::AttachStreamRegistry::default();
        let unix = owner("a");
        for index in 0..MAX_ATTACH_ROUTES_PER_OWNER {
            assert!(reserve_attach_route(
                &mut registry,
                &unix,
                &format!("s{index}"),
                "sub"
            ));
        }
        assert_eq!(
            registry.stream_count_for_owner(&unix),
            0,
            "pending keys alone fill the cap"
        );
        assert!(!reserve_attach_route(
            &mut registry,
            &unix,
            "overflow",
            "sub"
        ));
        assert!(reserve_attach_route(
            &mut registry,
            &owner("b"),
            "overflow",
            "sub"
        ));
        // Re-attaching a held key is not a new reservation.
        assert!(reserve_attach_route(&mut registry, &unix, "s0", "sub"));
        // A failed attach releases the key unless a live stream holds it.
        registry.start_attach(unix.clone(), "s0".into(), "sub".into());
        release_failed_attach_route(&mut registry, &unix, "s0", "sub");
        assert!(!reserve_attach_route(
            &mut registry,
            &unix,
            "overflow",
            "sub"
        ));
        registry.cancel_stream("s0", "sub");
        release_failed_attach_route(&mut registry, &unix, "s0", "sub");
        assert!(reserve_attach_route(
            &mut registry,
            &unix,
            "overflow",
            "sub"
        ));
        assert_eq!(
            registry.take_owner_routes("a").len(),
            MAX_ATTACH_ROUTES_PER_OWNER
        );
    }
}
