//! Single owner for Hub close-event bookkeeping.
//!
//! Unix and WebRTC muxes keep transport identity and wake calls. They delegate
//! pending `TerminalSubscriptionClosed` events, suppression keys, and bounded
//! slice classification to this ledger.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Mutex;

use botster_core::SessionId;
use botster_core_daemon::{CoreDaemonError, RegistrySessionState, SessionRegistryStateLookup};
use botster_hub_client::{
    DaemonEvent, TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER,
    TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER,
};

use crate::HubDaemon;
use crate::admission::unix_hello::{UnixTerminalAdmission, WebrtcTerminalAdmission};
use crate::daemon_maintenance::{
    PUMP_MAX_ADMISSIONS_VISITED, PUMP_MAX_CANDIDATE_CLASSIFICATIONS,
    PUMP_MAX_ROUTE_ENTRIES_VISITED, PumpAdmissionCursor,
};
use crate::daemon_transport::DaemonControlState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClosedEventSliceProgress {
    pub classified: usize,
    pub more: bool,
    pub after_route: Option<(String, String, u64)>,
}

pub(crate) trait ClosedHandle {
    fn is_closed(&self) -> bool;
    fn host_closed(&self) -> bool;
}

pub(crate) struct ClosedEventRoute<H> {
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
    pub handle: H,
    pub reported: bool,
}

#[derive(Default)]
pub(crate) struct ClosedEventLedger {
    pending_events: Mutex<Vec<DaemonEvent>>,
    suppress_generations: Mutex<BTreeSet<(String, String, u64)>>,
}

impl ClosedEventLedger {
    pub(crate) fn suppress_session_keys(&self, keys: Vec<(String, String, u64)>) {
        if keys.is_empty() {
            return;
        }
        if let Ok(mut generations) = self.suppress_generations.lock() {
            generations.extend(keys);
        }
    }

    pub(crate) fn suppress_generation(
        &self,
        session_id: impl Into<String>,
        subscription_id: impl Into<String>,
        generation: u64,
    ) {
        if let Ok(mut generations) = self.suppress_generations.lock() {
            generations.insert((session_id.into(), subscription_id.into(), generation));
        }
    }

    fn generation_is_suppressed(
        &self,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> bool {
        self.suppress_generations.lock().is_ok_and(|generations| {
            generations.contains(&(
                session_id.to_string(),
                subscription_id.to_string(),
                generation,
            ))
        })
    }

    pub(crate) fn has_pending_event(&self) -> bool {
        self.pending_events
            .lock()
            .ok()
            .is_some_and(|pending| !pending.is_empty())
    }

    pub(crate) fn pop_pending_event(&self) -> Option<DaemonEvent> {
        self.pending_events.lock().ok().and_then(|mut pending| {
            if pending.is_empty() {
                None
            } else {
                Some(pending.remove(0))
            }
        })
    }

    pub(crate) fn drop_pending_events(&self) {
        if let Ok(mut pending) = self.pending_events.lock() {
            pending.clear();
        }
    }

    pub(crate) fn queue_closed_subscription_events_bounded<H: ClosedHandle>(
        &self,
        dying: bool,
        routes: &mut BTreeMap<(String, String, u64), ClosedEventRoute<H>>,
        mut classify: impl FnMut(&str) -> Option<bool>,
        max_candidates: usize,
        after_route: Option<&(String, String, u64)>,
        max_entries_visited: usize,
        wake: impl FnOnce(),
    ) -> ClosedEventSliceProgress {
        if dying {
            for route in routes.values_mut() {
                route.reported = true;
            }
            return empty_close_event_progress();
        }
        let mut queued = Vec::new();
        let mut classified = 0;
        let mut visited = 0;
        let mut more = false;
        let mut last_visited = after_route.cloned();
        let start = match after_route {
            Some(after) => Bound::Excluded(after.clone()),
            None => Bound::Unbounded,
        };
        for (key, route) in routes.range_mut((start, Bound::Unbounded)) {
            if visited >= max_entries_visited {
                more = true;
                break;
            }
            if !route.reported && route.handle.is_closed() && classified >= max_candidates {
                more = true;
                break;
            }
            visited += 1;
            last_visited = Some(key.clone());
            if route.reported || !route.handle.is_closed() {
                continue;
            }
            classified += 1;
            if self.generation_is_suppressed(
                &route.session_id,
                &route.subscription_id,
                route.generation,
            ) {
                route.reported = true;
                continue;
            }
            match classify(&route.session_id) {
                None => continue,
                Some(false) => {
                    route.reported = true;
                    continue;
                }
                Some(true) => route.reported = true,
            }
            let reason = if route.handle.host_closed() {
                TERMINAL_SUBSCRIPTION_CLOSED_HOST_ADAPTER
            } else {
                TERMINAL_SUBSCRIPTION_CLOSED_CORE_ADAPTER
            };
            queued.push(DaemonEvent::TerminalSubscriptionClosed {
                session_id: route.session_id.clone(),
                subscription_id: route.subscription_id.clone(),
                generation: route.generation,
                reason: reason.to_string(),
            });
        }
        if !queued.is_empty() {
            if let Ok(mut pending) = self.pending_events.lock() {
                pending.extend(queued);
            }
            wake();
        }
        ClosedEventSliceProgress {
            classified,
            more,
            after_route: last_visited,
        }
    }
}

pub(crate) fn empty_close_event_progress() -> ClosedEventSliceProgress {
    ClosedEventSliceProgress {
        classified: 0,
        more: false,
        after_route: None,
    }
}

pub(crate) fn suppress_unix_session_close_events(
    pending_runtime: &crate::daemon_transport::PendingRuntimeState,
    session_id: &str,
) {
    for admission in pending_runtime.admission.unix_admissions.values() {
        if let UnixTerminalAdmission::Admitted { mux, .. } = admission {
            mux.suppress_session_route_generations(session_id);
        }
    }
}

pub(crate) fn suppress_webrtc_session_close_events(
    pending_runtime: &crate::daemon_transport::PendingRuntimeState,
    session_id: &str,
) {
    for admission in pending_runtime.admission.webrtc_admissions.values() {
        if let WebrtcTerminalAdmission::Admitted { mux, .. } = admission {
            mux.suppress_session_route_generations(session_id);
        }
    }
}

pub(crate) fn session_close_event_decision_for(
    runtime: &crate::HubRuntime,
    session_id: &str,
) -> Option<bool> {
    session_close_event_decision(runtime.session_registry_state(&SessionId(session_id.to_string())))
}

pub(crate) fn session_close_event_decision(
    lookup: Result<SessionRegistryStateLookup, CoreDaemonError>,
) -> Option<bool> {
    match lookup {
        Ok(SessionRegistryStateLookup::Found(RegistrySessionState::Running)) => Some(true),
        Ok(SessionRegistryStateLookup::Found(_)) => Some(false),
        Ok(SessionRegistryStateLookup::Absent) | Ok(_) | Err(_) => None,
    }
}

pub(crate) fn run_close_events_phase(daemon: &HubDaemon, state: &mut DaemonControlState) -> bool {
    let Some(runtime) = daemon.runtime() else {
        state.pump.close_cursor = PumpAdmissionCursor::default();
        return false;
    };
    let mut admissions_visited = 0;
    let mut classified = 0;
    loop {
        if admissions_visited >= PUMP_MAX_ADMISSIONS_VISITED
            || classified >= PUMP_MAX_CANDIDATE_CLASSIFICATIONS
        {
            return true;
        }
        let remaining_candidates = PUMP_MAX_CANDIDATE_CLASSIFICATIONS.saturating_sub(classified);
        match state.pump.close_cursor.clone() {
            PumpAdmissionCursor::Unix { after, after_route } => {
                let next_key = crate::admission::unix_hello::next_admission_key(
                    &state.pending_runtime.admission.unix_admissions,
                    after.as_deref(),
                );
                let Some(key) = next_key else {
                    state.pump.close_cursor = PumpAdmissionCursor::Webrtc {
                        after: None,
                        after_route: None,
                    };
                    continue;
                };
                admissions_visited += 1;
                let progress = match state.pending_runtime.admission.unix_admissions.get(&key) {
                    Some(UnixTerminalAdmission::Admitted { mux, .. }) => mux
                        .queue_closed_subscription_events_bounded(
                            |session_id| session_close_event_decision_for(runtime, session_id),
                            remaining_candidates,
                            after_route.as_ref(),
                            PUMP_MAX_ROUTE_ENTRIES_VISITED,
                        ),
                    _ => empty_close_event_progress(),
                };
                classified = classified.saturating_add(progress.classified);
                if progress.more {
                    state.pump.close_cursor = PumpAdmissionCursor::Unix {
                        after,
                        after_route: progress.after_route,
                    };
                    return true;
                }
                state.pump.close_cursor = PumpAdmissionCursor::Unix {
                    after: Some(key),
                    after_route: None,
                };
            }
            PumpAdmissionCursor::Webrtc { after, after_route } => {
                let next_key = crate::admission::unix_hello::next_admission_key(
                    &state.pending_runtime.admission.webrtc_admissions,
                    after.as_deref(),
                );
                let Some(key) = next_key else {
                    state.pump.close_cursor = PumpAdmissionCursor::default();
                    return false;
                };
                admissions_visited += 1;
                let progress = match state.pending_runtime.admission.webrtc_admissions.get(&key) {
                    Some(WebrtcTerminalAdmission::Admitted { mux, .. }) => mux
                        .queue_closed_subscription_events_bounded(
                            |session_id| session_close_event_decision_for(runtime, session_id),
                            remaining_candidates,
                            after_route.as_ref(),
                            PUMP_MAX_ROUTE_ENTRIES_VISITED,
                        ),
                    _ => empty_close_event_progress(),
                };
                classified = classified.saturating_add(progress.classified);
                if progress.more {
                    state.pump.close_cursor = PumpAdmissionCursor::Webrtc {
                        after,
                        after_route: progress.after_route,
                    };
                    return true;
                }
                state.pump.close_cursor = PumpAdmissionCursor::Webrtc {
                    after: Some(key),
                    after_route: None,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core_daemon::{CoreDaemonError, RegistrySessionState, SessionRegistryStateLookup};

    #[test]
    fn close_event_suppression_matrix_matches_prior_predicate() {
        assert_eq!(
            session_close_event_decision(Ok(SessionRegistryStateLookup::Found(
                RegistrySessionState::Running
            ))),
            Some(true)
        );
        assert_eq!(
            session_close_event_decision(Ok(SessionRegistryStateLookup::Found(
                RegistrySessionState::Exited
            ))),
            Some(false)
        );
        assert_eq!(
            session_close_event_decision(Ok(SessionRegistryStateLookup::Found(
                RegistrySessionState::Stopping
            ))),
            Some(false)
        );
        assert_eq!(
            session_close_event_decision(Ok(SessionRegistryStateLookup::Found(
                RegistrySessionState::Stale
            ))),
            Some(false)
        );
        assert_eq!(
            session_close_event_decision(Ok(SessionRegistryStateLookup::Absent)),
            None
        );
        assert_eq!(
            session_close_event_decision(Err(CoreDaemonError::Shutdown)),
            None
        );
    }

    #[test]
    fn close_events_phase_source_does_not_take_journal_wake() {
        const SOURCE: &str = include_str!("closed_events.rs");
        let close = SOURCE
            .split("fn run_close_events_phase")
            .nth(1)
            .expect("close phase");
        let close = close.split("#[cfg(test)]").next().expect("close end");
        assert!(!close.contains("take_journal_advanced_wake"));
        assert!(!close.contains("observe_session_lifecycle"));
        assert!(!close.contains("observe_lifecycle_slice"));
        assert!(
            !close.contains("prefer_close_events"),
            "close work must not rewrite the Pump phase pointer"
        );
        assert!(
            !close.contains("queue_unix_subscription_closed_events"),
            "control must not scan every Unix mux for close events"
        );
        assert!(
            !close.contains("queue_webrtc_subscription_closed_events"),
            "control must not scan every WebRTC mux for close events"
        );
    }

    #[test]
    fn shutdown_session_arm_installs_exact_suppression_before_core_request() {
        const TRANSPORT: &str = include_str!("../daemon_transport.rs");
        let arm = TRANSPORT
            .split("DaemonRequest::ShutdownSession { session_id } => {")
            .nth(1)
            .expect("ShutdownSession arm")
            .split("DaemonRequest::Drain {")
            .next()
            .expect("ShutdownSession arm end");
        let unix_suppress = arm
            .find("suppress_unix_session_close_events")
            .expect("unix suppression");
        let webrtc_suppress = arm
            .find("suppress_webrtc_session_close_events")
            .expect("webrtc suppression");
        let core = arm
            .find("HubClientRequest::Shutdown")
            .expect("Core Shutdown request");
        let stopping = arm
            .find("ShutdownSessionClassification::Stopping")
            .expect("Stopping classification");
        assert!(
            stopping < unix_suppress,
            "Stopping must stay on the suppress fall-through, not a pre-suppress return"
        );
        assert!(
            unix_suppress < core && webrtc_suppress < core,
            "ShutdownSession must install exact-key suppression before the Core request"
        );
        let after_core = &arm[core..];
        assert!(
            !after_core.contains("suppress_unix_session_close_events")
                && !after_core.contains("suppress_webrtc_session_close_events"),
            "ShutdownSession must not reinstall suppression after the Core request"
        );
        const CLOSED: &str = include_str!("closed_events.rs");
        assert!(
            arm.contains("suppress_unix_session_close_events")
                && CLOSED.contains("suppress_session_route_generations"),
            "helpers must snapshot exact route generations, not session-wide keys"
        );
    }
}
