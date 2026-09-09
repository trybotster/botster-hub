//! Retain publication ownership through incremental admission or Host disposal.

use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::DaemonControlState;
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::runtime::{CausalTransitionStatus, PublicationAdvance};
use crate::{HubDaemon, HubRuntime};

struct Pending {
    identity: HostJobIdentity,
    owner_permit: OwnerPermit,
}

struct Recovery {
    _pending: Pending,
    _failure: HostSubmissionFailure,
}

#[derive(Default)]
pub(crate) struct PublicationOwnerState {
    pending: Option<Pending>,
    completion: Option<HostCompletion>,
    admission_permit: Option<HostWorkPermit>,
    recovery: Option<Recovery>,
    faulted: bool,
    waiting_for_causal: bool,
    pub(crate) waiting_for_host: bool,
    pub(crate) waiting_for_owner: bool,
}

impl PublicationOwnerState {
    pub(crate) fn accepts(&self, identity: HostJobIdentity) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.identity == identity)
            && self.completion.is_none()
            && self.admission_permit.is_none()
    }

    pub(crate) fn retain_completion(&mut self, completion: HostCompletion) {
        self.completion = Some(completion);
    }

    pub(crate) fn note_causal_progress(&mut self) {
        self.waiting_for_causal = false;
    }

    pub(crate) fn ready(&self, runtime: &HubRuntime) -> bool {
        if self.faulted
            || self.recovery.is_some()
            || self.waiting_for_host
            || self.waiting_for_owner
            || self.waiting_for_causal
        {
            return false;
        }
        self.admission_permit.is_some()
            || self.completion.is_some()
            || (self.pending.is_none() && runtime.entity_publish_ready())
    }
}

pub(crate) fn drive(daemon: &HubDaemon, state: &mut DaemonControlState) -> bool {
    let Some(runtime) = daemon.runtime() else {
        return false;
    };
    if !state.publication_owner.ready(runtime) {
        return false;
    }
    if state.publication_owner.admission_permit.is_some() {
        match runtime.advance_entity_publish() {
            PublicationAdvance::Again => return true,
            PublicationAdvance::Fault => {
                state.publication_owner.faulted = true;
                return false;
            }
            PublicationAdvance::Complete => {}
        }
        assert!(matches!(
            runtime.finish_entity_publish_retirement(),
            CausalTransitionStatus::Applied
        ));
        drop(state.publication_owner.admission_permit.take());
        let pending = state
            .publication_owner
            .pending
            .take()
            .expect("admission retains its owner permit");
        state.budget.release(pending.owner_permit);
        crate::daemon::control::pending::wake_shutdown_waiter(state);
        return state.publication_owner.ready(runtime);
    }
    if let Some(completion) = state.publication_owner.completion.as_ref() {
        if !matches!(completion.result, HostResult::FamilyCleanupComplete { .. }) {
            state.publication_owner.faulted = true;
            return false;
        }
        runtime.complete_entity_publish_disposal();
        match runtime.finish_entity_publish_retirement() {
            CausalTransitionStatus::Waiting => {
                state.publication_owner.waiting_for_causal = true;
                return false;
            }
            CausalTransitionStatus::Fault => {
                state.publication_owner.faulted = true;
                return false;
            }
            CausalTransitionStatus::Applied => {}
        }
        drop(state.publication_owner.completion.take());
        let pending = state
            .publication_owner
            .pending
            .take()
            .expect("completion retains its dispatch");
        state.budget.release(pending.owner_permit);
        crate::daemon::control::pending::wake_shutdown_waiter(state);
        return state.publication_owner.ready(runtime);
    }
    let Some(owner_permit) = state.budget.reserve() else {
        state.publication_owner.waiting_for_owner = true;
        return false;
    };
    let Some(permit) = runtime.host_executor().try_reserve() else {
        state.budget.release(owner_permit);
        state.publication_owner.waiting_for_host = true;
        return false;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        drop(permit);
        state.budget.release(owner_permit);
        state.publication_owner.faulted = true;
        return false;
    };
    let payload = runtime.begin_entity_publish();
    if payload.is_none() && runtime.entity_publish_retirement_pending() {
        runtime.mark_entity_publish_daemon_owned();
        state.publication_owner.pending = Some(Pending {
            identity: HostJobIdentity {
                waiter_id,
                phase: 1,
            },
            owner_permit,
        });
        state.publication_owner.admission_permit = Some(permit);
        return true;
    }
    let Some(payload) = payload else {
        drop(permit);
        state.budget.release(owner_permit);
        crate::daemon::control::pending::wake_shutdown_waiter(state);
        return state.publication_owner.ready(runtime);
    };
    runtime.mark_entity_publish_daemon_owned();
    let pending = Pending {
        identity: HostJobIdentity {
            waiter_id,
            phase: 1,
        },
        owner_permit,
    };
    match runtime.host_executor().submit(
        pending.identity,
        HostCommand::FamilyCleanup(payload),
        permit,
    ) {
        Ok(()) => state.publication_owner.pending = Some(pending),
        Err(failure) => {
            state.publication_owner.recovery = Some(Recovery {
                _pending: pending,
                _failure: failure,
            })
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package_event_router::{CausalAdmitResult, CausalOp, LeaseIdentity};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn daemon(label: &str) -> (HubDaemon, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "publication-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(root.clone()),
            ..Default::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .unwrap();
        (HubDaemon::start(config).unwrap(), root)
    }

    fn queue(
        runtime: &HubRuntime,
        scope: Option<u64>,
    ) -> std::sync::mpsc::Receiver<
        Result<crate::package_entity_fanout::PackageEntityPublishResult, String>,
    > {
        runtime.entity_publish_bridge().test_queue_publish(
            botster_core::PluginKey("absent".into()),
            serde_json::json!({"type": "entity_patch", "entity_type": "absent.items", "snapshot_seq": 1, "id": "item", "patch": {"nested": [{"body": "x".repeat(16384)}]}}),
            scope,
        )
    }

    fn absorb(daemon: &HubDaemon, state: &mut DaemonControlState) {
        let mut turn = crate::daemon::owner_turn::OwnerTurnBudget::new(Instant::now());
        crate::subscription::entity::absorb_session_type_catalog_completions(
            daemon, state, &mut turn,
        );
        crate::daemon::owner_loop::publish_completion_wakes(daemon, state);
    }

    fn await_completion(daemon: &HubDaemon, state: &mut DaemonControlState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.publication_owner.completion.is_none() {
            absorb(daemon, state);
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }

    #[test]
    fn publication_continuation_moves_one_mutation_per_owner_activation() {
        for mode in [
            "consecutive",
            "snapshot",
            "gap",
            "replacement",
            "exhaustion",
        ] {
            let (mut daemon, daemon_root) = daemon(mode);
            let (runtime, provider_root) =
                crate::runtime::tests::publication_provider_runtime(mode);
            daemon.runtime = Some(runtime);
            let runtime = daemon.runtime().unwrap();
            let mut state = DaemonControlState::default();
            let frame = |seq| {
                serde_json::json!({
                    "type": "entity_upsert", "entity_type": "producer.item",
                    "snapshot_seq": seq, "id": "item", "entity": {"id": "item"}
                })
            };
            for seq in if mode == "gap" {
                vec![3, 5]
            } else {
                vec![2, 3, 4]
            } {
                runtime
                    .test_admit_publish("producer", frame(seq), None)
                    .unwrap();
            }
            if mode == "exhaustion" {
                runtime.test_fanout_sequence(Some(u64::MAX - 1));
            }
            let response = runtime.entity_publish_bridge().test_queue_publish(
                botster_core::PluginKey("producer".into()),
                frame(1),
                None,
            );
            let later = runtime.entity_publish_bridge().test_queue_publish(
                botster_core::PluginKey("producer".into()),
                frame(2),
                None,
            );
            assert!(drive(&daemon, &mut state));
            assert_eq!(runtime.test_family_seq("producer.item"), 1);
            assert!(response.try_recv().is_err());
            assert_eq!(state.budget.outstanding(), 1);
            assert!(
                !state
                    .publication_owner
                    .accepts(state.publication_owner.pending.as_ref().unwrap().identity)
            );
            assert!(
                runtime
                    .package_entity_resync_next_attempt("producer.item")
                    .is_none()
            );
            // Synchronous pumping cannot advance the daemon's retained publication.
            runtime.step_entity_publish();
            assert_eq!(runtime.test_family_seq("producer.item"), 1);
            assert_eq!(
                runtime.test_fanout_sequence(None),
                if mode == "exhaustion" { u64::MAX } else { 1 }
            );
            if mode == "exhaustion" {
                assert!(!drive(&daemon, &mut state));
                assert!(state.publication_owner.faulted);
                assert_eq!(runtime.test_family_seq("producer.item"), 1);
                assert!(runtime.entity_publish_retirement_pending());
                assert_eq!(state.budget.outstanding(), 1);
                assert!(state.publication_owner.admission_permit.is_some());
                assert!(response.try_recv().is_err());
                assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 1);
            } else if mode == "replacement" {
                let generation = runtime.package_entity_family_generation("producer.item");
                runtime
                    .drop_package_entity_families_for("producer")
                    .unwrap();
                runtime.begin_package_entity_provider_snapshot("producer.item", 40);
                assert_ne!(
                    runtime.package_entity_family_generation("producer.item"),
                    generation
                );
                assert!(drive(&daemon, &mut state));
                let result = response.try_recv().unwrap().unwrap();
                assert_eq!(result.last_accepted_seq, 1);
                assert_eq!(runtime.test_family_seq("producer.item"), 40);
                assert_eq!(runtime.test_fanout_sequence(None), 1);
                assert_eq!(state.budget.outstanding(), 0);
            } else if mode == "snapshot" {
                runtime.begin_package_entity_provider_snapshot("producer.item", 3);
                assert!(drive(&daemon, &mut state));
                assert_eq!(runtime.test_family_seq("producer.item"), 4);
                assert!(response.try_recv().is_err());
            } else if mode == "consecutive" {
                for expected in 2..=4 {
                    assert!(drive(&daemon, &mut state));
                    assert_eq!(runtime.test_family_seq("producer.item"), expected);
                    assert_eq!(runtime.test_fanout_sequence(None), expected);
                    assert!(response.try_recv().is_err());
                    assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 1);
                    assert_eq!(state.budget.outstanding(), 1);
                }
            }
            if mode != "replacement" && mode != "exhaustion" {
                assert!(drive(&daemon, &mut state));
                let result = response.try_recv().unwrap().unwrap();
                assert_eq!(
                    result.status,
                    crate::package_entity_fanout::PackageEntityPublishStatus::Accepted
                );
                assert_eq!(state.budget.outstanding(), 0);
                assert!(state.publication_owner.admission_permit.is_none());
                assert!(later.try_recv().is_err());
                assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 1);
                if mode == "gap" {
                    assert_eq!(runtime.test_family_seq("producer.item"), 1);
                    assert!(
                        runtime
                            .package_entity_resync_next_attempt("producer.item")
                            .is_some()
                    );
                    assert!(drive(&daemon, &mut state));
                    assert!(drive(&daemon, &mut state));
                    assert_eq!(runtime.test_family_seq("producer.item"), 3);
                    assert!(!drive(&daemon, &mut state));
                    assert!(later.try_recv().unwrap().unwrap().resync_needed);
                }
            }
            drop(daemon);
            std::fs::remove_dir_all(daemon_root).unwrap();
            std::fs::remove_dir_all(provider_root).unwrap();
        }
    }

    #[test]
    fn publication_continuation_keeps_pending_lease_after_response_closes() {
        let (mut daemon, daemon_root) = daemon("continuation-leases");
        let (runtime, provider_root) =
            crate::runtime::tests::publication_provider_runtime("continuation-leases");
        daemon.runtime = Some(runtime);
        let runtime = daemon.runtime().unwrap();
        let mut state = DaemonControlState::default();
        let frame = |seq| serde_json::json!({"type": "entity_remove", "entity_type": "producer.item", "snapshot_seq": seq, "id": "item"});
        let gap_scope = runtime.causal_scopes().mint().unwrap();
        let gap = runtime.entity_publish_bridge().test_queue_publish(
            botster_core::PluginKey("producer".into()),
            frame(2),
            Some(gap_scope),
        );
        assert!(!drive(&daemon, &mut state));
        assert!(gap.try_recv().unwrap().unwrap().resync_needed);
        runtime.apply_causal_owner_ops();
        let generation = runtime
            .package_entity_family_generation("producer.item")
            .unwrap();
        let admitted = LeaseIdentity::AdmittedEntityMutation {
            family: "producer.item".into(),
            generation,
            seq: 2,
        };
        assert!(
            runtime
                .causal_scopes()
                .identities(gap_scope)
                .unwrap()
                .contains(&admitted)
        );
        let scope = runtime.causal_scopes().mint().unwrap();
        let response = runtime.entity_publish_bridge().test_queue_publish(
            botster_core::PluginKey("producer".into()),
            frame(1),
            Some(scope),
        );
        assert!(drive(&daemon, &mut state));
        drop(response);
        assert!(drive(&daemon, &mut state));
        assert!(
            runtime
                .causal_scopes()
                .identities(gap_scope)
                .unwrap()
                .contains(&admitted)
        );
        assert_eq!(runtime.test_family_seq("producer.item"), 2);
        assert_eq!(state.budget.outstanding(), 1);
        assert!(!drive(&daemon, &mut state));
        assert_eq!(state.budget.outstanding(), 0);
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(
            runtime
                .causal_scopes()
                .identities(gap_scope)
                .unwrap()
                .contains(&admitted)
        );
        let mutations = runtime.take_package_entity_fanout();
        assert_eq!(mutations.len(), 2);
        while runtime.causal_family_release_ready() {
            runtime.retry_family_resync_release();
        }
        while runtime.causal_operation_count() > 0 {
            runtime.apply_causal_owner_ops();
        }
        assert!(!runtime.causal_scopes().is_live(scope));
        assert!(!runtime.causal_scopes().is_live(gap_scope));
        drop(daemon);
        std::fs::remove_dir_all(daemon_root).unwrap();
        std::fs::remove_dir_all(provider_root).unwrap();
    }

    #[test]
    fn publication_disposal_retains_completion_and_permits_until_causal_retirement() {
        for condition in ["capacity", "fault", "missing"] {
            let (mut daemon, root) = daemon(condition);
            let runtime = daemon.runtime().unwrap();
            let mut state = DaemonControlState::default();
            let scope = if condition == "missing" {
                u64::MAX
            } else {
                runtime.causal_scopes().mint().unwrap()
            };
            let response = queue(runtime, Some(scope));
            let host_permits = (0..crate::host_executor::HOST_OPERATION_CAPACITY - 1)
                .map(|_| runtime.host_executor().try_reserve().unwrap())
                .collect::<Vec<_>>();
            assert!(!drive(&daemon, &mut state));
            assert!(runtime.entity_publish_retirement_pending());
            assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 0);
            assert_eq!(state.budget.outstanding(), 1);
            let identity = state.publication_owner.pending.as_ref().unwrap().identity;
            assert!(!state.publication_owner.accepts(HostJobIdentity {
                phase: identity.phase + 1,
                ..identity
            }));
            for _ in 0..crate::runtime::CAUSAL_OWNER_CAPACITY {
                assert!(matches!(
                    runtime.admit_causal_op(CausalOp::Release {
                        scope_id: u64::MAX,
                        identity: LeaseIdentity::EventInFlight
                    }),
                    CausalAdmitResult::Applied
                ));
            }
            await_completion(&daemon, &mut state);
            let HostResult::FamilyCleanupComplete { worker_thread } =
                &state.publication_owner.completion.as_ref().unwrap().result
            else {
                panic!("worker must confirm disposal")
            };
            assert_ne!(*worker_thread, std::thread::current().id());
            assert!(!drive(&daemon, &mut state));
            if condition == "missing" {
                assert!(!runtime.entity_publish_retirement_pending());
                assert!(response.try_recv().unwrap().is_err());
                assert_eq!(
                    runtime.causal_operation_count(),
                    crate::runtime::CAUSAL_OWNER_CAPACITY
                );
                assert_eq!(state.budget.outstanding(), 0);
            } else {
                assert!(state.publication_owner.waiting_for_causal);
                assert!(state.publication_owner.completion.is_some());
                assert!(runtime.host_executor().try_reserve().is_none());
                assert!(response.try_recv().is_err());
                runtime.step_entity_publish();
                assert!(
                    runtime.entity_publish_retirement_pending(),
                    "synchronous pumping cannot retire Host work"
                );
                if condition == "fault" {
                    drop(response);
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime
                            .causal_scopes()
                            .test_with_inner_held(|| panic!("inject table fault"))
                    }));
                    absorb(&daemon, &mut state);
                    assert!(!drive(&daemon, &mut state));
                    assert!(state.publication_owner.faulted);
                    assert!(state.publication_owner.completion.is_some());
                    assert_eq!(state.budget.outstanding(), 1);
                    assert!(runtime.host_executor().try_reserve().is_none());
                } else {
                    runtime.apply_causal_owner_ops();
                    absorb(&daemon, &mut state);
                    assert!(!drive(&daemon, &mut state));
                    assert!(response.try_recv().unwrap().is_err());
                    assert_eq!(state.budget.outstanding(), 0);
                    assert!(runtime.host_executor().try_reserve().is_some());
                    while runtime.causal_operation_count() > 0 {
                        runtime.apply_causal_owner_ops();
                    }
                    assert!(!runtime.causal_scopes().is_live(scope));
                }
            }
            drop(host_permits);
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn publication_stopped_submission_retains_the_original_command() {
        let (mut daemon, root) = daemon("stopped");
        daemon.runtime_mut().unwrap().test_stop_host_submissions();
        let runtime = daemon.runtime().unwrap();
        let mut state = DaemonControlState::default();
        let scope = runtime.causal_scopes().mint().unwrap();
        let response = queue(runtime, Some(scope));
        assert!(!drive(&daemon, &mut state));
        let recovery = state.publication_owner.recovery.as_ref().unwrap();
        assert_eq!(
            recovery._failure.error,
            crate::host_executor::HostSubmitError::Stopped
        );
        assert_eq!(recovery._failure.identity, recovery._pending.identity);
        let HostCommand::FamilyCleanup(
            crate::package_entity_fanout::PackageEntityMutation::Patch { patch, .. },
        ) = &recovery._failure.command
        else {
            panic!("recovery retains the original mutation")
        };
        assert_eq!(patch["nested"][0]["body"].as_str().unwrap().len(), 16384);
        drop(response);
        runtime.step_entity_publish();
        assert!(runtime.entity_publish_retirement_pending());
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(runtime.causal_scopes().identities(scope).unwrap().len(), 1);
        assert!(!state.publication_owner.ready(runtime));
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_admission_waits_for_host_and_owner_capacity_separately() {
        for capacity in ["host", "owner"] {
            let (mut daemon, root) = daemon(capacity);
            let mut state = DaemonControlState::default();
            state.budget = crate::daemon::owner_budget::OwnerBudget::with_capacity(1);
            let owner_permit = (capacity == "owner").then(|| state.budget.reserve().unwrap());
            let mut host_permits = if capacity == "host" {
                (0..crate::host_executor::HOST_OPERATION_CAPACITY)
                    .map(|_| {
                        daemon
                            .runtime()
                            .unwrap()
                            .host_executor()
                            .try_reserve()
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let response = queue(daemon.runtime().unwrap(), None);
            assert!(!drive(&daemon, &mut state));
            assert_eq!(state.publication_owner.waiting_for_host, capacity == "host");
            assert_eq!(
                state.publication_owner.waiting_for_owner,
                capacity == "owner"
            );
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .entity_publish_bridge()
                    .pending_publish_count(),
                1
            );
            assert!(!state.publication_owner.ready(daemon.runtime().unwrap()));
            if let Some(permit) = owner_permit {
                state.budget.release(permit);
            }
            drop(host_permits.pop());
            absorb(&daemon, &mut state);
            assert!(!drive(&daemon, &mut state));
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .entity_publish_bridge()
                    .pending_publish_count(),
                0
            );
            await_completion(&daemon, &mut state);
            assert!(!drive(&daemon, &mut state));
            assert!(response.try_recv().unwrap().is_err());
            assert_eq!(state.budget.outstanding(), 0);
            drop(host_permits);
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
