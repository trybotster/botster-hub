//! Retain publication ownership through Host phases and causal table application.

use crate::daemon::owner_budget::OwnerPermit;
use crate::daemon::owner_loop::DaemonControlState;
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::runtime::CausalTransitionStatus;
use crate::runtime::entity_model::{Operation, PublicationSelection, Work};
use crate::runtime::publication::Next;
use crate::{HubDaemon, HubRuntime};

struct Pending {
    identity: HostJobIdentity,
    owner_permit: OwnerPermit,
}

struct Recovery {
    _failure: HostSubmissionFailure,
}

#[derive(Default)]
pub(crate) struct PublicationOwnerState {
    terminal: Option<crate::host_disposal::Job>,
    pending: Option<Pending>,
    completion: Option<HostCompletion>,
    permit: Option<HostWorkPermit>,
    recovery: Option<Recovery>,
    work: Option<Work>,
    operation: Option<Operation>,
    selecting: bool,
    faulted: bool,
    waiting_for_progress: bool,
    pub(crate) waiting_for_host: bool,
    pub(crate) waiting_for_owner: bool,
}

impl PublicationOwnerState {
    pub(crate) fn dispose_terminal(
        &mut self,
        runtime: &HubRuntime,
        budget: &mut crate::daemon::owner_budget::OwnerBudget,
    ) -> bool {
        if let Some(job) = self.terminal.as_mut() {
            if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                if let Some(work) = self.work.take() {
                    assert!(runtime.retire_terminal_entity_model(&work));
                }
                if let Some(pending) = self.pending.take() {
                    budget.release(pending.owner_permit);
                }
                drop(permit);
                self.terminal.take();
                return true;
            }
            return false;
        }
        let Some(pending) = self.pending.as_ref() else {
            return true;
        };
        let mut payload: Option<Box<dyn Send>> = None;
        let (identity, permit) = if let Some(permit) = self.permit.take() {
            (pending.identity, permit)
        } else if let Some(completion) = self.completion.take() {
            let (identity, result, permit) = completion.into_parts();
            payload = Some(Box::new(result));
            (identity, permit)
        } else if let Some(recovery) = self.recovery.take() {
            payload = Some(Box::new(recovery._failure.command));
            (recovery._failure.identity, recovery._failure.permit)
        } else {
            return false;
        };
        self.terminal = Some(crate::host_disposal::Job::new(
            crate::host_disposal::Parts {
                identity,
                permit,
                model: self.work.clone(),
                payload: Box::new((self.operation.take(), payload)),
            },
        ));
        false
    }

    pub(crate) fn accepts(&self, identity: HostJobIdentity) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| pending.identity == identity)
            && self.work.is_some()
            && self.permit.is_none()
            && self.completion.is_none()
            && self.recovery.is_none()
    }

    pub(crate) fn retain_completion(&mut self, completion: HostCompletion) {
        self.completion = Some(completion);
    }

    pub(crate) fn note_progress(&mut self) {
        self.waiting_for_progress = false;
    }

    pub(crate) fn ready(&self, runtime: &HubRuntime) -> bool {
        if self.faulted
            || self.recovery.is_some()
            || self.waiting_for_host
            || self.waiting_for_owner
            || self.waiting_for_progress
        {
            return false;
        }
        self.permit.is_some()
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
    if let Some(completion) = state.publication_owner.completion.as_ref() {
        let work = state
            .publication_owner
            .work
            .as_ref()
            .expect("the completion retains its model record")
            .clone();
        let status = match completion.result {
            HostResult::EntityModelComplete(kind) => {
                runtime.observe_entity_model(completion.identity, kind)
            }
            _ => {
                runtime.fault_entity_model(completion.identity);
                CausalTransitionStatus::Fault
            }
        };
        match status {
            CausalTransitionStatus::Waiting => {
                state.publication_owner.waiting_for_progress = true;
                return false;
            }
            CausalTransitionStatus::Fault => {
                state.publication_owner.faulted = true;
                return false;
            }
            CausalTransitionStatus::Applied => {}
        }
        let Some(next_identity) = completion.identity.next_phase() else {
            runtime.fault_entity_model(completion.identity);
            state.publication_owner.faulted = true;
            return false;
        };
        let mut output = runtime
            .entity_model_output(&work)
            .expect("exact observation permits output access");
        if !output.valid {
            runtime.fault_entity_model(completion.identity);
            state.publication_owner.faulted = true;
            return false;
        }
        let next = output.readiness.publication;
        state.publication_owner.operation = match next {
            Next::Idle => None,
            Next::Advance => Some(Operation::AdvancePublication(Default::default())),
            Next::Dispose => Some(Operation::DisposePublication(
                output.take_discarded_publication(),
            )),
            Next::Release => Some(Operation::ReleasePublication),
            Next::Reply => Some(Operation::ReplyPublication(Default::default())),
        };
        drop(output);
        assert!(
            runtime.release_entity_model(&work),
            "the exact table receipt permits model release"
        );
        assert!(
            work.owner_drop_ready(),
            "the Host cleared variable inputs before completion"
        );
        drop(state.publication_owner.work.take());
        let (_, _, permit) = state
            .publication_owner
            .completion
            .take()
            .unwrap()
            .into_parts();
        if next == Next::Idle {
            drop(permit);
            let pending = state
                .publication_owner
                .pending
                .take()
                .expect("the publication retains its owner permit");
            state.budget.release(pending.owner_permit);
            crate::daemon::control::pending::wake_shutdown_waiter(state);
        } else {
            state.publication_owner.pending.as_mut().unwrap().identity = next_identity;
            state.publication_owner.permit = Some(permit);
        }
        return state.publication_owner.ready(runtime);
    }
    if state.publication_owner.pending.is_none() {
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
        state.publication_owner.pending = Some(Pending {
            identity: HostJobIdentity::first(waiter_id),
            owner_permit,
        });
        state.publication_owner.permit = Some(permit);
        state.publication_owner.operation = Some(Operation::AdmitPublication(Default::default()));
        state.publication_owner.selecting = true;
        return true;
    }
    let identity = state.publication_owner.pending.as_ref().unwrap().identity;
    if state.publication_owner.work.is_none() {
        let operation = state
            .publication_owner
            .operation
            .take()
            .expect("the publication retains its next phase");
        let permit = state
            .publication_owner
            .permit
            .as_ref()
            .expect("the publication retains Host capacity");
        match runtime.begin_entity_model(identity, operation, permit) {
            Ok(work) => state.publication_owner.work = Some(work),
            Err((status, operation)) => {
                state.publication_owner.operation = Some(operation);
                match status {
                    CausalTransitionStatus::Waiting => {
                        state.publication_owner.waiting_for_progress = true
                    }
                    _ => state.publication_owner.faulted = true,
                }
                return false;
            }
        }
    }
    let work = state.publication_owner.work.as_ref().unwrap();
    if state.publication_owner.selecting {
        match runtime.select_entity_model_publication(work) {
            PublicationSelection::Selected | PublicationSelection::Empty => {
                state.publication_owner.selecting = false
            }
            PublicationSelection::Waiting => {
                state.publication_owner.waiting_for_progress = true;
                return false;
            }
            PublicationSelection::Fault => {
                runtime.fault_entity_model(identity);
                state.publication_owner.faulted = true;
                return false;
            }
        }
    }
    let command = HostCommand::EntityModel(work.clone());
    let permit = state.publication_owner.permit.take().unwrap();
    if let Err(failure) = runtime.host_executor().submit(identity, command, permit) {
        state.publication_owner.recovery = Some(Recovery { _failure: failure });
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package_event_router::LeaseIdentity;
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
        runtime.entity_publish_bridge().test_queue_stale_publish(
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

    // Run one Host phase and consume its exact completion.
    fn phase(daemon: &HubDaemon, state: &mut DaemonControlState) {
        if state.publication_owner.pending.is_none() {
            assert!(drive(daemon, state));
        }
        assert!(!drive(daemon, state));
        await_completion(daemon, state);
        drive(daemon, state);
        while state.publication_owner.waiting_for_progress {
            daemon.runtime().unwrap().apply_causal_owner_ops();
            absorb(daemon, state);
            drive(daemon, state);
        }
    }

    fn finish(daemon: &HubDaemon, state: &mut DaemonControlState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.publication_owner.pending.is_some() {
            assert!(!state.publication_owner.faulted);
            assert!(Instant::now() < deadline);
            phase(daemon, state);
        }
    }

    #[test]
    fn publication_continuation_moves_one_mutation_per_host_phase() {
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
            phase(&daemon, &mut state);
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
                phase(&daemon, &mut state);
                assert!(state.publication_owner.faulted);
                assert_eq!(runtime.test_family_seq("producer.item"), 1);
                assert!(runtime.entity_publish_retirement_pending());
                assert_eq!(state.budget.outstanding(), 1);
                assert!(state.publication_owner.completion.is_some());
                assert_eq!(runtime.host_executor().outstanding(), 1);
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
                phase(&daemon, &mut state);
                finish(&daemon, &mut state);
                let result = response.try_recv().unwrap().unwrap();
                assert_eq!(result.last_accepted_seq, 1);
                assert_eq!(runtime.test_family_seq("producer.item"), 40);
                assert_eq!(runtime.test_fanout_sequence(None), 1);
                assert_eq!(state.budget.outstanding(), 0);
            } else if mode == "snapshot" {
                runtime.begin_package_entity_provider_snapshot("producer.item", 3);
                phase(&daemon, &mut state);
                assert_eq!(runtime.test_family_seq("producer.item"), 4);
                assert!(response.try_recv().is_err());
            } else if mode == "consecutive" {
                for expected in 2..=4 {
                    phase(&daemon, &mut state);
                    assert_eq!(runtime.test_family_seq("producer.item"), expected);
                    assert_eq!(runtime.test_fanout_sequence(None), expected);
                    assert!(response.try_recv().is_err());
                    assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 1);
                    assert_eq!(state.budget.outstanding(), 1);
                }
            }
            if mode != "replacement" && mode != "exhaustion" {
                phase(&daemon, &mut state);
                finish(&daemon, &mut state);
                let result = response.try_recv().unwrap().unwrap();
                assert_eq!(
                    result.status,
                    crate::package_entity_fanout::PackageEntityPublishStatus::Accepted
                );
                assert_eq!(state.budget.outstanding(), 0);
                assert!(state.publication_owner.permit.is_none());
                assert!(later.try_recv().is_err());
                assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 1);
                if mode == "gap" {
                    assert_eq!(runtime.test_family_seq("producer.item"), 1);
                    assert!(
                        runtime
                            .package_entity_resync_next_attempt("producer.item")
                            .is_some()
                    );
                    phase(&daemon, &mut state);
                    phase(&daemon, &mut state);
                    assert_eq!(runtime.test_family_seq("producer.item"), 3);
                    phase(&daemon, &mut state);
                    finish(&daemon, &mut state);
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
        phase(&daemon, &mut state);
        finish(&daemon, &mut state);
        assert!(gap.try_recv().unwrap().unwrap().resync_needed);
        runtime.apply_causal_owner_ops();
        let admitted = LeaseIdentity::AdmittedEntityMutation {
            family_token: runtime.test_family_causal_token("producer.item"),
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
        phase(&daemon, &mut state);
        drop(response);
        phase(&daemon, &mut state);
        assert!(
            runtime
                .causal_scopes()
                .identities(gap_scope)
                .unwrap()
                .contains(&admitted)
        );
        assert_eq!(runtime.test_family_seq("producer.item"), 2);
        assert_eq!(state.budget.outstanding(), 1);
        phase(&daemon, &mut state);
        finish(&daemon, &mut state);
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
        for condition in ["contention", "fault", "missing"] {
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
            phase(&daemon, &mut state);
            assert!(runtime.entity_publish_retirement_pending());
            assert_eq!(runtime.entity_publish_bridge().pending_publish_count(), 0);
            assert_eq!(state.budget.outstanding(), 1);
            assert!(response.try_recv().is_err());
            phase(&daemon, &mut state);
            assert!(response.try_recv().is_err());
            if condition == "missing" {
                finish(&daemon, &mut state);
                assert!(response.try_recv().unwrap().is_err());
                assert_eq!(runtime.entity_publish_bridge().retained_counts(), (0, 0));
                assert_eq!(state.budget.outstanding(), 0);
            } else {
                // The release reaches the queue, but the table remains locked.
                assert!(!drive(&daemon, &mut state));
                await_completion(&daemon, &mut state);
                runtime.causal_scopes().test_with_inner_held(|| {
                    assert!(!drive(&daemon, &mut state));
                    runtime.apply_causal_owner_ops();
                    assert!(state.publication_owner.waiting_for_progress);
                    assert!(state.publication_owner.completion.is_some());
                    assert_eq!(state.budget.outstanding(), 1);
                    assert!(runtime.host_executor().try_reserve().is_none());
                    assert!(response.try_recv().is_err());
                });
                runtime.step_entity_publish();
                assert!(runtime.entity_publish_retirement_pending());
                if condition == "fault" {
                    drop(response);
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime
                            .causal_scopes()
                            .test_with_inner_held(|| panic!("inject table fault"))
                    }));
                    runtime.apply_causal_owner_ops();
                    absorb(&daemon, &mut state);
                    assert!(!drive(&daemon, &mut state));
                    assert!(state.publication_owner.faulted);
                    assert!(state.publication_owner.completion.is_some());
                    assert_eq!(state.budget.outstanding(), 1);
                    assert!(runtime.host_executor().try_reserve().is_none());
                } else {
                    runtime.apply_causal_owner_ops();
                    absorb(&daemon, &mut state);
                    assert!(drive(&daemon, &mut state));
                    assert!(!runtime.causal_scopes().is_live(scope));
                    assert!(response.try_recv().is_err());
                    finish(&daemon, &mut state);
                    assert!(response.try_recv().unwrap().is_err());
                    assert_eq!(state.budget.outstanding(), 0);
                    assert!(runtime.host_executor().try_reserve().is_some());
                    assert_eq!(runtime.entity_publish_bridge().retained_counts(), (0, 0));
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
        let runtime = daemon.runtime().unwrap();
        let mut state = DaemonControlState::default();
        let scope = runtime.causal_scopes().mint().unwrap();
        let response = queue(runtime, Some(scope));
        assert!(drive(&daemon, &mut state));
        daemon.runtime_mut().unwrap().test_stop_host_submissions();
        let runtime = daemon.runtime().unwrap();
        assert!(!drive(&daemon, &mut state));
        let recovery = state.publication_owner.recovery.as_ref().unwrap();
        assert_eq!(
            recovery._failure.error,
            crate::host_executor::HostSubmitError::Stopped
        );
        assert_eq!(
            recovery._failure.identity,
            state.publication_owner.pending.as_ref().unwrap().identity
        );
        let HostCommand::EntityModel(work) = &recovery._failure.command else {
            panic!("recovery retains the original mutation")
        };
        work.test_pending_publication(|pending| {
            let crate::package_entity_fanout::PackageEntityMutation::Patch { patch, .. } =
                &pending.mutation
            else {
                panic!("the original mutation is a patch");
            };
            assert_eq!(patch["nested"][0]["body"].as_str().unwrap().len(), 16384);
            assert_eq!(pending.scope_id, Some(scope));
        });
        assert_eq!(runtime.entity_publish_bridge().retained_counts().0, 1);
        drop(response);
        runtime.step_entity_publish();
        assert!(!runtime.entity_model_available());
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(runtime.causal_scopes().identities(scope).unwrap().len(), 1);
        assert!(!state.publication_owner.ready(runtime));
        daemon.stop();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_bridge_fault_after_model_reservation_retains_the_request_and_permits() {
        let (mut daemon, root) = daemon("source-fault");
        let runtime = daemon.runtime().unwrap();
        let mut state = DaemonControlState::default();
        let scope = runtime.causal_scopes().mint().unwrap();
        let response = queue(runtime, Some(scope));
        let bridge = runtime.entity_publish_bridge();
        let charge = bridge.retained_counts();
        assert!(drive(&daemon, &mut state));
        let identity = state.publication_owner.pending.as_ref().unwrap().identity;
        state.publication_owner.work = Some(
            runtime
                .begin_entity_model(
                    identity,
                    state.publication_owner.operation.take().unwrap(),
                    state.publication_owner.permit.as_ref().unwrap(),
                )
                .unwrap_or_else(|_| panic!("the publication reserved model capacity")),
        );
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bridge.take_if::<()>(|_| panic!("inject a source queue fault"));
        }));
        assert!(bridge.is_faulted());
        absorb(&daemon, &mut state);
        assert!(!drive(&daemon, &mut state));
        assert!(state.publication_owner.faulted);
        assert!(!state.publication_owner.waiting_for_progress);
        assert!(state.publication_owner.work.is_some());
        assert!(state.publication_owner.permit.is_some());
        assert_eq!(state.budget.outstanding(), 1);
        assert_eq!(runtime.host_executor().outstanding(), 1);
        assert!(!runtime.entity_model_available());
        assert_eq!(bridge.pending_publish_count(), 1);
        assert_eq!(bridge.retained_counts(), charge);
        assert!(
            runtime
                .causal_scopes()
                .identities(scope)
                .unwrap()
                .is_empty()
        );
        assert!(response.try_recv().is_err());
        drop(response);
        assert!(!bridge.test_retract(1));
        assert_eq!(bridge.retained_counts(), charge);
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
            phase(&daemon, &mut state);
            assert_eq!(
                daemon
                    .runtime()
                    .unwrap()
                    .entity_publish_bridge()
                    .pending_publish_count(),
                0
            );
            finish(&daemon, &mut state);
            assert!(response.try_recv().unwrap().is_err());
            assert_eq!(state.budget.outstanding(), 0);
            drop(host_permits);
            daemon.stop();
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
