//! Incremental family inspection through retained Host model phases.

use std::sync::Arc;
use std::time::Instant;

use crate::HubDaemon;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::DeadlineKey;
use crate::daemon_maintenance::MaintenanceSliceKind;
use crate::host_executor::{
    HostCommand, HostCompletion, HostJobIdentity, HostResult, HostSubmissionFailure, HostWorkPermit,
};
use crate::plugin_entity::Target;
use crate::runtime::CausalTransitionStatus;
use crate::runtime::entity_model::{Operation, Work};
use crate::runtime::resync::{Action, Cursor};

#[derive(Default)]
enum Stage {
    #[default]
    NextFamily,
    CheckFamily,
    Target,
    Attempt,
    Clear,
}

/// One scan owns family queries and indexed releases through the existing scheduler.
#[derive(Default)]
pub(crate) struct PackageEntityResyncScan {
    terminal: Option<crate::host_disposal::Job>,
    terminal_admission: Option<crate::lua_runtime::EntityPublishPermit>,
    cursor: Option<Cursor>,
    target_after: Option<Arc<Target>>,
    subscription_id: Option<String>,
    stage: Stage,
    earliest: Option<Instant>,
    running: bool,
    scanning: bool,
    changed: bool,
    identity: Option<HostJobIdentity>,
    permit: Option<HostWorkPermit>,
    operation: Option<Operation>,
    work: Option<Work>,
    action: Option<Action>,
    completion: Option<HostCompletion>,
    failure: Option<HostSubmissionFailure>,
    faulted: bool,
    #[cfg(test)]
    fail_release: bool,
    waiting_for_progress: bool,
    pub(crate) waiting_for_host: bool,
    pub(crate) deadline_key: Option<DeadlineKey>,
}

enum ScanStep {
    Idle,
    Again,
    Complete(Option<Instant>),
}

impl PackageEntityResyncScan {
    pub(crate) fn dispose_terminal(&mut self, runtime: &crate::HubRuntime) -> bool {
        if let Some(job) = self.terminal.as_mut() {
            if let crate::host_disposal::Poll::Disposed(permit) = job.poll() {
                if let Some(work) = self.work.take() {
                    assert!(runtime.retire_terminal_entity_model(&work));
                }
                self.terminal_admission.take();
                drop(permit);
                self.terminal.take();
                self.identity = None;
                return true;
            }
            return false;
        }
        let mut payload: Option<Box<dyn Send>> = None;
        let (identity, permit) = if let Some(permit) = self.permit.take() {
            (
                self.identity.expect("resync retains its Host identity"),
                permit,
            )
        } else if let Some(completion) = self.completion.take() {
            let (identity, result, permit) = completion.into_parts();
            payload = Some(Box::new(result));
            (identity, permit)
        } else if let Some(failure) = self.failure.take() {
            payload = Some(Box::new(failure.command));
            (failure.identity, failure.permit)
        } else {
            return self.identity.is_none() && self.work.is_none();
        };
        self.terminal_admission = self.cursor.as_ref().and_then(Cursor::terminal_admission);
        self.terminal = Some(crate::host_disposal::Job::new(
            crate::host_disposal::Parts {
                storage: None,
                identity,
                permit,
                model: self.work.clone(),
                payload: Box::new((
                    self.cursor.take(),
                    self.target_after.take(),
                    self.subscription_id.take(),
                    self.operation.take(),
                    self.action.take(),
                    payload,
                )),
            },
        ));
        false
    }

    #[cfg(test)]
    pub(crate) fn test_running(&self) -> bool {
        self.running
    }

    #[cfg(test)]
    fn test_after(&self) -> Option<String> {
        self.cursor
            .as_ref()
            .and_then(|cursor| cursor.after.clone())
            .or_else(|| self.work.as_ref().and_then(Work::test_resync_after))
    }

    pub(crate) fn accepts(&self, identity: HostJobIdentity) -> bool {
        self.identity == Some(identity)
            && self.work.is_some()
            && self.permit.is_none()
            && self.completion.is_none()
            && self.failure.is_none()
    }

    pub(crate) fn retain_completion(&mut self, completion: HostCompletion) {
        self.completion = Some(completion);
    }

    pub(crate) fn note_progress(&mut self) {
        self.waiting_for_progress = false;
    }

    pub(crate) fn ready(&self, runtime: &crate::HubRuntime) -> bool {
        !self.faulted
            && self.failure.is_none()
            && !self.waiting_for_progress
            && !self.waiting_for_host
            && (self.completion.is_some()
                || (self.work.is_none()
                    && (self.running || self.changed || runtime.entity_model_readiness().releases)))
    }

    fn remember_deadline(&mut self, deadline: Instant) {
        self.earliest = Some(self.earliest.map_or(deadline, |old| old.min(deadline)));
    }

    fn inspect_family(&mut self, state: &DaemonControlState) {
        let cursor = self.cursor.as_ref().expect("the scan retains its cursor");
        let family = cursor
            .after
            .as_deref()
            .expect("the scan retains its family");
        if !state.plugin_entities.has_resync(family)
            && let Some(deadline) = cursor.observation.deadline
        {
            if deadline <= Instant::now() {
                self.stage = Stage::Target;
                return;
            }
            self.remember_deadline(deadline);
        }
        self.target_after = None;
        self.stage = Stage::NextFamily;
    }

    fn step(&mut self, daemon: &HubDaemon, state: &mut DaemonControlState) -> ScanStep {
        let Some(runtime) = daemon.runtime() else {
            return ScanStep::Idle;
        };
        if !self.ready(runtime) {
            return ScanStep::Idle;
        }
        if let Some(completion) = self.completion.as_ref() {
            let identity = self.identity.expect("the scan retains its identity");
            let status = if completion.identity != identity {
                CausalTransitionStatus::Fault
            } else if let HostResult::EntityModelComplete(kind) = completion.result {
                runtime.observe_entity_model(identity, kind)
            } else {
                CausalTransitionStatus::Fault
            };
            match status {
                CausalTransitionStatus::Waiting => {
                    self.waiting_for_progress = true;
                    return ScanStep::Idle;
                }
                CausalTransitionStatus::Fault => {
                    runtime.fault_entity_model(identity);
                    self.faulted = true;
                    return ScanStep::Idle;
                }
                CausalTransitionStatus::Applied => {}
            }
            let Some(next_identity) = identity.next_phase() else {
                runtime.fault_entity_model(identity);
                self.faulted = true;
                return ScanStep::Idle;
            };
            let work = self.work.as_ref().unwrap();
            let mut output = runtime
                .entity_model_output(work)
                .expect("the exact receipt permits output access");
            self.cursor = output.take_resync_cursor();
            drop(output);
            assert!(runtime.release_entity_model(work));
            assert!(work.owner_drop_ready());
            drop(self.work.take());
            let (_, _, permit) = self.completion.take().unwrap().into_parts();
            self.permit = Some(permit);
            self.identity = Some(next_identity);
            match self.action.take().unwrap() {
                Action::Release => {}
                Action::NextFamily => {
                    if self
                        .cursor
                        .as_ref()
                        .unwrap()
                        .observation
                        .generation
                        .is_none()
                    {
                        self.stage = Stage::Clear;
                    } else {
                        self.target_after = None;
                        self.inspect_family(state);
                    }
                }
                Action::CheckFamily => self.inspect_family(state),
                Action::RecordAttempt => {
                    let cursor = self.cursor.as_ref().unwrap();
                    let observation = cursor.observation;
                    if observation.attempted {
                        state.lifecycle_counters.package_entity_resync_attempts = state
                            .lifecycle_counters
                            .package_entity_resync_attempts
                            .saturating_add(1);
                        if observation.degraded {
                            state.lifecycle_counters.package_entity_resync_degraded = state
                                .lifecycle_counters
                                .package_entity_resync_degraded
                                .saturating_add(1);
                        } else {
                            let family = cursor.after.as_ref().unwrap();
                            crate::daemon::control::entities::begin_plugin_entity_resync(
                                daemon,
                                state,
                                family.clone(),
                                self.subscription_id.take().unwrap(),
                            );
                            if !state.plugin_entities.has_resync(family)
                                && let Some(deadline) = observation.deadline
                            {
                                self.remember_deadline(deadline);
                            }
                        }
                    } else if let Some(deadline) = observation.deadline {
                        self.remember_deadline(deadline);
                    }
                    self.subscription_id = None;
                    self.target_after = None;
                    self.stage = Stage::NextFamily;
                }
                Action::Clear => {
                    drop(self.cursor.take());
                    drop(self.permit.take());
                    self.identity = None;
                    self.running = false;
                    return if self.changed || runtime.entity_model_readiness().releases {
                        ScanStep::Again
                    } else if self.scanning {
                        ScanStep::Complete(self.earliest.take())
                    } else {
                        ScanStep::Idle
                    };
                }
            }
            return ScanStep::Again;
        }
        if !self.running {
            let Some(permit) = runtime.host_executor().try_reserve() else {
                self.waiting_for_host = true;
                return ScanStep::Idle;
            };
            let Some(waiter) = state.waiter_ids.next() else {
                drop(permit);
                self.faulted = true;
                return ScanStep::Idle;
            };
            self.permit = Some(permit);
            self.identity = Some(HostJobIdentity::first(waiter));
            self.running = true;
            self.scanning = self.changed;
            self.stage = if self.scanning {
                Stage::NextFamily
            } else {
                Stage::Clear
            };
            if self.changed {
                self.earliest = None;
            }
            self.changed = false;
            self.cursor = Some(Cursor::default());
            return ScanStep::Again;
        }
        if self.operation.is_none() {
            if matches!(self.stage, Stage::Target) && !runtime.entity_model_readiness().releases {
                let family = self.cursor.as_ref().unwrap().after.as_ref().unwrap();
                let target =
                    super::entity::next_package_entity_target(state, self.target_after.as_deref());
                self.subscription_id = match target {
                    Some(target) if target.entity_type == *family => {
                        Some(target.subscription_id.clone())
                    }
                    Some(target) => {
                        self.target_after = Some(target);
                        self.stage = Stage::CheckFamily;
                        return ScanStep::Again;
                    }
                    None => Some(format!("package-entity-resync-{family}")),
                };
                self.stage = Stage::Attempt;
            }
            let action = if runtime.entity_model_readiness().releases {
                Action::Release
            } else {
                match self.stage {
                    Stage::NextFamily => Action::NextFamily,
                    Stage::CheckFamily => Action::CheckFamily,
                    Stage::Attempt => Action::RecordAttempt,
                    Stage::Clear => Action::Clear,
                    Stage::Target => unreachable!(),
                }
            };
            self.action = Some(action);
            self.operation = Some(Operation::Resync {
                action,
                retained: self.cursor.take(),
            });
        }
        let identity = self.identity.unwrap();
        let operation = self.operation.take().unwrap();
        let work =
            match runtime.begin_entity_model(identity, operation, self.permit.as_ref().unwrap()) {
                Ok(work) => work,
                Err((status, operation)) => {
                    self.operation = Some(operation);
                    if status == CausalTransitionStatus::Waiting {
                        self.waiting_for_progress = true;
                    } else {
                        self.faulted = true;
                    }
                    return ScanStep::Idle;
                }
            };
        #[cfg(test)]
        if self.fail_release && self.action == Some(Action::Release) {
            work.test_fail_after_operation();
            self.fail_release = false;
        }
        self.work = Some(work.clone());
        if let Err(failure) = runtime.host_executor().submit(
            identity,
            HostCommand::EntityModel(work),
            self.permit.take().unwrap(),
        ) {
            runtime.fault_entity_model(identity);
            self.failure = Some(failure);
        }
        ScanStep::Idle
    }
}

pub(crate) fn mark_ready(state: &mut DaemonControlState) {
    state
        .maintenance
        .wakes
        .mark(MaintenanceSliceKind::ProviderResync);
}

/// Preserve changes behind the current cursor until the next complete pass.
pub(crate) fn note_package_entity_resync_change(state: &mut DaemonControlState) {
    state.package_entity_resync_scan.changed = true;
    if let Some(key) = state.package_entity_resync_scan.deadline_key.take() {
        state.deadlines.disarm(key);
    }
    mark_ready(state);
}

/// Advance one retained scan phase through the owner scheduler.
pub(crate) fn drive_package_entity_resync(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let mut scan = std::mem::take(&mut state.package_entity_resync_scan);
    let step = scan.step(daemon, state);
    state.package_entity_resync_scan = scan;
    match step {
        ScanStep::Idle => {}
        ScanStep::Again => mark_ready(state),
        ScanStep::Complete(deadline) => {
            crate::daemon::owner_loop::arm_package_entity_resync_deadline(state, deadline)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn daemon(name: &str) -> (HubDaemon, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "botster-resync-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let config = crate::HubStartupOptions {
            data_directory: crate::DataDirectoryOption::Explicit(directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".into(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("build the resync test configuration");
        (
            HubDaemon::start(config).expect("start the resync test daemon"),
            directory,
        )
    }

    fn add_release(daemon: &HubDaemon, family: &str) -> u64 {
        let runtime = daemon.runtime().unwrap();
        let family_token = runtime.test_family_causal_token(family);
        let scope = runtime
            .causal_scopes()
            .mint_with_lease(Some(
                crate::package_event_router::LeaseIdentity::ProviderResyncNeed { family_token },
            ))
            .unwrap();
        runtime.test_store_resync_lease(scope, family);
        scope
    }

    #[test]
    fn idle_scan_releases_multiple_leases_through_exact_table_receipts() {
        let (mut daemon, directory) = daemon("idle-releases");
        let mut state = DaemonControlState::default();
        let scopes: Vec<_> = (0..3).map(|_| add_release(&daemon, "releases")).collect();
        let table = Arc::clone(daemon.runtime().unwrap().causal_scopes());
        assert!(!state.package_entity_resync_scan.changed);
        let limit = Instant::now() + Duration::from_secs(3);
        table.test_with_inner_held(|| {
            while !state.package_entity_resync_scan.waiting_for_progress
                || state.package_entity_resync_scan.completion.is_none()
            {
                crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
                assert!(
                    Instant::now() < limit,
                    "the original release must reach its receipt wait"
                );
                std::thread::yield_now();
            }
            let identity = state.package_entity_resync_scan.identity;
            let release = state
                .package_entity_resync_scan
                .work
                .as_ref()
                .unwrap()
                .test_resync_release();
            assert_eq!(release, Some(scopes[0]));
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            assert_eq!(state.budget.outstanding(), 0);
            for _ in 0..20 {
                crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
                assert_eq!(state.package_entity_resync_scan.identity, identity);
                assert_eq!(
                    state
                        .package_entity_resync_scan
                        .work
                        .as_ref()
                        .unwrap()
                        .test_resync_release(),
                    release
                );
            }
            assert!(
                !state
                    .package_entity_resync_scan
                    .ready(daemon.runtime().unwrap())
            );
        });
        while scopes.iter().any(|scope| table.is_live(*scope))
            || state.package_entity_resync_scan.running
        {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "every release must apply before the scan retires"
            );
            std::thread::yield_now();
        }
        assert!(!daemon.runtime().unwrap().entity_model_readiness().releases);
        assert!(
            !state
                .package_entity_resync_scan
                .ready(daemon.runtime().unwrap())
        );
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        assert_eq!(state.budget.outstanding(), 0);
        assert!(!state.package_entity_resync_scan.changed);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn indexed_release_progresses_when_owner_capacity_refuses_new_providers() {
        let (mut daemon, directory) = daemon("release-full-owner");
        let mut state = DaemonControlState::default();
        let mut permits = Vec::new();
        while let Some(permit) = state.budget.reserve() {
            permits.push(permit);
        }
        let scope = add_release(&daemon, "releases");
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("missing.family");
        let limit = Instant::now() + Duration::from_secs(3);
        while daemon.runtime().unwrap().causal_scopes().is_live(scope)
            || state.package_entity_resync_scan.running
            || state.package_entity_resync_scan.deadline_key.is_none()
        {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "existing releases must not need new Owner capacity"
            );
            assert_eq!(state.budget.outstanding(), permits.len());
            std::thread::yield_now();
        }
        assert_eq!(state.lifecycle_counters.package_entity_resync_attempts, 1);
        assert!(!state.plugin_entities.has_resync("missing.family"));
        assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 0);
        for permit in permits {
            state.budget.release(permit);
        }
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn release_faults_retain_the_original_work_and_capacity() {
        for condition in ["host-failure", "phase-exhaustion", "stopped"] {
            let (mut daemon, directory) = daemon(condition);
            let mut state = DaemonControlState::default();
            let scope = add_release(&daemon, "releases");
            let mut scan = std::mem::take(&mut state.package_entity_resync_scan);
            assert!(matches!(scan.step(&daemon, &mut state), ScanStep::Again));
            let limit = Instant::now() + Duration::from_secs(3);
            match condition {
                "host-failure" => scan.fail_release = true,
                "phase-exhaustion" => scan.identity.as_mut().unwrap().phase = u64::MAX,
                "stopped" => daemon.runtime_mut().unwrap().test_stop_host_submissions(),
                _ => unreachable!(),
            }
            state.package_entity_resync_scan = scan;
            while !state.package_entity_resync_scan.faulted
                && state.package_entity_resync_scan.failure.is_none()
            {
                crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
                assert!(
                    Instant::now() < limit,
                    "the real Host route must retain its failure"
                );
                std::thread::yield_now();
            }
            let runtime = daemon.runtime().unwrap();
            let scan = &state.package_entity_resync_scan;
            assert!(scan.work.is_some());
            if condition == "stopped" {
                assert!(scan.failure.is_some());
                assert_eq!(runtime.test_resync_lease_count("releases"), 1);
            } else {
                assert!(scan.completion.is_some());
                assert_eq!(
                    scan.work.as_ref().unwrap().test_resync_release(),
                    Some(scope)
                );
            }
            assert!(!runtime.entity_model_available());
            assert_eq!(runtime.host_executor().outstanding(), 1);
            assert_eq!(state.budget.outstanding(), 0);
            assert!(!scan.ready(runtime));
            for _ in 0..20 {
                crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            }
            assert!(state.package_entity_resync_scan.work.is_some());
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 1);
            daemon.stop();
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn indexed_release_preserves_an_existing_provider_retry_deadline() {
        let (mut daemon, directory) = daemon("release-deadline");
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("missing.family");
        let limit = Instant::now() + Duration::from_secs(3);
        while state.package_entity_resync_scan.deadline_key.is_none() {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(Instant::now() < limit);
            std::thread::yield_now();
        }
        let deadline = state.package_entity_resync_scan.deadline_key.unwrap();
        let attempts = state.lifecycle_counters.package_entity_resync_attempts;
        assert!(!state.package_entity_resync_scan.changed);
        let scope = add_release(&daemon, "releases");
        while daemon.runtime().unwrap().causal_scopes().is_live(scope)
            || state.package_entity_resync_scan.running
        {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(Instant::now() < limit);
            std::thread::yield_now();
        }
        assert_eq!(
            state.package_entity_resync_scan.deadline_key,
            Some(deadline)
        );
        while state.lifecycle_counters.package_entity_resync_attempts == attempts {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "the retained deadline must trigger the next attempt"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn scan_returns_the_last_host_slot_without_provider_progress() {
        let (mut daemon, directory) = daemon("scan-last-slot");
        let mut state = DaemonControlState::default();
        let permits: Vec<_> = (0..7)
            .map(|_| {
                daemon
                    .runtime()
                    .unwrap()
                    .host_executor()
                    .try_reserve()
                    .unwrap()
            })
            .collect();
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("missing.family");
        let limit = Instant::now() + Duration::from_secs(3);
        while !state.package_entity_resync_scan.running {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(Instant::now() < limit);
        }
        while state.package_entity_resync_scan.running {
            assert_eq!(daemon.runtime().unwrap().host_executor().outstanding(), 8);
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "the scan must retire without provider Host capacity"
            );
            std::thread::yield_now();
        }
        assert_eq!(state.lifecycle_counters.package_entity_resync_attempts, 1);
        assert!(state.package_entity_resync_scan.permit.is_none());
        assert!(state.package_entity_resync_scan.cursor.is_none());
        drop(permits);
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn migrated_fanout_finish_wakes_an_idle_scan_after_table_application() {
        use crate::host_executor::{HostCommand, HostCompletionPoll, HostJobIdentity, HostResult};
        use crate::owner_identity::WaiterId;
        use crate::package_entity_fanout::PackageEntityMutation;
        use crate::package_event_router::LeaseIdentity;
        use crate::runtime::entity_model::{Kind, Operation};
        use crate::runtime::{CausalTransitionStatus, PackageEntitySnapshotStep};

        let (mut daemon, directory) = daemon("model-finish-wake");
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().unwrap();
        let family = "idle.family";
        runtime.test_store_family_payload(PackageEntityMutation::Upsert {
            admission: None,
            entity_type: family.into(),
            snapshot_seq: 1,
            id: "item".into(),
            entity: serde_json::json!({"id": "item"}),
        });
        let family_token = runtime.test_family_causal_token(family);
        let scope = runtime
            .causal_scopes()
            .mint_with_lease(Some(LeaseIdentity::AdmittedEntityMutation {
                family_token,
                seq: 1,
            }))
            .unwrap();
        runtime.test_store_pending_lease(scope, family, 1);
        runtime.begin_package_entity_provider_snapshot(family, 0);
        let PackageEntitySnapshotStep::Ready(item) =
            runtime.step_package_entity_provider_snapshot(family)
        else {
            panic!("the fixture retains an admitted mutation");
        };
        let (mutation, mut finish) = item.into_parts();
        drop(mutation);
        finish.scheduled_resync = true;
        runtime.take_package_entity_resync_notification();
        assert!(matches!(
            state
                .package_entity_resync_scan
                .step(&daemon, &mut DaemonControlState::default()),
            ScanStep::Idle
        ));
        assert!(state.package_entity_resync_scan.deadline_key.is_none());
        let identity = HostJobIdentity::first(WaiterId(39));
        let permit = runtime.host_executor().try_reserve().unwrap();
        let work = runtime
            .begin_entity_model(
                identity,
                Operation::FinishFanout {
                    finish,
                    retained: None,
                },
                &permit,
            )
            .unwrap_or_else(|_| panic!("model capacity is available"));
        runtime
            .host_executor()
            .submit(identity, HostCommand::EntityModel(work.clone()), permit)
            .unwrap();
        let limit = Instant::now() + Duration::from_secs(3);
        let completion = loop {
            match runtime.host_executor().poll_completion() {
                HostCompletionPoll::Ready(completion) => break completion,
                HostCompletionPoll::Empty => std::thread::yield_now(),
                HostCompletionPoll::Stopped => panic!("the Host executor stopped"),
            }
            assert!(Instant::now() < limit);
        };
        assert!(matches!(
            completion.result,
            HostResult::EntityModelComplete(Kind::FinishFanout)
        ));
        assert_eq!(
            runtime.observe_entity_model(identity, Kind::FinishFanout),
            CausalTransitionStatus::Waiting
        );
        assert!(!runtime.take_package_entity_resync_notification());
        runtime
            .causal_scopes()
            .test_with_inner_held(|| runtime.apply_causal_owner_ops());
        assert!(!runtime.release_entity_model(&work));
        assert!(!runtime.take_package_entity_resync_notification());
        runtime.apply_causal_owner_ops();
        assert_eq!(
            runtime.observe_entity_model(identity, Kind::FinishFanout),
            CausalTransitionStatus::Applied
        );
        assert!(runtime.release_entity_model(&work));
        assert!(work.owner_drop_ready());
        drop(work);
        drop(completion);
        crate::daemon::owner_loop::publish_completion_wakes(&daemon, &mut state);
        assert!(state.package_entity_resync_scan.changed);
        let mut scan = std::mem::take(&mut state.package_entity_resync_scan);
        assert!(matches!(scan.step(&daemon, &mut state), ScanStep::Again));
        assert!(scan.running);
        state.package_entity_resync_scan = scan;
        daemon.stop();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn resync_retry_uses_the_owner_deadline_without_control_traffic() {
        let (mut daemon, directory) = daemon("deadline");
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("missing.family");
        let limit = Instant::now() + Duration::from_secs(2);
        while state.lifecycle_counters.package_entity_resync_attempts == 0
            || state.package_entity_resync_scan.deadline_key.is_none()
        {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "the first attempt must arm its retry deadline"
            );
            std::thread::yield_now();
        }
        let attempts = state.lifecycle_counters.package_entity_resync_attempts;
        let deadline = state.package_entity_resync_scan.deadline_key.unwrap();
        assert_eq!(state.deadlines.next_deadline(), Some(deadline.instant()));
        while state.lifecycle_counters.package_entity_resync_attempts == attempts {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "the shared deadline must wake the next attempt"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(state.lifecycle_counters.package_entity_resync_attempts > attempts);
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove the resync test directory");
    }

    #[test]
    fn change_behind_the_family_cursor_survives_coalesced_continuations() {
        let (mut daemon, directory) = daemon("cursor");
        let mut state = DaemonControlState::default();
        let runtime = daemon.runtime().unwrap();
        for index in 0..1000 {
            runtime.test_set_family_seq(&format!("family-{index:04}"), 0);
        }
        runtime.note_package_entity_resync_changed();
        let limit = Instant::now() + Duration::from_secs(3);
        loop {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            if state
                .package_entity_resync_scan
                .test_after()
                .as_deref()
                .is_some_and(|after| after > "family-0000")
            {
                break;
            }
            assert!(Instant::now() < limit, "the family cursor must advance");
        }
        assert!(
            state.package_entity_resync_scan.running,
            "one owner turn cannot inspect all families"
        );
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("family-0000");
        while state.lifecycle_counters.package_entity_resync_attempts == 0 {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            assert!(
                Instant::now() < limit,
                "a changed family behind the cursor must receive another pass"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .package_entity_resync_attempt_total("family-0000"),
            1
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove the resync test directory");
    }

    #[test]
    fn provider_floor_progress_removes_an_obsolete_retry_deadline() {
        let (mut daemon, directory) = daemon("floor");
        let mut state = DaemonControlState::default();
        daemon
            .runtime()
            .unwrap()
            .mark_package_entity_resync_needed("family");
        crate::daemon::owner_loop::publish_completion_wakes(&daemon, &mut state);
        assert!(
            state
                .maintenance
                .wakes
                .take(MaintenanceSliceKind::ProviderResync)
        );
        crate::daemon::owner_loop::arm_package_entity_resync_deadline(
            &mut state,
            Some(Instant::now() + Duration::from_secs(1)),
        );
        assert!(state.package_entity_resync_scan.deadline_key.is_some());
        daemon
            .runtime()
            .unwrap()
            .begin_package_entity_provider_snapshot("family", 1);
        let pending_attempt = daemon
            .runtime()
            .unwrap()
            .package_entity_resync_next_attempt("family");
        assert!(pending_attempt.is_some());
        crate::daemon::owner_loop::publish_completion_wakes(&daemon, &mut state);
        assert!(state.package_entity_resync_scan.deadline_key.is_none());
        assert!(state.deadlines.is_empty());
        assert!(state.package_entity_resync_scan.changed);
        assert!(
            state
                .maintenance
                .wakes
                .take(MaintenanceSliceKind::ProviderResync)
        );
        state
            .maintenance
            .wakes
            .mark(MaintenanceSliceKind::ProviderResync);
        let limit = Instant::now() + Duration::from_secs(3);
        loop {
            crate::daemon::owner_loop::drive_ready_test_turn(&mut daemon, &mut state);
            if !state.package_entity_resync_scan.changed
                && !state.package_entity_resync_scan.running
            {
                break;
            }
            assert!(
                Instant::now() < limit,
                "the owner must finish the fresh resync scan"
            );
            std::thread::yield_now();
        }
        assert!(
            daemon
                .runtime()
                .unwrap()
                .package_entity_resync_next_attempt("family")
                .is_some(),
            "the scan must retain the need until provider delivery"
        );
        daemon.stop();
        std::fs::remove_dir_all(directory).expect("remove the resync test directory");
    }

    #[test]
    fn a_family_change_removes_the_previous_policy_deadline() {
        let mut state = DaemonControlState::default();
        crate::daemon::owner_loop::arm_package_entity_resync_deadline(
            &mut state,
            Some(Instant::now() + Duration::from_secs(1)),
        );
        let previous = state.package_entity_resync_scan.deadline_key.unwrap();
        assert_eq!(state.deadlines.len(), 1);
        note_package_entity_resync_change(&mut state);
        assert!(state.package_entity_resync_scan.deadline_key.is_none());
        assert!(state.deadlines.is_empty());
        assert!(!state.deadlines.disarm(previous));
        assert!(state.package_entity_resync_scan.changed);
    }
}
