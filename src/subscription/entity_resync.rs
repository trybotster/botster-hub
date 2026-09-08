//! Incremental family inspection for package entity resync.

use std::sync::Arc;
use std::time::Instant;

use crate::HubDaemon;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::DeadlineKey;
use crate::daemon_maintenance::MaintenanceSliceKind;
use crate::plugin_entity::Target;

/// One cursor covers the family collection. Changes request another complete pass.
#[derive(Default)]
pub(crate) struct PackageEntityResyncScan {
    after: Option<String>,
    target_after: Option<Arc<Target>>,
    finding_target: bool,
    releasing_degraded_leases: bool,
    earliest: Option<Instant>,
    running: bool,
    changed: bool,
    pub(crate) deadline_key: Option<DeadlineKey>,
}

enum ScanStep {
    Idle,
    Again,
    Complete(Option<Instant>),
}

impl PackageEntityResyncScan {
    fn remember_deadline(&mut self, deadline: Instant) {
        self.earliest = Some(self.earliest.map_or(deadline, |old| old.min(deadline)));
    }

    fn step(&mut self, daemon: &HubDaemon, state: &mut DaemonControlState) -> ScanStep {
        let Some(runtime) = daemon.runtime() else {
            return ScanStep::Idle;
        };
        if !self.running {
            if !self.changed {
                return ScanStep::Idle;
            }
            self.running = true;
            self.changed = false;
            self.after = None;
            self.target_after = None;
            self.finding_target = false;
            self.releasing_degraded_leases = false;
            self.earliest = None;
        }
        if self.finding_target {
            return self.step_target(daemon, state);
        }
        if self.releasing_degraded_leases {
            let family = self
                .after
                .as_deref()
                .expect("lease release retains its family");
            self.releasing_degraded_leases =
                runtime.release_one_degraded_package_entity_resync_lease(family);
            if self.releasing_degraded_leases {
                state
                    .maintenance
                    .wakes
                    .mark(MaintenanceSliceKind::HostBridge);
            }
            return ScanStep::Again;
        }
        let Some((family, deadline, degraded_leases)) =
            runtime.next_package_entity_resync_family(self.after.as_deref())
        else {
            self.running = false;
            return if self.changed {
                ScanStep::Again
            } else {
                ScanStep::Complete(self.earliest.take())
            };
        };
        let pending = state.plugin_entities.has_resync(&family);
        self.after = Some(family);
        self.releasing_degraded_leases = degraded_leases;
        if !pending && let Some(deadline) = deadline {
            if deadline <= Instant::now() {
                self.finding_target = true;
                self.target_after = None;
            } else {
                self.remember_deadline(deadline);
            }
        }
        ScanStep::Again
    }

    /// Inspect one subscriber, then admit the provider after a matching subscriber or the end.
    fn step_target(&mut self, daemon: &HubDaemon, state: &mut DaemonControlState) -> ScanStep {
        let runtime = daemon.runtime().expect("the scan has a live runtime");
        let family = self
            .after
            .as_ref()
            .expect("target selection retains the family");
        let deadline = runtime.package_entity_resync_next_attempt(family);
        if state.plugin_entities.has_resync(family) || deadline.is_none() {
            self.finding_target = false;
            self.target_after = None;
            return ScanStep::Again;
        }
        let deadline = deadline.expect("the resync need was checked");
        if deadline > Instant::now() {
            self.finding_target = false;
            self.target_after = None;
            self.remember_deadline(deadline);
            return ScanStep::Again;
        }
        let target = super::entity::next_package_entity_target(state, self.target_after.as_deref());
        let subscription_id = match target {
            Some(target) if target.entity_type == *family => target.subscription_id.clone(),
            Some(target) => {
                self.target_after = Some(target);
                return ScanStep::Again;
            }
            None => format!("package-entity-resync-{family}"),
        };
        state.lifecycle_counters.package_entity_resync_attempts = state
            .lifecycle_counters
            .package_entity_resync_attempts
            .saturating_add(1);
        if runtime.record_package_entity_resync_attempt(family) {
            self.releasing_degraded_leases = true;
            state
                .maintenance
                .wakes
                .mark(MaintenanceSliceKind::HostBridge);
            state.lifecycle_counters.package_entity_resync_degraded = state
                .lifecycle_counters
                .package_entity_resync_degraded
                .saturating_add(1);
        } else {
            crate::daemon::control::entities::begin_plugin_entity_resync(
                daemon,
                state,
                family.clone(),
                subscription_id,
            );
            if !state.plugin_entities.has_resync(family)
                && let Some(deadline) = runtime.package_entity_resync_next_attempt(family)
            {
                self.remember_deadline(deadline);
            }
        }
        self.finding_target = false;
        self.target_after = None;
        ScanStep::Again
    }
}

/// Preserve changes behind the current cursor until the next complete pass.
pub(crate) fn note_package_entity_resync_change(state: &mut DaemonControlState) {
    state.package_entity_resync_scan.changed = true;
    if let Some(key) = state.package_entity_resync_scan.deadline_key.take() {
        state.deadlines.disarm(key);
    }
    state
        .maintenance
        .wakes
        .mark(MaintenanceSliceKind::ProviderResync);
}

/// Perform one family inspection or one subscriber inspection through the owner scheduler.
pub(crate) fn drive_package_entity_resync(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let mut scan = std::mem::take(&mut state.package_entity_resync_scan);
    let step = scan.step(daemon, state);
    state.package_entity_resync_scan = scan;
    match step {
        ScanStep::Idle => {}
        ScanStep::Again => state
            .maintenance
            .wakes
            .mark(MaintenanceSliceKind::ProviderResync),
        ScanStep::Complete(deadline) => {
            crate::daemon::owner_loop::arm_package_entity_resync_deadline(state, deadline);
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
                .after
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
        crate::daemon::owner_loop::arm_package_entity_resync_deadline(
            &mut state,
            Some(Instant::now() + Duration::from_secs(1)),
        );
        assert!(state.package_entity_resync_scan.deadline_key.is_some());
        daemon
            .runtime()
            .unwrap()
            .begin_package_entity_provider_snapshot("family", 1);
        crate::daemon::owner_loop::publish_completion_wakes(&daemon, &mut state);
        assert!(state.package_entity_resync_scan.deadline_key.is_none());
        assert!(state.deadlines.is_empty());
        assert_eq!(
            daemon
                .runtime()
                .unwrap()
                .package_entity_resync_next_attempt("family"),
            None
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
