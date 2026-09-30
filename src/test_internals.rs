//! Test-only entry points over crate-private terminal binding mechanisms.
//!
//! Compiled only with the `test-internals` feature, which this crate's own
//! dev-dependency enables for integration tests. Every entry returns the
//! same Core ticket the runtime uses internally; nothing here blocks.

use botster_core::contract::terminal_wake::WakingTerminalAdapter;
use botster_core::{
    ClientId, SessionId, SubscriptionId, TerminalCapabilitySet, TerminalSubscriptionGeneration,
};

use crate::data_plane::driver::CoreTicket;
use crate::persistence::{FileCommitError, FileCommitOutcome};
use crate::runtime::{
    AttachBindPlan, BindRoutePlan, HubRuntime, attach_and_bind_on_core, attach_route_on_core,
    bind_route_on_core,
};
use crate::shared_view::SharedView;
use crate::{
    FileHubStateStore, HubConfig, HubState, HubStateStore, HubStateStoreError, HubStateStoreResult,
};

/// Test-only durable state fixture writes through the reserved production path.
pub trait TestHubStateStoreExt {
    fn update_test_fixture(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState>;
}

impl TestHubStateStoreExt for FileHubStateStore {
    fn update_test_fixture(
        &self,
        config: &HubConfig,
        update: impl FnOnce(&mut HubState),
    ) -> HubStateStoreResult<HubState> {
        let (mut state, Some(mut authority)) = self.load_retained(config)? else {
            unreachable!("File load returns authority")
        };
        let prior = SharedView::from_reserved(
            state.clone(),
            authority.take_startup_charge().expect("startup charge"),
        );
        update(&mut state);
        let outcome = self.save_retained_startup_state(&authority, 0, Some(prior), state);
        match outcome {
            Ok(FileCommitOutcome::Synced { state, .. }) => Ok((*state).clone()),
            Ok(FileCommitOutcome::PublishedUncertain(write)) => {
                Err(HubStateStoreError::PublishedUncertain(write))
            }
            Err(FileCommitError::Preparation(error))
            | Err(FileCommitError::BeforePublication { error, .. }) => Err(error),
            Err(FileCommitError::Stale(_)) | Err(FileCommitError::RevisionExhausted(_)) => {
                unreachable!("fixture startup revision is fixed")
            }
        }
    }
}

/// A fake or real terminal adapter a test binds to one route.
pub type TestTerminalAdapter = Box<dyn WakingTerminalAdapter + Send>;

/// Attach one client route and bind an adapter to it as one Core operation.
pub struct TestAttachBindPlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: TestTerminalAdapter,
}

/// Bind an adapter to a route that is already attached at `generation`.
pub struct TestBindRoutePlan {
    pub client_id: ClientId,
    pub session_id: SessionId,
    pub subscription_id: SubscriptionId,
    pub generation: TerminalSubscriptionGeneration,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub adapter: TestTerminalAdapter,
}

/// Attach and bind on the Core owner thread. Failures are reported as their
/// debug rendering; tests only assert on success or on the failure text.
pub fn attach_and_bind_terminal(
    runtime: &HubRuntime,
    plan: TestAttachBindPlan,
) -> CoreTicket<Result<TerminalSubscriptionGeneration, String>> {
    let plan = AttachBindPlan {
        client_id: plan.client_id,
        session_id: plan.session_id,
        subscription_id: plan.subscription_id,
        capabilities: plan.capabilities,
        now_seconds: plan.now_seconds,
        adapter: plan.adapter,
    };
    runtime.submit_core(move |daemon| {
        attach_and_bind_on_core(daemon, plan).map_err(|error| format!("{error:?}"))
    })
}

/// Attach one client route without binding an adapter.
pub fn attach_route(
    runtime: &HubRuntime,
    client_id: ClientId,
    session_id: SessionId,
    subscription_id: SubscriptionId,
    now_seconds: u64,
) -> CoreTicket<Result<TerminalSubscriptionGeneration, String>> {
    runtime.submit_core(move |daemon| {
        attach_route_on_core(daemon, client_id, session_id, subscription_id, now_seconds)
            .map_err(|error| format!("{error:?}"))
    })
}

/// Bind an adapter to an attached route.
pub fn bind_route_adapter(
    runtime: &HubRuntime,
    plan: TestBindRoutePlan,
) -> CoreTicket<Result<(), String>> {
    let plan = BindRoutePlan {
        client_id: plan.client_id,
        session_id: plan.session_id,
        subscription_id: plan.subscription_id,
        generation: plan.generation,
        capabilities: plan.capabilities,
        now_seconds: plan.now_seconds,
        adapter: plan.adapter,
    };
    runtime.submit_core(move |daemon| {
        bind_route_on_core(daemon, plan).map_err(|error| format!("{error:?}"))
    })
}

/// Navigation rows of one package row, the function the daemon's package-navigation read shares.
pub fn package_navigation_entries(
    package: crate::HubClientPackage,
) -> Vec<crate::HubClientPackageNavigationEntry> {
    package.navigation_entries()
}

/// The admission check that a plugin surface render or action passes before runtime dispatch.
pub fn admit_plugin_surface_operation(
    packages: &crate::PackageRegistry,
    package_name: &str,
    surface_id: &str,
    required_operation: botster_ui_contract::PackageSurfaceOperation,
    request_id: botster_core::RequestId,
    operation: crate::HubClientOperation,
) -> crate::HubClientResult<()> {
    crate::client_api::admit_plugin_surface_operation(
        packages,
        package_name,
        surface_id,
        required_operation,
        request_id,
        operation,
    )
}

const LOCAL_CLIENT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// A local client identity over the runtime's public Core operations. It stands in for the
/// removed in-process request layer, so these tests still drive one client's session verbs.
pub struct LocalClient {
    pub client_id: botster_core::ClientId,
}

impl LocalClient {
    pub fn new(name: &str) -> Self {
        Self {
            client_id: botster_core::ClientId(name.to_string()),
        }
    }

    pub fn spawn(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
        command: &str,
    ) -> botster_core::CoreSession {
        let config = runtime.config();
        let request = botster_core::SessionSpawnRequest {
            request_id: botster_core::RequestId(format!("spawn-{}", session_id.0)),
            session_id: session_id.clone(),
            executable: config.session_defaults.shell.clone(),
            arguments: vec!["-c".to_string(), command.to_string()],
            working_directory: botster_core::SpawnWorkingDirectory {
                path: config
                    .session_defaults
                    .working_directory
                    .as_deref()
                    .expect("test config has an explicit working directory")
                    .display()
                    .to_string(),
            },
            environment: botster_core::SpawnEnvironment::default(),
            initial_pty_size: Some(botster_core::ResizePayload {
                rows: config.session_defaults.initial_rows,
                cols: config.session_defaults.initial_cols,
            }),
        };
        match runtime
            .begin_spawn(request, botster_core::CoreSessionMetadata::new())
            .wait(runtime, LOCAL_CLIENT_WAIT)
        {
            Ok(botster_core_daemon::CoreCompletion::Spawn { result, .. }) => {
                result.expect("spawn through core daemon")
            }
            other => panic!("unexpected spawn completion: {other:?}"),
        }
    }

    pub fn read_screen(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<botster_core_daemon::ScreenReadback, botster_core_daemon::CoreDaemonError> {
        match runtime
            .begin_read_screen(
                botster_core::RequestId("read-screen".to_string()),
                session_id.clone(),
                now_seconds,
            )
            .wait(runtime, LOCAL_CLIENT_WAIT)
        {
            Ok(botster_core_daemon::CoreCompletion::ReadScreen { result, .. }) => result,
            Err(error) => Err(error),
            other => panic!("unexpected read-screen completion: {other:?}"),
        }
    }

    pub fn read_mode_flags(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<botster_core_daemon::ModeFlagsReadback, botster_core_daemon::CoreDaemonError> {
        match runtime
            .begin_read_mode_flags(
                botster_core::RequestId("read-mode-flags".to_string()),
                session_id.clone(),
                now_seconds,
            )
            .wait(runtime, LOCAL_CLIENT_WAIT)
        {
            Ok(botster_core_daemon::CoreCompletion::ReadModeFlags { result, .. }) => result,
            Err(error) => Err(error),
            other => panic!("unexpected mode-flags completion: {other:?}"),
        }
    }

    pub fn capture_snapshot(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
        now_seconds: u64,
    ) -> Result<botster_core_daemon::SnapshotCapture, botster_core_daemon::CoreDaemonError> {
        let owner = botster_core_daemon::CaptureOwner(format!("client:{}", self.client_id.0));
        match runtime
            .begin_capture_snapshot(
                botster_core::RequestId("capture-snapshot".to_string()),
                session_id.clone(),
                now_seconds,
                owner,
            )
            .wait(runtime, LOCAL_CLIENT_WAIT)
        {
            Ok(botster_core_daemon::CoreCompletion::CaptureSnapshot { result, .. }) => result,
            Err(error) => Err(error),
            other => panic!("unexpected snapshot completion: {other:?}"),
        }
    }

    pub fn detach(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
        subscription_id: &SubscriptionId,
        now_seconds: u64,
    ) -> Result<(), botster_core_daemon::CoreDaemonError> {
        runtime
            .detach_client(
                self.client_id.clone(),
                session_id.clone(),
                subscription_id.clone(),
                now_seconds,
            )
            .wait(LOCAL_CLIENT_WAIT)
            .expect("core bridge answers the detach")
    }

    pub fn shutdown(
        &self,
        runtime: &HubRuntime,
        session_id: &SessionId,
    ) -> Result<(), botster_core_daemon::CoreDaemonError> {
        match runtime
            .begin_shutdown_session(session_id.clone())
            .wait(runtime, LOCAL_CLIENT_WAIT)
        {
            Ok(botster_core_daemon::CoreCompletion::ShutdownSession { result, .. }) => result,
            Err(error) => Err(error),
            other => panic!("unexpected shutdown completion: {other:?}"),
        }
    }
}
