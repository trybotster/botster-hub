//! Plugin test kit: the real Hub daemon without transports.
//!
//! [`KitHub`] starts a [`HubDaemon`] and drives its production owner turn on
//! the caller's thread. Requests enter through the production control
//! request path, and every plugin invocation runs in the real Lua runtime.
//! A step returns only after the owner has settled: no request is pending,
//! no plugin invocation is in flight, and no owner work is ready. Session
//! lifecycle is the one supplied input: the kit feeds Core's own page types
//! through the production baseline and journal consumers.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use botster_core::{
    EnvelopeCursor, EnvelopeId, EnvelopeTarget, RoutedEnvelope, RoutedEnvelopeDrainOutcome,
    SessionId, SessionLifecycleState,
};
pub use botster_core_daemon::RoutedEnvelopeDeliveryStateResult;
pub use botster_core_daemon::{DaemonSession, RegistrySessionState, SessionLifecycleRecord};
use botster_core_daemon::{
    SessionLifecycleBaselinePage, SessionLifecycleChange, SessionLifecycleChangeKind,
    SessionLifecycleCursor, SessionLifecyclePage, SessionLifecycleSourceId,
};
pub use botster_hub_client::DaemonPluginLogs;
use botster_hub_client::{DaemonRequest, DaemonResponse};
use tokio::sync::mpsc as tokio_mpsc;

use crate::HubDaemon;
use crate::daemon::control::message::{ControlMessage, ControlReplyReceiver};
use crate::daemon::owner_loop::{
    DaemonControlState, drive_ready_test_turn, publish_test_readiness,
};

/// The name prefix of the packages the kit generates to observe emitted
/// events as a consumer. Each observed package gets its own observer,
/// named `<prefix>-<package>`, because the Hub installs a package once.
pub const OBSERVER_PACKAGE: &str = "botster-plugin-test-kit-observer";

/// The plugin_db key under which the observer appends observed events.
const OBSERVED_EVENTS_KEY: &str = "events";

/// The observer's Lua before its generated `observe` calls.
const OBSERVER_LUA_PRELUDE: &str = r#"local plugin_db = botster.capabilities.plugin_db
local function observe(owner, name)
  botster.events.on({ owner = owner, name = name }, function(payload)
    local current = plugin_db.get({ key = "events" })
    local items = {}
    if current.record then items = current.record.payload.items end
    items[#items + 1] = { owner = owner, name = name, payload = payload }
    plugin_db.set({ key = "events", schema_version = 1, payload = { items = items } })
    return { ok = true }
  end)
end
"#;

/// The logical clock's start: Unix milliseconds, then Hub monotonic time.
const KIT_CLOCK_WALL_START_MS: u64 = 1_700_000_000_000;
const KIT_CLOCK_MONOTONIC_START_MS: u64 = 0;

/// How many recent wake kinds a `not_settled` description names.
const RECENT_WAKES: usize = 8;

/// Queue depth of the kit's owner wake channel, as `serve` bounds its own.
const WAKE_QUEUE: usize = 64;

/// Queue depth of one kit entity subscription.
const ENTITY_FRAME_QUEUE: usize = 64;

/// The most routed envelopes one `routed` read returns.
const ROUTED_READ_LIMIT: usize = 256;

/// The lifecycle source id that the kit's supplied pages carry.
const KIT_LIFECYCLE_SOURCE: &str = "botster-plugin-test-kit";

/// The shared hang guard for one kit step. Normal progress arrives as an
/// owner wake long before it; expiry means the step cannot settle.
pub const DEFAULT_STEP_DEADLINE: Duration = Duration::from_secs(60);

/// Why a kit operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KitError {
    /// The daemon could not start.
    Start(String),
    /// The owner did not settle before the step deadline. The description
    /// names the work that was still pending.
    NotSettled {
        /// Pending owner work at the deadline.
        pending: String,
    },
    /// The daemon dropped a request's reply channel without a reply.
    ReplyDropped,
    /// The daemon returned a transport-level error for a request.
    Daemon(String),
    /// The owner turn requested daemon shutdown.
    Shutdown,
    /// The kit could not read a package manifest or write its observer.
    Package(String),
    /// The kit observer package failed to enable.
    Observer(String),
    /// The Hub does not support this yet; the named premise gate tracks it.
    Unsupported {
        /// The refused kit feature.
        feature: &'static str,
        /// The blocked acceptance gate that delivers it.
        gate: &'static str,
    },
}

impl std::fmt::Display for KitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start(message) => write!(formatter, "kit daemon failed to start: {message}"),
            Self::NotSettled { pending } => {
                write!(formatter, "not_settled: the step did not settle: {pending}")
            }
            Self::ReplyDropped => write!(formatter, "the daemon dropped a request reply"),
            Self::Daemon(message) => write!(formatter, "daemon error: {message}"),
            Self::Shutdown => write!(formatter, "the owner turn requested shutdown"),
            Self::Package(message) => write!(formatter, "package error: {message}"),
            Self::Observer(message) => write!(formatter, "kit observer failed: {message}"),
            Self::Unsupported { feature, gate } => write!(
                formatter,
                "unsupported_by_kit: {feature} is not supported yet (gate {gate})"
            ),
        }
    }
}

impl std::error::Error for KitError {}

/// Kit start options.
#[derive(Debug, Clone)]
pub struct KitOptions {
    /// Directory that holds the daemon's data directory. The caller owns it.
    pub root: PathBuf,
    /// Hang guard for each step.
    pub step_deadline: Duration,
    /// Deadline for one event handler invocation. `None` keeps the Hub's
    /// production value. A test that holds a handler raises it.
    pub event_invocation_timeout: Option<Duration>,
    /// Remove `root` after the daemon stops.
    pub remove_root: bool,
}

impl KitOptions {
    /// Options with the default step deadline.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            step_deadline: DEFAULT_STEP_DEADLINE,
            event_invocation_timeout: None,
            remove_root: false,
        }
    }

    /// Options over a new, empty, unique root that the kit removes when it
    /// drops. The root is short and under `/tmp`, so Unix socket paths inside
    /// the data directory stay under the platform limit.
    pub fn temporary(label: &str) -> Result<Self, KitError> {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let label: String = label
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
            .take(24)
            .collect();
        let root = PathBuf::from("/tmp").join(format!(
            "bpk-{}-{}-{label}",
            std::process::id(),
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        if root.exists() {
            std::fs::remove_dir_all(&root)
                .map_err(|error| KitError::Start(format!("{}: {error}", root.display())))?;
        }
        std::fs::create_dir_all(&root)
            .map_err(|error| KitError::Start(format!("{}: {error}", root.display())))?;
        Ok(Self {
            remove_root: true,
            ..Self::new(root)
        })
    }
}

/// A real Hub daemon driven by the test thread.
pub struct KitHub {
    daemon: HubDaemon,
    state: DaemonControlState,
    /// Owner wakes from the same sources `serve` binds.
    wake_rx: tokio_mpsc::Receiver<ControlMessage>,
    /// Blocks the test thread in the owner's wait.
    waiter: tokio::runtime::Runtime,
    transport: tokio::runtime::Runtime,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    control_rx: tokio_mpsc::Receiver<ControlMessage>,
    step_deadline: Duration,
    lifecycle_sequence: u64,
    next_request: u64,
    root: PathBuf,
    remove_root: bool,
    entity_subscriptions: Vec<KitEntitySubscription>,
    /// The kinds of the latest wakes, for a `not_settled` description.
    recent_wakes: std::collections::VecDeque<&'static str>,
    wake_count: u64,
    /// The observer package of each enabled package that declares
    /// plugin-audience events, in enable order.
    observers: Vec<String>,
    /// The global names of the plugin sandbox, read back from a probe plugin
    /// in this Hub the first time a spec asks.
    sandbox_globals: Option<std::collections::BTreeSet<String>>,
    /// The packages that loaded, in enable order. `advance` drains their timers.
    loaded: Vec<String>,
}

/// One plugin timer that `KitHub::advance` found due.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerFired {
    pub package: String,
    pub resource_id: String,
    pub sequence: u64,
}

/// Who a kit tool call runs for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KitCaller {
    /// The local operator, as over the Hub socket.
    Operator,
    /// A verified session. `hub_id` defaults to this Hub's own id.
    Session {
        hub_id: Option<String>,
        session_id: String,
    },
}

/// One kit-held entity subscription and the frames it has received.
struct KitEntitySubscription {
    entity_type: String,
    receiver: tokio_mpsc::Receiver<crate::entity_delivery::EntityDelivery>,
    frames: Vec<serde_json::Value>,
}

impl KitHub {
    /// Start a daemon under `options.root` without transports.
    pub fn start(options: KitOptions) -> Result<Self, KitError> {
        let config = kit_config(&options.root.join("data"))?;
        let mut daemon =
            HubDaemon::start(config).map_err(|error| KitError::Start(error.to_string()))?;
        // The kit's Hub runs on a logical clock, so a test never waits on wall
        // time. It is chosen here, before any plugin loads.
        if let Some(runtime) = daemon.runtime() {
            runtime
                .clock()
                .make_logical(KIT_CLOCK_WALL_START_MS, KIT_CLOCK_MONOTONIC_START_MS);
        }
        // Mirror serve: the same wake sources, one owner doorbell, and a
        // plugin result budget whose notifier wakes the owner.
        let (wake_tx, wake_rx) = tokio_mpsc::channel(WAKE_QUEUE);
        let mut state = DaemonControlState::default();
        state.maintenance = crate::daemon_maintenance::MaintenanceState::with_supplied_lifecycle();
        state.maintenance.test_event_invocation_timeout_ms = options
            .event_invocation_timeout
            .map(|timeout| u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX));
        state.event_plane = daemon.local_webrtc().event_plane();
        state.plugin_result_budget =
            crate::daemon::control::reply::RetainedPluginResultBudget::new();
        if let Some(runtime) = daemon.runtime() {
            runtime.bind_data_plane_owner_wake(wake_tx.clone());
            runtime.bind_host_owner_wake(wake_tx.clone());
            runtime.bind_managed_spawn_owner_wake(wake_tx.clone());
            state.pending_runtime.close_source = runtime.close_work_source();
        }
        state.plugin_result_budget.bind_owner_wake(wake_tx.clone());
        state.entity_capacity_wake.bind(wake_tx);
        daemon
            .local_webrtc()
            .bind_entity_capacity_wake(state.entity_capacity_wake.clone());
        if let Some(runtime) = daemon.runtime() {
            runtime.install_plugin_completion_notifier(
                state.plugin_result_budget.completion_notifier(),
            );
            if !Arc::ptr_eq(state.event_plane.owner_signal(), runtime.owner_signal()) {
                return Err(KitError::Start(
                    "the event plane and the runtime must share one owner doorbell".to_string(),
                ));
            }
        }
        let waiter = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| KitError::Start(error.to_string()))?;
        let transport = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| KitError::Start(error.to_string()))?;
        let (control_tx, control_rx) = tokio_mpsc::channel(64);
        Ok(Self {
            daemon,
            state,
            wake_rx,
            waiter,
            transport,
            control_tx,
            control_rx,
            step_deadline: options.step_deadline,
            lifecycle_sequence: 0,
            next_request: 0,
            root: options.root,
            remove_root: options.remove_root,
            entity_subscriptions: Vec::new(),
            recent_wakes: std::collections::VecDeque::new(),
            wake_count: 0,
            observers: Vec::new(),
            sandbox_globals: None,
            loaded: Vec::new(),
        })
    }

    /// Send one request through the production control path and settle.
    pub fn request(&mut self, request: DaemonRequest) -> Result<DaemonResponse, KitError> {
        let reply = self.submit(request);
        self.await_reply(reply)
    }

    /// Settle, then return the reply that the step produced.
    fn await_reply(&mut self, mut reply: ControlReplyReceiver) -> Result<DaemonResponse, KitError> {
        let mut response = None;
        let settled = self.settle_with(|_| {
            if response.is_none() {
                match reply.try_recv() {
                    Ok(value) => response = Some(value.into_parts().0),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return false,
                    Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                        response = Some(Err(
                            crate::daemon::error::DaemonTransportError::DaemonNotRunning,
                        ));
                    }
                }
            }
            true
        });
        if let Err(KitError::NotSettled { pending }) = settled {
            return Err(KitError::NotSettled {
                pending: format!("reply_received={} {pending}", response.is_some()),
            });
        }
        settled?;
        match response {
            Some(Ok(response)) => Ok(response),
            Some(Err(error)) => Err(KitError::Daemon(error.to_string())),
            None => Err(KitError::ReplyDropped),
        }
    }

    /// Install and enable a local package directory.
    ///
    /// When the package declares events for the `plugins` audience and
    /// loads, the kit also enables its observer package, which subscribes
    /// to each of them through the real event router. Each package has its
    /// own observer, so enabling another package keeps every earlier one.
    pub fn enable_package(&mut self, path: &Path) -> Result<DaemonResponse, KitError> {
        let response = self.request(DaemonRequest::EnablePackageLocalPath {
            path: path.to_path_buf(),
        })?;
        if response.error.is_none() {
            self.loaded.push(package_name_of(path)?);
            let observed = observable_events(path)?;
            if !observed.is_empty() {
                let observer_name = format!("{OBSERVER_PACKAGE}-{}", observed[0].0);
                let observer = write_observer_package(&self.root, &observer_name, &observed)?;
                let enabled =
                    self.request(DaemonRequest::EnablePackageLocalPath { path: observer })?;
                if let Some(error) = enabled.error {
                    return Err(KitError::Observer(format!("{error:?}")));
                }
                self.observers.push(observer_name);
            }
        }
        Ok(response)
    }

    /// Move the Hub's logical clock forward and return the plugin timers that
    /// became due, in due order per package. Nothing sleeps.
    ///
    /// The Hub does not run Lua timer callbacks yet (gate G2), so a fired
    /// timer is reported and no plugin code runs.
    pub fn advance(&mut self, ms: u64) -> Result<Vec<TimerFired>, KitError> {
        let runtime = self
            .daemon
            .runtime_mut()
            .ok_or_else(|| KitError::Daemon("the daemon has no runtime".to_string()))?;
        let now_ms = runtime
            .clock()
            .advance(ms)
            .ok_or_else(|| KitError::Daemon("the kit clock is not logical".to_string()))?;
        let mut fired = Vec::new();
        for package in &self.loaded {
            let events = runtime
                .drain_capability_events_at(&botster_core::PluginKey(package.clone()), now_ms)
                .map_err(|error| KitError::Daemon(error.to_string()))?;
            for event in events {
                if let botster_core::CapabilityRuntimeEvent::TimerFired(event) = event {
                    fired.push(TimerFired {
                        package: package.clone(),
                        resource_id: event.resource.resource_id,
                        sequence: event.sequence,
                    });
                }
            }
        }
        Ok(fired)
    }

    /// The Hub's logical monotonic time, in milliseconds.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.daemon
            .runtime()
            .map_or(0, |runtime| runtime.clock().monotonic_ms())
    }

    /// The MCP tools that plugins publish, as an agent sees them.
    pub fn list_tools(&mut self) -> Result<Vec<serde_json::Value>, KitError> {
        Ok(self
            .request(DaemonRequest::PluginMcpListTools)?
            .plugin_tools)
    }

    /// Every structured log record of one package.
    pub fn logs(&mut self, package_name: &str) -> Result<DaemonPluginLogs, KitError> {
        let response = self.request(DaemonRequest::ReadPluginLogs {
            package_name: package_name.to_string(),
            after_seq: 0,
        })?;
        if let Some(error) = response.error {
            return Err(KitError::Daemon(format!("{error:?}")));
        }
        response
            .plugin_logs
            .ok_or_else(|| KitError::Daemon("read_plugin_logs returned no logs".to_string()))
    }

    /// Every plugin_db record payload of one package, by key.
    pub fn plugin_db(
        &self,
        package_name: &str,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, KitError> {
        let runtime = self
            .daemon
            .runtime()
            .ok_or_else(|| KitError::Daemon("the daemon has no runtime".to_string()))?;
        let capabilities = runtime.capability_runtime();
        let capabilities = capabilities
            .lock()
            .map_err(|_| KitError::Daemon("capability runtime lock poisoned".to_string()))?;
        let records = capabilities
            .kit_plugin_records(&botster_core::PluginKey(package_name.to_string()))
            .map_err(|error| KitError::Daemon(error.to_string()))?;
        Ok(records
            .into_iter()
            .map(|record| (record.key.0, record.payload))
            .collect())
    }

    /// Subscribe to one entity type as a client does. The kit drains the
    /// subscription on every owner turn, so a bounded subscription queue
    /// never holds the owner back.
    pub fn subscribe_entities(&mut self, entity_type: &str) -> Result<DaemonResponse, KitError> {
        let (frame_tx, frame_rx) = tokio_mpsc::channel(ENTITY_FRAME_QUEUE);
        let (reply_tx, reply) = crate::daemon::control::message::control_reply_channel();
        self.next_request += 1;
        crate::daemon::control::entities::handle(
            &mut self.daemon,
            &mut self.state,
            ControlMessage::SubscribeEntities {
                entity_type: entity_type.to_string(),
                subscription_id: format!("plugin-test-kit-{}", self.next_request),
                transport_request_id: None,
                client_id: Some("plugin-test-kit".to_string()),
                frame_tx: crate::subscription::entity::EntityFrameSender::Async(frame_tx),
                frame_rx: None,
                reply_tx,
                grant_id: None,
            },
        );
        self.entity_subscriptions.push(KitEntitySubscription {
            entity_type: entity_type.to_string(),
            receiver: frame_rx,
            frames: Vec::new(),
        });
        let response = self.await_reply(reply)?;
        if response.error.is_some() {
            // A refused subscription delivers no frames; keep none of it.
            self.entity_subscriptions.pop();
        }
        Ok(response)
    }

    /// Every entity frame the kit's subscriptions to `entity_type` have
    /// received, as a client decodes them.
    #[must_use]
    pub fn entity_frames(&self, entity_type: &str) -> Vec<serde_json::Value> {
        self.entity_subscriptions
            .iter()
            .filter(|subscription| subscription.entity_type == entity_type)
            .flat_map(|subscription| subscription.frames.iter().cloned())
            .collect()
    }

    /// Routed envelopes queued for one target, as Core holds them. Reading
    /// does not acknowledge them.
    pub fn routed(
        &self,
        target: botster_core::EnvelopeTarget,
    ) -> Result<Vec<botster_core::RoutedEnvelope>, KitError> {
        let runtime = self
            .daemon
            .runtime()
            .ok_or_else(|| KitError::Daemon("the daemon has no runtime".to_string()))?;
        let outcome = runtime
            .drain_routed_envelopes(target, None, ROUTED_READ_LIMIT)
            .wait(self.step_deadline)
            .map_err(|error| KitError::Daemon(format!("{error:?}")))?
            .map_err(|error| KitError::Daemon(error.to_string()))?;
        Ok(outcome.envelopes)
    }

    /// Receive routed envelopes as a target does, through Core's own drain.
    /// Envelopes stay queued until `ack_routed`, so a second read returns
    /// them again (at-least-once delivery).
    pub fn receive_routed(
        &self,
        target: EnvelopeTarget,
        after: Option<EnvelopeCursor>,
        limit: usize,
    ) -> Result<RoutedEnvelopeDrainOutcome, KitError> {
        let runtime = self
            .daemon
            .runtime()
            .ok_or_else(|| KitError::Daemon("the daemon has no runtime".to_string()))?;
        runtime
            .drain_routed_envelopes(target, after, limit)
            .wait(self.step_deadline)
            .map_err(|error| KitError::Daemon(format!("{error:?}")))?
            .map_err(|error| KitError::Daemon(error.to_string()))
    }

    /// Acknowledge one routed envelope as its target, through Core. An
    /// acknowledged envelope leaves the queue.
    pub fn ack_routed(
        &self,
        target: EnvelopeTarget,
        envelope_id: EnvelopeId,
    ) -> Result<RoutedEnvelopeDeliveryStateResult, KitError> {
        let runtime = self
            .daemon
            .runtime()
            .ok_or_else(|| KitError::Daemon("the daemon has no runtime".to_string()))?;
        runtime
            .acknowledge_routed_envelope(target, envelope_id)
            .wait(self.step_deadline)
            .map_err(|error| KitError::Daemon(format!("{error:?}")))?
            .map_err(|error| KitError::Daemon(error.to_string()))
    }

    /// The global names that a plugin can read in this Hub's real sandbox. The
    /// kit enables a probe plugin once, and the probe lists its own `_G`: the
    /// answer comes from the runtime, not from a list kept by hand.
    pub fn sandbox_globals(&mut self) -> Result<std::collections::BTreeSet<String>, KitError> {
        if let Some(names) = &self.sandbox_globals {
            return Ok(names.clone());
        }
        let package = write_globals_probe_package(&self.root)?;
        let enabled = self.request(DaemonRequest::EnablePackageLocalPath { path: package })?;
        if let Some(error) = enabled.error {
            return Err(KitError::Observer(format!("{error:?}")));
        }
        let listed = self.call_tool(
            &format!("{GLOBALS_PROBE_PACKAGE}.list"),
            serde_json::json!({}),
        )?;
        let names: std::collections::BTreeSet<String> = listed
            .plugin_tool_result
            .get("names")
            .and_then(serde_json::Value::as_array)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| name.as_str().map(str::to_string))
                    .collect()
            })
            .ok_or_else(|| KitError::Package("the globals probe returned no names".to_string()))?;
        self.sandbox_globals = Some(names.clone());
        Ok(names)
    }

    /// The Hub id of this kit Hub, as `botster.hub.identity()` reports it.
    #[must_use]
    pub fn hub_id(&self) -> String {
        self.daemon
            .runtime()
            .map(|runtime| runtime.state().host.id.clone())
            .unwrap_or_default()
    }

    /// A tool call as a chosen caller (gate G1). The kit stands in for the
    /// Hub's verified-caller path: the Hub sets `request.caller` in the
    /// invocation context, exactly as it does for a verified session, and the
    /// plugin cannot forge it. The kit does not verify credentials; a token
    /// check needs the credential path (the collab writer's step).
    pub fn call_tool_as(
        &mut self,
        caller: KitCaller,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<DaemonResponse, KitError> {
        let caller = match caller {
            KitCaller::Operator => crate::plugin_caller::PluginCaller::Operator,
            KitCaller::Session { hub_id, session_id } => {
                crate::plugin_caller::PluginCaller::Session {
                    hub_id: hub_id.unwrap_or_else(|| self.hub_id()),
                    session_id,
                }
            }
        };
        self.state.plugin_controls.kit_caller = Some(caller);
        let response = self.call_tool(name, arguments);
        self.state.plugin_controls.kit_caller = None;
        response
    }

    /// Events delivered to the kit observers. Each item is
    /// `{ owner, name, payload }`. Events are in delivery order within one
    /// producer package; producers appear in the order they were enabled.
    pub fn emitted_events(&self) -> Result<Vec<serde_json::Value>, KitError> {
        let mut events = Vec::new();
        for observer in &self.observers {
            let records = self.plugin_db(observer)?;
            events.extend(
                records
                    .get(OBSERVED_EVENTS_KEY)
                    .and_then(|payload| payload.get("items"))
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .cloned(),
            );
        }
        Ok(events)
    }

    /// Call one plugin MCP tool, as `botster mcp-serve` does.
    pub fn call_tool(
        &mut self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<DaemonResponse, KitError> {
        self.request(DaemonRequest::PluginMcpCallTool {
            name: name.to_string(),
            arguments,
        })
    }

    /// Install a complete session baseline. The production baseline consumer
    /// seals it and starts each session_family consumer's snapshot.
    pub fn sessions_baseline(
        &mut self,
        sessions: Vec<SessionLifecycleRecord>,
    ) -> Result<(), KitError> {
        let snapshot = self.next_cursor();
        let page = SessionLifecycleBaselinePage {
            snapshot_sequence: snapshot,
            sessions,
            next: None,
            stop: botster_core_daemon::LifecycleBaselineStop::Complete,
        };
        if let Some(runtime) = self.daemon.runtime() {
            crate::daemon_maintenance::supply_baseline_page(
                runtime,
                &mut self.state.maintenance,
                Ok(page),
            );
        }
        self.settle()
    }

    /// Create or replace one session row, as a Core journal upsert.
    pub fn session_upsert(&mut self, record: SessionLifecycleRecord) -> Result<(), KitError> {
        self.supply_change(SessionLifecycleChangeKind::Upsert { record })
    }

    /// Forget one session row, as a Core journal removal.
    pub fn session_remove(&mut self, session_id: &str) -> Result<(), KitError> {
        self.supply_change(SessionLifecycleChangeKind::Removed {
            session_id: SessionId(session_id.to_string()),
        })
    }

    /// Drive the owner until it settles.
    pub fn settle(&mut self) -> Result<(), KitError> {
        self.settle_with(|_| true)
    }

    /// The daemon under test.
    #[must_use]
    pub fn daemon(&self) -> &HubDaemon {
        &self.daemon
    }

    fn supply_change(&mut self, kind: SessionLifecycleChangeKind) -> Result<(), KitError> {
        let cursor = self.next_cursor();
        let page = SessionLifecyclePage {
            changes: vec![SessionLifecycleChange {
                cursor: cursor.clone(),
                kind,
            }],
            next: cursor.clone(),
            source_watermark: cursor,
            resync_required: None,
        };
        crate::daemon_maintenance::supply_journal_page(&mut self.state.maintenance, Ok(page));
        self.settle()
    }

    fn next_cursor(&mut self) -> SessionLifecycleCursor {
        self.lifecycle_sequence += 1;
        SessionLifecycleCursor {
            source_id: SessionLifecycleSourceId(KIT_LIFECYCLE_SOURCE.to_string()),
            sequence: self.lifecycle_sequence,
        }
    }

    fn submit(&mut self, request: DaemonRequest) -> ControlReplyReceiver {
        self.next_request += 1;
        let (reply_tx, reply_rx) = crate::daemon::control::message::control_reply_channel();
        crate::daemon::control::request::handle(
            &mut self.daemon,
            &mut self.state,
            self.transport.handle(),
            self.control_tx.clone(),
            ControlMessage::Request {
                request: Box::new(request),
                transport_request_id: Some(format!("plugin-test-kit-{}", self.next_request)),
                reply_tx,
                response_delivery_rx: None,
                grant_id: None,
                client_id: Some("plugin-test-kit".to_string()),
            },
        );
        reply_rx
    }

    /// Run owner turns while work is ready; otherwise block on the next
    /// owner wake. Return when `done` holds and the owner is idle.
    ///
    /// Every idle judgment directly follows a readiness publish, as the
    /// owner publishes before it sleeps, so work that a turn made ready is
    /// never mistaken for idleness.
    fn settle_with(&mut self, mut done: impl FnMut(&mut Self) -> bool) -> Result<(), KitError> {
        let deadline = Instant::now() + self.step_deadline;
        loop {
            if Instant::now() >= deadline {
                return Err(self.not_settled());
            }
            publish_test_readiness(&self.daemon, &mut self.state);
            self.drain_entity_subscriptions()?;
            if !self.state.owner_ready.is_empty() {
                if drive_ready_test_turn(&mut self.daemon, &mut self.state) {
                    return Err(KitError::Shutdown);
                }
                continue;
            }
            if done(self) && self.idle() {
                return Ok(());
            }
            if !self.wait_for_wake(deadline) {
                return Err(self.not_settled());
            }
        }
    }

    /// Block where the daemon owner would: on the next wake message or the
    /// owner doorbell. Returns false at `deadline`.
    fn wait_for_wake(&mut self, deadline: Instant) -> bool {
        let Some(runtime) = self.daemon.runtime() else {
            return false;
        };
        let signal = Arc::clone(runtime.owner_signal());
        let wake_rx = &mut self.wake_rx;
        let wake = self.waiter.block_on(async {
            tokio::select! {
                biased;
                message = wake_rx.recv() => Some(message.as_ref().map_or("closed", wake_kind)),
                () = signal.rung() => Some("doorbell"),
                // timer: deadline — a kit step did not settle; the step fails with not_settled.
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => None,
            }
        });
        self.note_wake(wake)
    }

    fn note_wake(&mut self, wake: Option<&'static str>) -> bool {
        let Some(kind) = wake else {
            return false;
        };
        self.wake_count += 1;
        if self.recent_wakes.len() == RECENT_WAKES {
            self.recent_wakes.pop_front();
        }
        self.recent_wakes.push_back(kind);
        true
    }

    /// Move every delivered entity frame into the kit's record, as a client
    /// transport would. Receiving releases the subscription's queue slot.
    fn drain_entity_subscriptions(&mut self) -> Result<(), KitError> {
        for subscription in &mut self.entity_subscriptions {
            while let Ok(delivery) = subscription.receiver.try_recv() {
                let frame = match delivery {
                    crate::entity_delivery::EntityDelivery::Typed(frame) => {
                        serde_json::to_value(frame)
                    }
                    crate::entity_delivery::EntityDelivery::Encoded(prepared) => {
                        serde_json::from_slice(prepared.json())
                    }
                }
                .map_err(|error| KitError::Daemon(format!("entity frame: {error}")))?;
                subscription.frames.push(frame);
            }
        }
        Ok(())
    }

    /// No request, invocation, or owner work remains.
    fn idle(&mut self) -> bool {
        self.control_rx.is_empty()
            && self.state.owner_ready.is_empty()
            && self.pending_work().is_empty()
    }

    /// Every kind of retained owner work that is still open, by name.
    fn pending_work(&self) -> Vec<&'static str> {
        let maintenance = &self.state.maintenance;
        let mut pending = Vec::new();
        let mut note = |open: bool, name: &'static str| {
            if open {
                pending.push(name);
            }
        };
        note(!self.state.pending_requests.is_empty(), "pending_requests");
        note(self.state.plugin_controls.has_pending(), "plugin_controls");
        note(!maintenance.event_in_flight.is_empty(), "event_in_flight");
        note(
            !maintenance.pending_retirements.is_empty(),
            "pending_retirements",
        );
        note(maintenance.needs_work(), "maintenance");
        note(
            crate::daemon::control::entities::plugin_entity_cleanup_pending(&self.state),
            "plugin_entity_cleanup",
        );
        note(self.state.budget.outstanding() != 0, "owner_budget");
        if let Some(runtime) = self.daemon.runtime() {
            note(runtime.package_entity_work_pending(), "package_entity_work");
            note(
                runtime.entity_publish_bridge().pending_publish_count() != 0,
                "entity_publish",
            );
            note(
                runtime.entity_publish_retirement_pending(),
                "entity_publish_retirement",
            );
            note(runtime.host_executor().outstanding() != 0, "host_executor");
            // Copies queued in the router are owner work even before the
            // delivery slice sees their readiness.
            note(
                runtime
                    .package_event_router()
                    .snapshot()
                    .map_or(true, |router| {
                        router.queued_holders != 0
                            || router.admitted_holders != 0
                            || router.global_in_flight_bytes != 0
                    }),
                "event_router",
            );
        }
        pending
    }

    fn not_settled(&self) -> KitError {
        KitError::NotSettled {
            pending: format!(
                "open={:?} owner_ready_empty={} event_in_flight={:?} control_messages={} \
                 wakes={} recent_wakes={:?}",
                self.pending_work(),
                self.state.owner_ready.is_empty(),
                self.state
                    .maintenance
                    .event_in_flight
                    .keys()
                    .collect::<Vec<_>>(),
                self.control_rx.len(),
                self.wake_count,
                self.recent_wakes,
            ),
        }
    }
}

impl Drop for KitHub {
    fn drop(&mut self) {
        let _ = self.daemon.stop();
        if self.remove_root {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

/// Holds the next invocation of one plugin handler until it is released or
/// dropped. The handler then runs normally. Only one hold exists at a time
/// in a process; a second `hold_handler` waits for the first to drop.
///
/// The hold is process-wide: it matches the handler id in any kit daemon of
/// this process. Put a test that holds a handler in its own test target, or
/// run no other kit that invokes the same handler meanwhile.
pub struct HandlerHold {
    _exclusive: std::sync::MutexGuard<'static, ()>,
}

impl HandlerHold {
    /// Let the held invocation run.
    pub fn release(self) {}

    /// How long the hold waits before it fails the invocation. It is `None`:
    /// only a release lets the handler run.
    #[must_use]
    pub fn expiry(&self) -> Option<Duration> {
        crate::lua_runtime::test_plugin_invocation_gate_deadline()
    }
}

impl Drop for HandlerHold {
    fn drop(&mut self) {
        crate::lua_runtime::release_test_plugin_invocation_gate();
    }
}

/// Hold the next invocation of `handler_id` (for example
/// `event:<owner>:<name>:<n>`) of `package_name` inside the real Lua runtime.
///
/// A step whose chain reaches the held handler cannot settle while the
/// hold exists, so tests can prove what a step waits for.
#[must_use]
pub fn hold_handler(package_name: &str, handler_id: &str) -> HandlerHold {
    static EXCLUSIVE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let exclusive = EXCLUSIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    crate::lua_runtime::arm_test_plugin_invocation_gate_for(
        Some(package_name),
        handler_id,
        // Only a release runs the handler. The step deadline bounds a step
        // that waits on the hold; the hold cannot expire into a settle.
        None,
    );
    HandlerHold {
        _exclusive: exclusive,
    }
}

fn wake_kind(message: &ControlMessage) -> &'static str {
    match message {
        ControlMessage::PluginCompletionPublished => "plugin_completion",
        ControlMessage::HostProgressPublished => "host_progress",
        ControlMessage::CausalProgressPublished => "causal_progress",
        ControlMessage::EntityPublishProgress => "entity_publish",
        ControlMessage::CoordinationProgress => "coordination",
        ControlMessage::CoreCompletionPublished => "core_completion",
        ControlMessage::DataPlaneProgress => "data_plane",
        ControlMessage::EntitySubscriptionCapacityReleased => "entity_capacity",
        ControlMessage::PluginResultCapacityReleased => "plugin_result_capacity",
        ControlMessage::ManagedSessionSpawnQueued => "managed_spawn",
        _ => "other",
    }
}

/// Build one session record for kit lifecycle input.
#[must_use]
pub fn session_record(
    session_id: &str,
    registry_state: RegistrySessionState,
    lifecycle: Option<SessionLifecycleState>,
) -> SessionLifecycleRecord {
    SessionLifecycleRecord {
        session: DaemonSession {
            session_id: SessionId(session_id.to_string()),
            registry_state,
            size: botster_core::ResizePayload { rows: 24, cols: 80 },
            process: None,
            updated_at: 0,
        },
        metadata: Default::default(),
        lifecycle,
    }
}

fn package_name_of(package: &Path) -> Result<String, KitError> {
    let manifest_path = package.join(crate::packages::LOCAL_PACKAGE_MANIFEST_FILE);
    let bytes = std::fs::read(&manifest_path)
        .map_err(|error| KitError::Package(format!("{}: {error}", manifest_path.display())))?;
    let manifest: crate::packages::HubPackageManifest = serde_json::from_slice(&bytes)
        .map_err(|error| KitError::Package(format!("{}: {error}", manifest_path.display())))?;
    Ok(manifest.name)
}

/// The `(owner, name)` of each event the package declares for plugins.
fn observable_events(package: &Path) -> Result<Vec<(String, String)>, KitError> {
    let manifest_path = package.join(crate::packages::LOCAL_PACKAGE_MANIFEST_FILE);
    let bytes = std::fs::read(&manifest_path)
        .map_err(|error| KitError::Package(format!("{}: {error}", manifest_path.display())))?;
    let manifest: crate::packages::HubPackageManifest = serde_json::from_slice(&bytes)
        .map_err(|error| KitError::Package(format!("{}: {error}", manifest_path.display())))?;
    Ok(manifest
        .events
        .emitted
        .iter()
        .filter(|event| event.audience.iter().any(|audience| audience == "plugins"))
        .map(|event| (manifest.name.clone(), event.name.clone()))
        .collect())
}

/// Write the observer package under the kit root. It subscribes to each
/// observed event and appends `{ owner, name, payload }` to its plugin_db.
fn write_observer_package(
    root: &Path,
    name: &str,
    events: &[(String, String)],
) -> Result<PathBuf, KitError> {
    let directory = root.join(name);
    std::fs::create_dir_all(&directory)
        .map_err(|error| KitError::Package(format!("{}: {error}", directory.display())))?;
    let manifest = serde_json::json!({
        "name": name,
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "description": "Plugin test kit observer: records delivered events.",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "plugin_db", "scope": name }],
        "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
    });
    let mut lua = String::from(OBSERVER_LUA_PRELUDE);
    for (owner, name) in events {
        let owner =
            serde_json::to_string(owner).map_err(|error| KitError::Package(error.to_string()))?;
        let name =
            serde_json::to_string(name).map_err(|error| KitError::Package(error.to_string()))?;
        lua.push_str(&format!("observe({owner}, {name})\n"));
    }
    lua.push_str("return botster.register({})\n");
    std::fs::write(
        directory.join(crate::packages::LOCAL_PACKAGE_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| KitError::Package(error.to_string()))?,
    )
    .map_err(|error| KitError::Package(error.to_string()))?;
    std::fs::write(directory.join("plugin.lua"), lua)
        .map_err(|error| KitError::Package(error.to_string()))?;
    Ok(directory)
}

/// The package the kit enables to read the sandbox's global names.
const GLOBALS_PROBE_PACKAGE: &str = "botster-plugin-test-kit-globals";

/// Write the probe package under the kit root. Its one tool lists the names in
/// its own global table, which is the table every plugin reads.
fn write_globals_probe_package(root: &Path) -> Result<PathBuf, KitError> {
    let directory = root.join(GLOBALS_PROBE_PACKAGE);
    std::fs::create_dir_all(&directory)
        .map_err(|error| KitError::Package(format!("{}: {error}", directory.display())))?;
    let manifest = serde_json::json!({
        "name": GLOBALS_PROBE_PACKAGE,
        "version": "1.0.0",
        "kind": "plugin",
        "botster": ">=0.1.0",
        "description": "Plugin test kit probe: lists the sandbox's global names.",
        "source": { "type": "path", "path": "." },
        "capabilities": [{ "surface": "mcp" }],
        "entrypoints": [{ "runtime": "lua", "path": "plugin.lua", "bootstrap": false }]
    });
    let lua = format!(
        r#"return botster.register({{ tools = {{ {{
  name = "{GLOBALS_PROBE_PACKAGE}.list",
  description = "List the global names of the sandbox.",
  input_schema = {{ type = "object" }},
  handler = "list",
  call = function()
    local names = {{}}
    for name in pairs(_G) do
      names[#names + 1] = tostring(name)
    end
    table.sort(names)
    return {{ names = names }}
  end,
}} }} }})
"#
    );
    std::fs::write(
        directory.join(crate::packages::LOCAL_PACKAGE_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)
            .map_err(|error| KitError::Package(error.to_string()))?,
    )
    .map_err(|error| KitError::Package(error.to_string()))?;
    std::fs::write(directory.join("plugin.lua"), lua)
        .map_err(|error| KitError::Package(error.to_string()))?;
    Ok(directory)
}

fn kit_config(data_directory: &Path) -> Result<crate::HubConfig, KitError> {
    crate::HubStartupOptions {
        host: crate::HostIdentityOptions {
            id: "plugin-test-kit".to_string(),
            display_name: "Plugin Test Kit".to_string(),
            fingerprint: None,
        },
        data_directory: crate::DataDirectoryOption::Explicit(data_directory.to_path_buf()),
        session_defaults: crate::SessionDefaults {
            shell: "/bin/sh".to_string(),
            working_directory: Some(".".into()),
            initial_rows: 24,
            initial_cols: 80,
        },
        ..crate::HubStartupOptions::default()
    }
    .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
    .map_err(|error| KitError::Start(error.to_string()))
}
