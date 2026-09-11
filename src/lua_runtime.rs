//! Minimal safe Lua plugin runtime behind the core `PluginRuntime` boundary.
//!
//! This module intentionally exposes a narrow ABI: plugin registration,
//! handler invocation by stable id, and selected hub capability helpers.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use botster_core::{
    BoundaryJson, CapabilityOperation, CapabilityOperationId, CapabilityRuntimeErrorKind,
    CapabilityRuntimeRequest, EndpointId, EntityContract, EntityKind, EnvelopeCursor, EnvelopeId,
    EnvelopeTarget, PluginCancellationToken, PluginCapabilityRuntime, PluginDescriptorKind,
    PluginDescriptorRef, PluginHandlerKind, PluginHandlerRef, PluginHandlerRegistration,
    PluginInvocationFailure, PluginInvocationFailureKind, PluginInvocationRequest,
    PluginInvocationResult, PluginInvocationSuccess, PluginKey, PluginOwnedDescriptor,
    PluginResourceKind, PluginResourceRef, PluginRuntime, PluginStoreCapabilityRequest,
    PluginStoreKey, PluginStoreOperation, RoutedEnvelope, RoutedEnvelopeDrainOutcome,
    RoutedEnvelopePayload, RoutedEnvelopePublishOutcome, TimerCapabilityRequest,
};
use mlua::{Function, HookTriggers, Lua, LuaOptions, LuaSerdeExt, StdLib, Table, Value, VmState};
use serde_json::json;

use crate::capabilities::{HubCapabilityRuntime, PluginStoreBatchMutation, PluginStoreBatchResult};
use crate::lifecycle::{
    HubPluginEventHandler, HubPluginRuntimeBundle, PACKAGE_EVENT_INVOCATION_ORIGIN,
    SESSION_FAMILY_INVOCATION_ORIGIN, package_entity_owner_token,
};
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount, LuaVmCharge};
use crate::package_event_router::{CausalScopeTable, EventPlaneStatus, PackageEventRouter};
use crate::packages::{PackageConfigurationView, PackageRecord, PreparedLocalPackage};
use crate::runtime::{SharedSessionTypeSpawner, SharedSpawnTargets, SharedWorktrees};

mod acknowledge_input;
mod sandbox;
pub(crate) use acknowledge_input::ownership::{
    CoordinationDelivery, CoordinationFailure, CoordinationOutcome as HubCoordinationResponse,
    CoordinationRefusal, CoordinationReply, CoordinationReplySender, CoordinationStorage,
};
use acknowledge_input::{AcknowledgeInput, AcknowledgeOperation};

#[cfg(feature = "allocation-oracle")]
pub(crate) use acknowledge_input::ownership::{AcknowledgeOutcome, reply_channel};

thread_local! {
    static INVOCATION_CAUSAL_SCOPE: Cell<Option<u64>> = const { Cell::new(None) };
}

pub(crate) fn current_causal_scope() -> Option<u64> {
    INVOCATION_CAUSAL_SCOPE.with(Cell::get)
}

fn set_current_causal_scope(scope_id: Option<u64>) {
    INVOCATION_CAUSAL_SCOPE.with(|cell| cell.set(scope_id));
}
use crate::session_types::{
    ManagedSessionTypeRequest, SessionTypeContextInput, SessionTypeRequest,
};

const DEFAULT_INSTRUCTION_BUDGET: u64 = 500_000;
const INSTRUCTION_BUDGET_ERROR: &str = "lua instruction budget exceeded";
pub(crate) const LUA_CALLBACK_CAPACITY_EXHAUSTED: &str = "Lua callback memory capacity exhausted";

#[derive(Debug)]
struct InstructionBudgetExceeded;

impl fmt::Display for InstructionBudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(INSTRUCTION_BUDGET_ERROR)
    }
}

impl Error for InstructionBudgetExceeded {}

const COORDINATION_REQUEST_TIMEOUT_MS: u64 = 1_000;
const ENTITY_PUBLISH_REQUEST_TIMEOUT_MS: u64 = 1_000;
/// Shared host capability runtime used by Lua capability helpers.
pub type SharedHubCapabilityRuntime = Arc<Mutex<HubCapabilityRuntime>>;

/// Narrow CoreDaemon-backed coordination bridge exposed to Lua helpers.
#[derive(Clone)]
pub struct HubCoordinationBridge {
    owner_thread: thread::ThreadId,
    pending: Arc<
        Mutex<crate::lua_memory::charged_collection::ChargedVecDeque<PendingCoordinationRequest>>,
    >,
    progress: Arc<CoordinationProgress>,
    account: Arc<LuaMemoryAccount>,
}

struct CoordinationProgress {
    pending: AtomicBool,
    sealed: AtomicBool,
    owner: std::sync::OnceLock<crate::daemon::control::message::ControlSender>,
    #[cfg(test)]
    admitted: Mutex<Vec<crate::owner_identity::WaiterId>>,
}

#[allow(clippy::large_enum_variant)] // transient owner poll return; Ready moves the queued request out by value
pub(crate) enum CoordinationIngressPoll {
    Ready(PendingCoordinationRequest),
    Empty,
    Contended,
    Poisoned,
}

impl CoordinationProgress {
    fn publish(&self) {
        if !self.pending.swap(true, Ordering::AcqRel)
            && let Some(owner) = self.owner.get()
        {
            let _ = owner
                .try_send(crate::daemon::control::message::ControlMessage::CoordinationProgress);
        }
    }
}

struct CoordinationUnlock<'a>(&'a CoordinationProgress);

impl Drop for CoordinationUnlock<'_> {
    fn drop(&mut self) {
        self.0.publish();
    }
}

#[derive(Clone)]
pub(crate) enum CoordinationCaller {
    NonAcknowledge(Arc<std::sync::atomic::AtomicU8>),
    Acknowledge(acknowledge_input::ownership::AcknowledgeCaller),
}

impl CoordinationCaller {
    fn new() -> Self {
        Self::NonAcknowledge(Arc::new(std::sync::atomic::AtomicU8::new(0)))
    }

    pub(crate) fn claim(&self) -> bool {
        match self {
            Self::NonAcknowledge(state) => state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            Self::Acknowledge(caller) => caller.claim(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_bits(&self) -> u8 {
        match self {
            Self::NonAcknowledge(state) => state.load(Ordering::Acquire),
            Self::Acknowledge(caller) => caller.test_bits(),
        }
    }
}

struct CoordinationCallerGuard(CoordinationCaller);

impl Drop for CoordinationCallerGuard {
    fn drop(&mut self) {
        match &self.0 {
            CoordinationCaller::NonAcknowledge(state) => {
                state.fetch_or(2, Ordering::AcqRel);
            }
            CoordinationCaller::Acknowledge(caller) => caller.finish(),
        }
    }
}

mod callback;
mod entity_publish;
pub(crate) mod lua_json;
#[cfg(test)]
mod registration_tests;
mod session_type_spawn;
use entity_publish::EntityPublishError;
pub(crate) use entity_publish::PendingEntityPublishRequest;
pub use entity_publish::{EntityPublishPermit, HubEntityPublishBridge};

#[derive(Debug, Clone, Copy)]
enum CoordinationLocalError {
    OwnerThread,
    Timeout,
    Unexpected,
    QueuePoisoned,
    IngressSealed,
    Capacity,
}

impl CoordinationLocalError {
    fn as_str(self) -> &'static str {
        match self {
            Self::OwnerThread => {
                "botster.coordination is only available during handler invocation, not at plugin load"
            }
            Self::Timeout => "coordination request did not complete before timeout",
            Self::Unexpected => "coordination acknowledge returned unexpected response",
            Self::QueuePoisoned => "coordination queue lock poisoned",
            Self::IngressSealed => "coordination ingress is sealed",
            Self::Capacity => LUA_CALLBACK_CAPACITY_EXHAUSTED,
        }
    }
}

#[derive(Debug)]
enum CoordinationRequestError {
    Local(CoordinationLocalError),
    Response(CoordinationFailure),
}

impl CoordinationRequestError {
    fn as_str(&self) -> &str {
        match self {
            Self::Local(error) => error.as_str(),
            Self::Response(failure) => failure.message(),
        }
    }
}

impl HubCoordinationBridge {
    pub(crate) fn new(account: Arc<LuaMemoryAccount>) -> Self {
        Self {
            owner_thread: thread::current().id(),
            pending: Arc::new(Mutex::new(
                crate::lua_memory::charged_collection::ChargedVecDeque::new(Arc::clone(&account)),
            )),
            progress: Arc::new(CoordinationProgress {
                pending: AtomicBool::new(false),
                sealed: AtomicBool::new(false),
                owner: std::sync::OnceLock::new(),
                #[cfg(test)]
                admitted: Mutex::new(Vec::new()),
            }),
            account,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_new() -> Self {
        Self::new(
            crate::lua_memory::LuaMemoryAccount::new(crate::config::lua_memory_limits()).unwrap(),
        )
    }

    fn acknowledge(
        &self,
        input: AcknowledgeInput,
    ) -> Result<acknowledge_input::ownership::AcknowledgeOutcome, CoordinationRequestError> {
        if thread::current().id() == self.owner_thread {
            return Err(CoordinationRequestError::Local(
                CoordinationLocalError::OwnerThread,
            ));
        }
        let AcknowledgeInput { input, transport } = input;
        let (sender, receiver) = acknowledge_input::ownership::reply_channel(transport.reply);
        let caller = CoordinationCaller::Acknowledge(
            acknowledge_input::ownership::AcknowledgeCaller::new(transport.caller),
        );
        let _caller = CoordinationCallerGuard(caller.clone());
        self.enqueue(PendingCoordinationRequest {
            #[cfg(test)]
            terminal_drop_probe: None,
            operation: PendingCoordinationOperation::Acknowledge { input },
            response: CoordinationReplySender::Acknowledge {
                sender,
                error: transport.error,
                conversion: transport.conversion,
            },
            caller,
            storage: Some(transport.work),
            entry: None,
        })?;
        // First blocking wait on this thread may allocate std mpmc Context once
        // (rust 1.97.0 library/std/src/sync/mpmc/context.rs:41-44, :67-77).
        let response = receiver
            .recv_timeout(Duration::from_millis(COORDINATION_REQUEST_TIMEOUT_MS))
            .map_err(|_| CoordinationRequestError::Local(CoordinationLocalError::Timeout))?
            .map_err(CoordinationRequestError::Response)?;
        match response {
            HubCoordinationResponse::Acknowledge(outcome) => Ok(outcome),
            _ => Err(CoordinationRequestError::Local(
                CoordinationLocalError::Unexpected,
            )),
        }
    }

    fn request(
        &self,
        operation: PendingCoordinationOperation,
    ) -> Result<HubCoordinationResponse, String> {
        self.request_typed(operation).map_err(|error| match error {
            CoordinationRequestError::Local(error) => error.as_str().to_owned(),
            CoordinationRequestError::Response(CoordinationFailure::NonAcknowledge(message)) => {
                message
            }
            CoordinationRequestError::Response(CoordinationFailure::Acknowledge(_)) => {
                CoordinationRefusal::Unexpected.message().to_owned()
            }
        })
    }

    fn request_typed(
        &self,
        operation: PendingCoordinationOperation,
    ) -> Result<HubCoordinationResponse, CoordinationRequestError> {
        if thread::current().id() == self.owner_thread {
            return Err(CoordinationRequestError::Local(
                CoordinationLocalError::OwnerThread,
            ));
        }

        let bytes = nonacknowledge_entry_bytes(&operation).ok_or(
            CoordinationRequestError::Local(CoordinationLocalError::Capacity),
        )?;
        let entry = self
            .account
            .reserve_callback_total(bytes)
            .map_err(|_| CoordinationRequestError::Local(CoordinationLocalError::Capacity))?;
        self.submit_admitted(operation, entry)
    }

    fn submit_admitted(
        &self,
        operation: PendingCoordinationOperation,
        entry: LuaCallbackCharge,
    ) -> Result<HubCoordinationResponse, CoordinationRequestError> {
        if thread::current().id() == self.owner_thread {
            return Err(CoordinationRequestError::Local(
                CoordinationLocalError::OwnerThread,
            ));
        }
        let caller = CoordinationCaller::new();
        let _caller = CoordinationCallerGuard(caller.clone());
        let (response, receiver) = mpsc::sync_channel(1);
        self.enqueue(PendingCoordinationRequest {
            #[cfg(test)]
            terminal_drop_probe: None,
            operation,
            response: CoordinationReplySender::NonAcknowledge(response),
            caller,
            storage: None,
            entry: Some(entry),
        })?;
        receiver
            .recv_timeout(Duration::from_millis(COORDINATION_REQUEST_TIMEOUT_MS))
            .map_err(|_| CoordinationRequestError::Local(CoordinationLocalError::Timeout))?
            .map_err(CoordinationRequestError::Response)
    }

    fn enqueue(&self, request: PendingCoordinationRequest) -> Result<(), CoordinationRequestError> {
        let _unlock = CoordinationUnlock(&self.progress);
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| CoordinationRequestError::Local(CoordinationLocalError::QueuePoisoned))?;
        if self.progress.sealed.load(Ordering::Acquire) {
            return Err(CoordinationRequestError::Local(
                CoordinationLocalError::IngressSealed,
            ));
        }
        pending
            .try_push_back(request)
            .map_err(|_| CoordinationRequestError::Local(CoordinationLocalError::Capacity))?;
        Ok(())
    }

    pub(crate) fn take_pending(&self) -> Option<PendingCoordinationRequest> {
        if self.progress.owner.get().is_some() {
            return None;
        }
        self.pending
            .lock()
            .expect("coordination queue lock")
            .pop_front()
    }

    pub(crate) fn bind_owner_wake(&self, owner: crate::daemon::control::message::ControlSender) {
        if let Err(owner) = self.progress.owner.set(owner) {
            assert!(self.progress.owner.get().unwrap().same_channel(&owner));
        }
        if self.progress.pending.load(Ordering::Acquire) {
            let _ =
                self.progress.owner.get().unwrap().try_send(
                    crate::daemon::control::message::ControlMessage::CoordinationProgress,
                );
        }
    }

    pub(crate) fn take_progress_notification(&self) -> bool {
        self.progress.pending.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_pending_for_owner(&self) -> CoordinationIngressPoll {
        let mut pending = match self.pending.try_lock() {
            Ok(pending) => pending,
            Err(std::sync::TryLockError::WouldBlock) => return CoordinationIngressPoll::Contended,
            Err(std::sync::TryLockError::Poisoned(_)) => return CoordinationIngressPoll::Poisoned,
        };
        let request = pending.pop_front();
        let remaining = !pending.is_empty();
        drop(pending);
        if remaining {
            self.progress.publish();
        }
        request.map_or(
            CoordinationIngressPoll::Empty,
            CoordinationIngressPoll::Ready,
        )
    }

    /// Host-only terminal cleanup, after the engine receipt seals all producers.
    /// Poison cannot invalidate ownership of the actual queue entries: terminal
    /// cleanup extracts the container without interpreting normal queue state.
    /// A destructor panic propagates to the enclosing Host disposal receipt.
    pub(crate) fn dispose_terminal_pending(&self) -> bool {
        let pending = {
            let mut queue = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.progress.sealed.store(true, Ordering::Release);
            queue.take()
        };
        drop(pending);
        true
    }

    #[cfg(test)]
    pub(crate) fn test_queue_pending(
        &self,
        operation: PendingCoordinationOperation,
    ) -> mpsc::Receiver<CoordinationReply> {
        let (response, receiver) = mpsc::sync_channel(1);
        self.pending
            .lock()
            .unwrap()
            .try_push_back(PendingCoordinationRequest {
                terminal_drop_probe: None,
                operation,
                response: CoordinationReplySender::NonAcknowledge(response),
                caller: CoordinationCaller::new(),
                storage: None,
                entry: None,
            })
            .expect("test queue capacity");
        receiver
    }

    #[cfg(test)]
    pub(crate) fn test_pending_count(&self) -> usize {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    pub(crate) fn test_pending_capacity(&self) -> usize {
        self.pending.lock().unwrap().capacity()
    }

    #[cfg(test)]
    pub(crate) fn test_pending_charge_bytes(&self) -> usize {
        self.pending.lock().unwrap().charge_bytes()
    }

    #[cfg(test)]
    pub(crate) fn test_set_charge_after_grow(&self, enabled: bool) {
        self.pending.lock().unwrap().set_charge_after_grow(enabled);
    }

    #[cfg(test)]
    pub(crate) fn test_set_release_capacity_on_pop(&self, enabled: bool) {
        self.pending
            .lock()
            .unwrap()
            .set_release_capacity_on_pop(enabled);
    }

    #[cfg(test)]
    pub(crate) fn test_set_skip_capacity_check(&self, enabled: bool) {
        self.pending
            .lock()
            .unwrap()
            .set_skip_capacity_check(enabled);
    }

    #[cfg(test)]
    pub(crate) fn test_set_always_grow(&self, enabled: bool) {
        self.pending.lock().unwrap().set_always_grow(enabled);
    }

    #[cfg(test)]
    pub(crate) fn test_set_take_without_charge(&self, enabled: bool) {
        self.pending
            .lock()
            .unwrap()
            .set_take_without_charge(enabled);
    }

    #[cfg(test)]
    pub(crate) fn test_note_core_admission(&self, waiter_id: crate::owner_identity::WaiterId) {
        self.progress.admitted.lock().unwrap().push(waiter_id);
    }

    #[cfg(test)]
    pub(crate) fn test_admitted_waiters(&self) -> Vec<crate::owner_identity::WaiterId> {
        self.progress.admitted.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn test_set_pending_drop_probe(&self, probe: impl Send + 'static) {
        let mut queue = self.pending.lock().unwrap();
        let head = queue
            .front_mut()
            .expect("a real pending coordination request");
        assert!(head.terminal_drop_probe.is_none());
        head.terminal_drop_probe = Some(Box::new(probe));
    }
}

pub(crate) struct PendingCoordinationRequest {
    // Drop before actual payload fields so terminal tests can pause destruction.
    #[cfg(test)]
    pub(crate) terminal_drop_probe: Option<Box<dyn Send>>,
    pub(crate) operation: PendingCoordinationOperation,
    pub(crate) response: CoordinationReplySender,
    pub(crate) caller: CoordinationCaller,
    pub(crate) storage: Option<CoordinationStorage>,
    pub(crate) entry: Option<LuaCallbackCharge>,
}

pub(crate) enum PendingCoordinationOperation {
    #[cfg(test)]
    Tracked {
        operation: Box<PendingCoordinationOperation>,
        probe: Box<dyn Send>,
        executions: Arc<std::sync::atomic::AtomicUsize>,
    },
    Publish {
        envelope: RoutedEnvelope,
    },
    Drain {
        target: EnvelopeTarget,
        after: Option<EnvelopeCursor>,
        limit: usize,
    },
    Acknowledge {
        input: AcknowledgeOperation,
    },
}

impl PendingCoordinationOperation {
    pub(crate) fn execute(self, daemon: &mut botster_core_daemon::CoreDaemon) -> CoordinationReply {
        match self {
            #[cfg(test)]
            Self::Tracked {
                operation,
                probe,
                executions,
            } => {
                executions.fetch_add(1, Ordering::AcqRel);
                let result = operation.execute(daemon);
                drop(probe);
                return result;
            }
            Self::Publish { envelope } => daemon
                .publish_routed_envelope(botster_core_daemon::PublishRoutedEnvelopeRequest {
                    envelope,
                })
                .map(HubCoordinationResponse::Publish)
                .map_err(|error| CoordinationFailure::NonAcknowledge(error.to_string())),
            Self::Drain {
                target,
                after,
                limit,
            } => daemon
                .drain_routed_envelopes(botster_core_daemon::DrainRoutedEnvelopesRequest {
                    target,
                    after,
                    limit,
                })
                .map(HubCoordinationResponse::Drain)
                .map_err(|error| CoordinationFailure::NonAcknowledge(error.to_string())),
            Self::Acknowledge { input } => input
                .acknowledge(daemon)
                .map(HubCoordinationResponse::Acknowledge)
                .map_err(CoordinationFailure::Acknowledge),
        }
    }
}

fn nonacknowledge_entry_bytes(operation: &PendingCoordinationOperation) -> Option<usize> {
    let caller = crate::lua_memory::layout::arc_bytes::<std::sync::atomic::AtomicU8>();
    let reply = crate::lua_memory::layout::single_reply_bytes::<CoordinationReply>(true)?;
    operation
        .payload_bytes()?
        .checked_add(caller)?
        .checked_add(reply)
}

impl PendingCoordinationOperation {
    fn payload_bytes(&self) -> Option<usize> {
        match self {
            #[cfg(test)]
            Self::Tracked { operation, .. } => operation.payload_bytes(),
            Self::Publish { envelope } => routed_envelope_bytes(envelope),
            Self::Drain { target, after, .. } => drain_payload_bytes(target, after.as_ref()),
            Self::Acknowledge { .. } => Some(0),
        }
    }
}

fn drain_payload_bytes(target: &EnvelopeTarget, after: Option<&EnvelopeCursor>) -> Option<usize> {
    envelope_target_heap_bytes(target)?
        .checked_add(after.map_or(0, |_| std::mem::size_of::<EnvelopeCursor>()))
}

fn envelope_target_heap_bytes(target: &EnvelopeTarget) -> Option<usize> {
    Some(match target {
        EnvelopeTarget::Endpoint { endpoint_id } => endpoint_id.0.capacity(),
        EnvelopeTarget::Client { client_id } => client_id.0.capacity(),
        EnvelopeTarget::Session { session_id } => session_id.0.capacity(),
        EnvelopeTarget::Subscription {
            session_id,
            subscription_id,
        } => session_id
            .0
            .capacity()
            .checked_add(subscription_id.0.capacity())?,
        EnvelopeTarget::Plugin { plugin_key } => plugin_key.0.capacity(),
        EnvelopeTarget::Stream { stream } => stream.capacity(),
        EnvelopeTarget::Topic { topic } => topic.capacity(),
    })
}

fn routed_envelope_bytes(envelope: &RoutedEnvelope) -> Option<usize> {
    let mut bytes = envelope
        .id
        .0
        .capacity()
        .checked_add(envelope.source.0.capacity())?
        .checked_add(envelope.payload.content_type.capacity())?
        .checked_add(envelope.payload.body.capacity())?
        .checked_add(
            envelope
                .targets
                .capacity()
                .checked_mul(std::mem::size_of::<EnvelopeTarget>())?,
        )?;
    if let Some(extension) = &envelope.payload.extension {
        bytes = bytes.checked_add(lua_json::retained_bytes(&extension.0)?)?;
    }
    for target in &envelope.targets {
        bytes = bytes.checked_add(envelope_target_heap_bytes(target)?)?;
    }
    Some(bytes)
}

struct LuaHostApi {
    configuration: PackageConfigurationView,
    capabilities: SharedHubCapabilityRuntime,
    coordination: HubCoordinationBridge,
    entity_publish: HubEntityPublishBridge,
    session_types: SharedSessionTypeSpawner,
    spawn_targets: SharedSpawnTargets,
    worktrees: SharedWorktrees,
    package_records: Vec<PackageRecord>,
    package_event_router: Arc<PackageEventRouter>,
    causal_scopes: Arc<CausalScopeTable>,
    memory: Arc<LuaMemoryAccount>,
}

/// Validate the event name before the body without copying its Rust bytes.
struct EventName(mlua::String);

impl mlua::FromLua for EventName {
    fn from_lua(value: Value, lua: &Lua) -> mlua::Result<Self> {
        let from = value.type_name();
        let name =
            lua.coerce_string(value)?
                .ok_or_else(|| mlua::Error::FromLuaConversionError {
                    from,
                    to: "String".to_owned(),
                    message: Some("expected string or number".to_owned()),
                })?;
        std::str::from_utf8(&name.as_bytes()).map_err(|error| {
            mlua::Error::FromLuaConversionError {
                from: "string",
                to: "String".to_owned(),
                message: Some(error.to_string()),
            }
        })?;
        Ok(Self(name))
    }
}

#[cfg(test)]
mod event_name_tests {
    use super::*;

    #[test]
    fn borrowed_event_names_preserve_argument_conversion() {
        let lua = Lua::new();
        let old = lua.create_function(|_, _: String| Ok(())).unwrap();
        let borrowed = lua.create_function(|_, _: EventName| Ok(())).unwrap();
        for source in [
            "'event'",
            "''",
            "42",
            "1.25",
            "true",
            "nil",
            "{}",
            "function() end",
            "string.char(255)",
        ] {
            let value: Value = lua.load(format!("return {source}")).eval().unwrap();
            let previous = old.call::<()>(value.clone());
            let current = borrowed.call::<()>(value);
            match (previous, current) {
                (Ok(()), Ok(())) => {}
                (Err(previous), Err(current)) => {
                    assert_eq!(previous.to_string(), current.to_string(), "{source}");
                }
                _ => panic!("event name conversion changed for {source}"),
            }
        }
    }

    #[test]
    fn borrowed_event_name_keeps_the_original_lua_string() {
        use mlua::FromLua;

        let lua = Lua::new();
        let value = lua.create_string("event\0name").unwrap();
        let pointer = value.to_pointer();
        let name = EventName::from_lua(Value::String(value), &lua).unwrap();
        assert_eq!(name.0.to_pointer(), pointer);
        assert_eq!(&*name.0.to_str().unwrap(), "event\0name");
    }
}

/// Test-only hold applied before package-event handler invocations, in
/// milliseconds. Production code never sets it; it compiles out of release
/// builds. Tests use it to prove timeout classification deterministically.
#[cfg(test)]
pub(crate) static TEST_EVENT_HANDLER_HOLD_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
#[derive(Default)]
struct TestPluginInvocationGateState {
    armed: bool,
    entered: bool,
    released: bool,
}

#[cfg(test)]
fn test_plugin_invocation_gate() -> &'static (
    std::sync::Mutex<TestPluginInvocationGateState>,
    std::sync::Condvar,
) {
    static GATE: std::sync::OnceLock<(
        std::sync::Mutex<TestPluginInvocationGateState>,
        std::sync::Condvar,
    )> = std::sync::OnceLock::new();
    GATE.get_or_init(|| {
        (
            std::sync::Mutex::new(TestPluginInvocationGateState::default()),
            std::sync::Condvar::new(),
        )
    })
}

#[cfg(test)]
pub(crate) fn arm_test_plugin_invocation_gate() {
    let (lock, _) = test_plugin_invocation_gate();
    let mut state = lock.lock().expect("plugin invocation gate mutex");
    *state = TestPluginInvocationGateState {
        armed: true,
        entered: false,
        released: false,
    };
}

#[cfg(test)]
pub(crate) fn wait_for_test_plugin_invocation_gate(deadline: Duration) -> bool {
    let (lock, condition) = test_plugin_invocation_gate();
    let state = lock.lock().expect("plugin invocation gate mutex");
    let (state, _) = condition
        .wait_timeout_while(state, deadline, |state| !state.entered)
        .expect("plugin invocation gate wait");
    state.entered
}

#[cfg(test)]
pub(crate) fn release_test_plugin_invocation_gate() {
    let (lock, condition) = test_plugin_invocation_gate();
    let mut state = lock.lock().expect("plugin invocation gate mutex");
    state.released = true;
    condition.notify_all();
}

#[cfg(test)]
fn hold_controlled_test_plugin_invocation(request: &PluginInvocationRequest) -> bool {
    if request.handler.handler_id != "controlled_gate" {
        return true;
    }
    let (lock, condition) = test_plugin_invocation_gate();
    let mut state = lock.lock().expect("plugin invocation gate mutex");
    if !state.armed {
        return true;
    }
    state.entered = true;
    condition.notify_all();
    // Ten seconds is a test safety bound. It is not a runtime latency requirement.
    let (mut state, timeout) = condition
        .wait_timeout_while(state, Duration::from_secs(10), |state| !state.released)
        .expect("plugin invocation gate wait");
    let released = state.released && !timeout.timed_out();
    state.armed = false;
    released
}

/// Shared hub-owned primitives exposed to one Lua plugin runtime.
/// Construct this API through `HubRuntime::lua_plugin_host_api` to retain its account.
#[derive(Clone)]
pub struct LuaPluginHostApi {
    pub(crate) memory: Arc<LuaMemoryAccount>,
    pub capabilities: SharedHubCapabilityRuntime,
    pub coordination: HubCoordinationBridge,
    pub entity_publish: HubEntityPublishBridge,
    pub session_types: SharedSessionTypeSpawner,
    pub spawn_targets: SharedSpawnTargets,
    pub worktrees: SharedWorktrees,
    pub package_event_router: Arc<PackageEventRouter>,
    pub causal_scopes: Arc<CausalScopeTable>,
    #[cfg(test)]
    pub(crate) lua_plugin_runtimes: Arc<Mutex<Vec<std::sync::Weak<LuaPluginRuntime>>>>,
}

/// Real Lua runtime for one loaded plugin package.
pub struct LuaPluginRuntime {
    plugin_key: PluginKey,
    lua: Mutex<LuaState>,
    instruction_budget: Arc<AtomicU64>,
    stopped: AtomicBool,
}

/// This private owner covers construction, synchronous use, and Lua destruction.
/// No strong Lua owner may escape its borrowed API. Such an escape requires a new proof.
struct LuaState {
    lua: Option<Lua>,
    charges: LuaStateCharges,
    #[cfg(test)]
    drop_hook: Option<Box<dyn FnMut(LuaStateDropPhase) + Send>>,
}

/// An armed charge remains reserved if covered cleanup does not return.
struct LuaStateCharges {
    // None means completed cleanup disarmed this mandatory reservation.
    vm: Option<LuaVmCharge>,
    instruction_error: Option<LuaCallbackCharge>,
    capacity_string: Option<LuaCallbackCharge>,
}

impl LuaStateCharges {
    fn new(vm: LuaVmCharge) -> Self {
        Self {
            vm: Some(vm),
            instruction_error: None,
            capacity_string: None,
        }
    }

    fn release(&mut self) {
        drop(self.capacity_string.take());
        drop(self.instruction_error.take());
        drop(self.vm.take());
    }
}

impl Drop for LuaStateCharges {
    fn drop(&mut self) {
        if let Some(capacity_string) = self.capacity_string.take() {
            std::mem::forget(capacity_string);
        }
        if let Some(instruction_error) = self.instruction_error.take() {
            std::mem::forget(instruction_error);
        }
        if let Some(vm) = self.vm.take() {
            std::mem::forget(vm);
        }
    }
}

#[cfg(test)]
enum LuaStateConstructionTest {
    RejectLibrary,
    PanicBeforeCreation,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LuaStateDropPhase {
    BeforeLua,
    AfterLua,
}

impl LuaState {
    fn new(vm: LuaVmCharge) -> mlua::Result<Self> {
        Self::construct(
            vm,
            #[cfg(test)]
            None,
        )
    }

    fn construct(
        vm: LuaVmCharge,
        #[cfg(test)] test: Option<LuaStateConstructionTest>,
    ) -> mlua::Result<Self> {
        let mut state = Self {
            lua: None,
            charges: LuaStateCharges::new(vm),
            #[cfg(test)]
            drop_hook: None,
        };
        let libraries = StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::UTF8;
        #[cfg(test)]
        let libraries = match test {
            Some(LuaStateConstructionTest::RejectLibrary) => StdLib::DEBUG,
            Some(LuaStateConstructionTest::PanicBeforeCreation) => {
                panic!("test constructor panic after VM admission")
            }
            None => libraries,
        };
        match Lua::new_with(libraries, LuaOptions::default()) {
            Ok(lua) => state.lua = Some(lua),
            Err(error) => {
                // Pinned mlua 0.11.6 returns Err only before state allocation.
                // Internal construction failures panic and retain the armed charge.
                state.charges.release();
                return Err(error);
            }
        }
        Ok(state)
    }

    fn lua(&self) -> &Lua {
        self.lua.as_ref().expect("constructed Lua state")
    }

    fn hold_instruction_error(&mut self, charge: LuaCallbackCharge) {
        debug_assert!(self.charges.instruction_error.is_none());
        self.charges.instruction_error = Some(charge);
    }

    fn hold_capacity_string(&mut self, charge: LuaCallbackCharge) {
        debug_assert!(self.charges.capacity_string.is_none());
        self.charges.capacity_string = Some(charge);
    }

    #[cfg(test)]
    fn instruction_error_bytes(&self) -> usize {
        self.charges
            .instruction_error
            .as_ref()
            .map(LuaCallbackCharge::bytes)
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn capacity_string_bytes(&self) -> usize {
        self.charges
            .capacity_string
            .as_ref()
            .map(LuaCallbackCharge::bytes)
            .unwrap_or(0)
    }
}

impl Drop for LuaState {
    fn drop(&mut self) {
        if let Some(lua) = self.lua.take() {
            #[cfg(test)]
            if let Some(hook) = self.drop_hook.as_mut() {
                hook(LuaStateDropPhase::BeforeLua);
            }
            // Current callbacks keep temporary strong owners inside synchronous calls.
            // Thus this drop completes the last strong owner's destruction.
            drop(lua);
            #[cfg(test)]
            if let Some(hook) = self.drop_hook.as_mut() {
                hook(LuaStateDropPhase::AfterLua);
            }
            self.charges.release();
        }
        // A single cleanup panic skips release and the charge guard retains funding.
        // A second panic during unwind aborts; it is not a guard-drop path.
    }
}

#[cfg(feature = "allocation-oracle")]
pub struct HookRaiseStorm {
    state: LuaState,
    budget: Arc<AtomicU64>,
}

#[cfg(feature = "allocation-oracle")]
pub fn prepare_hook_raise_storm() -> Result<HookRaiseStorm, String> {
    let memory = LuaMemoryAccount::new(crate::config::lua_memory_limits())
        .map_err(|error| format!("{error:?}"))?;
    let mut state = LuaState::new(memory.reserve_vm().map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    let shared: Arc<dyn Error + Send + Sync> = Arc::new(InstructionBudgetExceeded);
    let charge = memory
        .reserve_shared_callback_storage(crate::lua_memory::layout::arc_bytes::<
            InstructionBudgetExceeded,
        >())
        .map_err(|error| error.to_string())?;
    state.hold_instruction_error(charge);
    let lua = state.lua();
    lua.set_memory_limit(memory.limits().per_vm_bytes)
        .map_err(|error| error.to_string())?;
    let budget = Arc::new(AtomicU64::new(1_000));
    let hook_budget = Arc::clone(&budget);
    let hook_error = Arc::clone(&shared);
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(1_000),
        move |_lua, _debug| {
            let previous = hook_budget.fetch_sub(1_000, Ordering::Relaxed);
            if previous <= 1_000 {
                return Err(mlua::Error::ExternalError(Arc::clone(&hook_error)));
            }
            Ok(VmState::Continue)
        },
    )
    .map_err(|error| error.to_string())?;
    lua.load("kept = {}; for i = 1, 32 do kept[i] = false end")
        .exec()
        .map_err(|error| error.to_string())?;
    Ok(HookRaiseStorm { state, budget })
}

#[cfg(feature = "allocation-oracle")]
impl HookRaiseStorm {
    pub fn used_memory(&self) -> usize {
        self.state.lua().used_memory()
    }

    pub fn retain_errors(&self, n: u32) -> Result<(), String> {
        for index in 1..=n {
            self.budget.store(1_000, Ordering::Relaxed);
            self.state
                .lua()
                .load(&format!(
                    r#"
                    local ok, err = pcall(function()
                        for j = 1, 100000 do end
                    end)
                    assert(not ok, tostring(err))
                    kept[{index}] = err
                    "#
                ))
                .exec()
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod state_owner_tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    fn memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(crate::config::lua_memory_limits()).unwrap()
    }

    #[test]
    fn instruction_budget_hook_reuses_shared_external_error() {
        let memory = memory();
        let mut state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
        let shared: Arc<dyn Error + Send + Sync> = Arc::new(InstructionBudgetExceeded);
        let charge = memory
            .reserve_shared_callback_storage(crate::lua_memory::layout::arc_bytes::<
                InstructionBudgetExceeded,
            >())
            .unwrap();
        state.hold_instruction_error(charge);
        let lua = state.lua();
        lua.set_memory_limit(memory.limits().per_vm_bytes).unwrap();
        let budget = Arc::new(AtomicU64::new(1_000));
        let hook_budget = Arc::clone(&budget);
        let hook_error = Arc::clone(&shared);
        drop(shared);
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(1_000),
            move |_lua, _debug| {
                let previous = hook_budget.fetch_sub(1_000, Ordering::Relaxed);
                if previous <= 1_000 {
                    return Err(mlua::Error::ExternalError(Arc::clone(&hook_error)));
                }
                Ok(VmState::Continue)
            },
        )
        .unwrap();
        budget.store(1_000, Ordering::Relaxed);
        let first = lua
            .load("for i = 1, 100000 do end")
            .exec()
            .expect_err("instruction budget must raise");
        assert_eq!(first.to_string(), INSTRUCTION_BUDGET_ERROR);
        assert!(!first.to_string().contains("runtime error:"));
        let mlua::Error::ExternalError(arc) = &first else {
            panic!("hook error must be ExternalError");
        };
        let arc = Arc::clone(arc);
        let base = Arc::strong_count(&arc);
        let mut kept = vec![first];
        for _ in 0..7 {
            budget.store(1_000, Ordering::Relaxed);
            let error = lua
                .load("for i = 1, 100000 do end")
                .exec()
                .expect_err("instruction budget must raise");
            assert_eq!(error.to_string(), INSTRUCTION_BUDGET_ERROR);
            let mlua::Error::ExternalError(next) = &error else {
                panic!("hook error must be ExternalError");
            };
            assert!(Arc::ptr_eq(&arc, next));
            kept.push(error);
        }
        assert!(Arc::strong_count(&arc) > base);
    }

    fn runtime(memory: &Arc<LuaMemoryAccount>) -> Arc<LuaPluginRuntime> {
        let state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
        Arc::new(LuaPluginRuntime {
            plugin_key: PluginKey("state-owner-test".into()),
            lua: Mutex::new(state),
            instruction_budget: Arc::new(AtomicU64::new(DEFAULT_INSTRUCTION_BUDGET)),
            stopped: AtomicBool::new(false),
        })
    }

    #[test]
    fn normal_transfer_releases_after_lua_destruction() {
        let memory = memory();
        let mut state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
        let phases = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&phases);
        let charged = Arc::clone(&memory);
        state.drop_hook = Some(Box::new(move |phase| {
            assert_eq!(charged.usage().0, charged.limits().per_vm_bytes);
            observed.lock().unwrap().push(phase);
        }));
        {
            // Current callers end temporary strong string borrows inside state use.
            let string = state.lua().create_string("owned-state").unwrap();
            let bytes = string.as_bytes();
            assert_eq!(&*bytes, b"owned-state");
        }
        let state = Mutex::new(state);
        assert_eq!(memory.usage().0, memory.limits().per_vm_bytes);
        drop(state);
        assert_eq!(
            *phases.lock().unwrap(),
            vec![LuaStateDropPhase::BeforeLua, LuaStateDropPhase::AfterLua,]
        );
        assert_eq!(memory.usage(), (0, 0));
    }

    #[test]
    fn returned_constructor_error_releases_vm_charge() {
        let memory = memory();
        let result = LuaState::construct(
            memory.reserve_vm().unwrap(),
            Some(LuaStateConstructionTest::RejectLibrary),
        );
        assert!(matches!(result, Err(mlua::Error::SafetyError(_))));
        assert_eq!(memory.usage(), (0, 0));
    }

    #[test]
    fn constructor_panic_retains_vm_charge() {
        let memory = memory();
        let result = catch_unwind(AssertUnwindSafe(|| {
            // This seam proves owner retention, not mlua partial-state cleanup.
            let _state = LuaState::construct(
                memory.reserve_vm().unwrap(),
                Some(LuaStateConstructionTest::PanicBeforeCreation),
            )
            .unwrap();
        }));
        assert!(result.is_err());
        assert_eq!(memory.usage(), (memory.limits().per_vm_bytes, 0));
    }

    #[test]
    fn setup_error_releases_after_clean_state_destruction() {
        let memory = memory();
        let setup = || -> mlua::Result<()> {
            let state = LuaState::new(memory.reserve_vm().unwrap())?;
            state
                .lua()
                .load("error('state owner setup failure')")
                .exec()?;
            Ok(())
        };
        let error = setup().unwrap_err();
        assert!(error.to_string().contains("state owner setup failure"));
        assert_eq!(memory.usage(), (0, 0));
    }

    #[test]
    fn unrelated_unwind_releases_after_clean_state_destruction() {
        let memory = memory();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
            state.lua().load("return 1").exec().unwrap();
            panic!("test unrelated setup unwind");
        }));
        assert!(result.is_err());
        assert_eq!(memory.usage(), (0, 0));
    }

    #[test]
    fn cleanup_hook_panic_retains_vm_charge() {
        let memory = memory();
        let mut state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
        let charged = Arc::clone(&memory);
        state.drop_hook = Some(Box::new(move |phase| {
            assert_eq!(phase, LuaStateDropPhase::BeforeLua);
            assert_eq!(charged.usage().0, charged.limits().per_vm_bytes);
            // This proves owner ordering, not a real mlua finalizer panic.
            panic!("test cleanup panic before completion");
        }));
        let result = catch_unwind(AssertUnwindSafe(|| drop(state)));
        assert!(result.is_err());
        assert_eq!(memory.usage(), (memory.limits().per_vm_bytes, 0));
    }

    #[test]
    fn poisoned_runtime_keeps_charge_until_last_clone_drops() {
        let memory = memory();
        let runtime = runtime(&memory);
        let surviving = Arc::clone(&runtime);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _state = runtime.lua.lock().unwrap();
            panic!("test poisoned state mutex");
        }));
        assert!(result.is_err());
        assert!(runtime.lua.is_poisoned());
        drop(runtime);
        assert_eq!(memory.usage(), (memory.limits().per_vm_bytes, 0));
        drop(surviving);
        assert_eq!(memory.usage(), (0, 0));
    }

    #[test]
    fn poisoned_mutex_extraction_keeps_charge_until_state_drops() {
        let memory = memory();
        let mutex = Mutex::new(LuaState::new(memory.reserve_vm().unwrap()).unwrap());
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _state = mutex.lock().unwrap();
            panic!("test poisoned extraction");
        }));
        assert!(result.is_err());
        let state = match mutex.into_inner() {
            Ok(_) => panic!("the mutex must be poisoned"),
            Err(poison) => poison.into_inner(),
        };
        assert_eq!(memory.usage(), (memory.limits().per_vm_bytes, 0));
        drop(state);
        assert_eq!(memory.usage(), (0, 0));
    }
}

impl LuaPluginRuntime {
    /// Load a prepared Lua package with the supplied Hub account.
    pub fn load_prepared(
        prepared: &PreparedLocalPackage,
        configuration: PackageConfigurationView,
        api: LuaPluginHostApi,
        package_records: Vec<PackageRecord>,
    ) -> Result<HubPluginRuntimeBundle, LuaPluginRuntimeError> {
        Self::load_prepared_bounded(prepared, configuration, api, package_records)
    }

    /// Load with the shared account retained by the supplied Host API.
    pub(crate) fn load_prepared_bounded(
        prepared: &PreparedLocalPackage,
        configuration: PackageConfigurationView,
        api: LuaPluginHostApi,
        package_records: Vec<PackageRecord>,
    ) -> Result<HubPluginRuntimeBundle, LuaPluginRuntimeError> {
        #[cfg(test)]
        let lua_plugin_runtimes = Arc::clone(&api.lua_plugin_runtimes);
        let memory = api.memory;
        let plugin_key = PluginKey(prepared.package_name.clone());
        let entrypoint = prepared.selected_entrypoint_path.as_ref().ok_or_else(|| {
            LuaPluginRuntimeError::Load("local package has no lua entrypoint".to_string())
        })?;
        let host_api = LuaHostApi {
            configuration,
            capabilities: api.capabilities,
            coordination: api.coordination,
            entity_publish: api.entity_publish,
            session_types: api.session_types,
            spawn_targets: api.spawn_targets,
            worktrees: api.worktrees,
            package_records,
            package_event_router: api.package_event_router,
            causal_scopes: api.causal_scopes,
            memory: Arc::clone(&memory),
        };
        let loaded = LoadedLuaPlugin::load(plugin_key.clone(), entrypoint, host_api, memory)?;
        let runtime = Arc::new(loaded.runtime);
        #[cfg(test)]
        lua_plugin_runtimes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(std::sync::Arc::downgrade(&runtime));
        Ok(HubPluginRuntimeBundle {
            runtime,
            handlers: loaded.handlers,
            event_handlers: loaded.event_handlers,
            descriptors: loaded.descriptors,
            resources: loaded.resources,
            entrypoint: Some(entrypoint.to_string_lossy().into_owned()),
            metadata: Some(BoundaryJson(json!({
                "runtime": "lua",
                "abi": "botster.lua.v1",
            }))),
        })
    }

    fn new(
        plugin_key: PluginKey,
        entrypoint: &Path,
        host_api: LuaHostApi,
        memory: Arc<LuaMemoryAccount>,
    ) -> Result<(Self, LuaRegistration), LuaPluginRuntimeError> {
        let vm_charge = memory
            .reserve_vm()
            .map_err(|error| LuaPluginRuntimeError::Load(error.to_string()))?;
        let mut state = LuaState::new(vm_charge)?;
        let instruction_error: Arc<dyn Error + Send + Sync> = Arc::new(InstructionBudgetExceeded);
        let instruction_charge = memory
            .reserve_shared_callback_storage(crate::lua_memory::layout::arc_bytes::<
                InstructionBudgetExceeded,
            >())
            .map_err(|error| LuaPluginRuntimeError::Load(error.to_string()))?;
        state.hold_instruction_error(instruction_charge);
        let budget = Arc::new(AtomicU64::new(DEFAULT_INSTRUCTION_BUDGET));
        let source_charge = memory
            .reserve_callback()
            .map_err(|error| LuaPluginRuntimeError::Load(error.to_string()))?;
        let source = read_lua_source_bounded(entrypoint, memory.limits().per_callback_bytes)?;
        let (registration, capacity_string) = {
            let lua = state.lua();
            lua.set_memory_limit(memory.limits().per_vm_bytes)
                .map_err(LuaPluginRuntimeError::from)?;
            let hook_budget = budget.clone();
            let hook_error = Arc::clone(&instruction_error);
            lua.set_hook(
                HookTriggers::new().every_nth_instruction(1_000),
                move |_lua, _debug| {
                    let previous = hook_budget.fetch_sub(1_000, Ordering::Relaxed);
                    if previous <= 1_000 {
                        return Err(mlua::Error::ExternalError(Arc::clone(&hook_error)));
                    }
                    Ok(VmState::Continue)
                },
            )?;
            sandbox::install(lua)?;
            let capacity_string = install_botster_api(lua, plugin_key.clone(), host_api)?;
            let value: Value = lua
                .load(&source)
                .set_name(entrypoint.to_string_lossy().as_ref())
                .eval()
                .map_err(LuaPluginRuntimeError::from)?;
            let registration = registration_from_value(lua, value)?;
            (registration, capacity_string)
        };
        state.hold_capacity_string(capacity_string);
        drop(source);
        drop(source_charge);

        Ok((
            Self {
                plugin_key,
                lua: Mutex::new(state),
                instruction_budget: budget,
                stopped: AtomicBool::new(false),
            },
            registration,
        ))
    }

    #[cfg(test)]
    pub(crate) fn test_plugin_key(&self) -> &str {
        &self.plugin_key.0
    }

    #[cfg(test)]
    pub(crate) fn test_instruction_error_bytes(&self) -> usize {
        self.lua
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .instruction_error_bytes()
    }

    #[cfg(test)]
    pub(crate) fn test_capacity_string_bytes(&self) -> usize {
        self.lua
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .capacity_string_bytes()
    }
}

fn read_lua_source_bounded(
    entrypoint: &Path,
    byte_limit: usize,
) -> Result<String, LuaPluginRuntimeError> {
    let mut file = std::fs::File::open(entrypoint).map_err(|error| {
        LuaPluginRuntimeError::Load(format!("failed to open Lua entrypoint: {error}"))
    })?;
    // Allocate the complete charged ceiling once. A growing file cannot make
    // Vec choose an uncharged capacity beyond it.
    let mut bytes = vec![0_u8; byte_limit];
    let mut filled = 0;
    while filled < byte_limit {
        let read = file.read(&mut bytes[filled..]).map_err(|error| {
            LuaPluginRuntimeError::Load(format!("failed to read Lua entrypoint: {error}"))
        })?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    let mut extra = [0_u8; 1];
    if filled == byte_limit
        && file.read(&mut extra).map_err(|error| {
            LuaPluginRuntimeError::Load(format!("failed to read Lua entrypoint: {error}"))
        })? != 0
    {
        return Err(LuaPluginRuntimeError::Load(format!(
            "Lua entrypoint exceeds the {byte_limit} byte callback limit"
        )));
    }
    bytes.truncate(filled);
    String::from_utf8(bytes).map_err(|error| {
        LuaPluginRuntimeError::Load(format!("Lua entrypoint is not UTF-8: {error}"))
    })
}

impl PluginRuntime for LuaPluginRuntime {
    fn invoke(
        &self,
        request: PluginInvocationRequest,
        cancellation: PluginCancellationToken,
    ) -> PluginInvocationResult {
        if self.stopped.load(Ordering::SeqCst) {
            return failed(
                request,
                PluginInvocationFailureKind::WorkerStopped,
                "lua runtime stopped",
            );
        }
        if cancellation.is_cancelled() {
            return failed(
                request,
                PluginInvocationFailureKind::Cancelled,
                "invocation cancelled",
            );
        }
        if request.handler.plugin_key != self.plugin_key {
            return failed(
                request,
                PluginInvocationFailureKind::HandlerFailed,
                "handler belongs to a different plugin",
            );
        }

        #[cfg(test)]
        if !hold_controlled_test_plugin_invocation(&request) {
            return failed(
                request,
                PluginInvocationFailureKind::TimedOut,
                "controlled test plugin invocation gate timed out",
            );
        }

        #[cfg(test)]
        if request.context.origin.as_deref() == Some(PACKAGE_EVENT_INVOCATION_ORIGIN) {
            let hold_ms = TEST_EVENT_HANDLER_HOLD_MS.load(Ordering::Relaxed);
            if hold_ms > 0 {
                thread::sleep(Duration::from_millis(hold_ms));
            }
        }
        let state = self.lua.lock().expect("lua runtime mutex");
        let lua = state.lua();
        self.instruction_budget
            .store(DEFAULT_INSTRUCTION_BUDGET, Ordering::Relaxed);
        let handlers = match lua.globals().get::<Table>("__botster_handlers") {
            Ok(handlers) => handlers,
            Err(error) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("handler registry missing: {error}"),
                );
            }
        };
        let function = match handlers.get::<Function>(request.handler.handler_id.as_str()) {
            Ok(function) => function,
            Err(_) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    "plugin handler is not registered in Lua",
                );
            }
        };
        let payload = match lua.to_value(&request.payload.0) {
            Ok(payload) => payload,
            Err(error) => {
                return failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("failed to encode invocation payload: {error}"),
                );
            }
        };

        let scope_id = request
            .context
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.0.get("causal_scope_id"))
            .and_then(serde_json::Value::as_u64);
        set_current_causal_scope(scope_id);
        // These event consumers read success or failure, but never the return value.
        let acknowledge_event = request.handler.kind == PluginHandlerKind::Event
            && matches!(
                request.context.origin.as_deref(),
                Some(PACKAGE_EVENT_INVOCATION_ORIGIN | SESSION_FAMILY_INVOCATION_ORIGIN)
            );
        let outcome = match function.call::<Value>(payload) {
            Ok(_) if acknowledge_event => {
                PluginInvocationResult::Completed(PluginInvocationSuccess {
                    request_id: request.request_id,
                    handler: request.handler,
                    payload: None,
                })
            }
            Ok(Value::Nil) => PluginInvocationResult::Completed(PluginInvocationSuccess {
                request_id: request.request_id,
                handler: request.handler,
                payload: None,
            }),
            Ok(value) => match lua.from_value::<serde_json::Value>(value) {
                Ok(value) => PluginInvocationResult::Completed(PluginInvocationSuccess {
                    request_id: request.request_id,
                    handler: request.handler,
                    payload: Some(BoundaryJson(value)),
                }),
                Err(error) => failed(
                    request,
                    PluginInvocationFailureKind::HandlerFailed,
                    format!("failed to decode Lua handler response: {error}"),
                ),
            },
            Err(error) => failed(
                request,
                PluginInvocationFailureKind::HandlerFailed,
                sanitize_lua_error(error),
            ),
        };
        set_current_causal_scope(None);
        outcome
    }

    fn stop(&self, _plugin_key: &PluginKey) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Ok(state) = self.lua.lock()
            && let Ok(handlers) = state.lua().create_table()
        {
            let _ = state.lua().globals().set("__botster_handlers", handlers);
        }
    }
}

struct LoadedLuaPlugin {
    runtime: LuaPluginRuntime,
    handlers: Vec<PluginHandlerRegistration>,
    event_handlers: Vec<HubPluginEventHandler>,
    descriptors: Vec<PluginOwnedDescriptor>,
    resources: Vec<PluginResourceRef>,
}

impl LoadedLuaPlugin {
    fn load(
        plugin_key: PluginKey,
        entrypoint: &Path,
        host_api: LuaHostApi,
        memory: Arc<LuaMemoryAccount>,
    ) -> Result<Self, LuaPluginRuntimeError> {
        let (runtime, registration) =
            LuaPluginRuntime::new(plugin_key.clone(), entrypoint, host_api, memory)?;
        let mut handlers = Vec::new();
        let mut event_handlers = Vec::new();
        let mut descriptors = Vec::new();
        let mut resources = Vec::new();
        let mut entity_provider_families = BTreeSet::new();

        for tool in registration.tools {
            let handler = PluginHandlerRef {
                plugin_key: plugin_key.clone(),
                kind: PluginHandlerKind::McpTool,
                handler_id: tool.handler.clone(),
            };
            handlers.push(PluginHandlerRegistration {
                handler: handler.clone(),
                required_capability: None,
            });
            descriptors.push(PluginOwnedDescriptor {
                descriptor: PluginDescriptorRef {
                    plugin_key: plugin_key.clone(),
                    kind: PluginDescriptorKind::McpTool,
                    descriptor_id: tool.name.clone(),
                },
                handler: Some(handler),
                body: BoundaryJson(json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                })),
            });
            resources.push(PluginResourceRef {
                plugin_key: plugin_key.clone(),
                kind: PluginResourceKind::McpRegistration,
                resource_id: tool.name,
            });
        }

        for handler in registration.handlers {
            if handler.id.trim().is_empty() {
                return Err(LuaPluginRuntimeError::Lua(
                    "plugin handler id must be non-empty".to_string(),
                ));
            }
            let descriptor_kind = descriptor_kind_for_handler_kind(handler.kind.clone());
            let handler_ref = PluginHandlerRef {
                plugin_key: plugin_key.clone(),
                kind: handler.kind.clone(),
                handler_id: handler.id.clone(),
            };
            handlers.push(PluginHandlerRegistration {
                handler: handler_ref.clone(),
                required_capability: None,
            });
            if handler.kind == PluginHandlerKind::EntityProvider {
                let entity_type = EntityKind(handler.descriptor_id.clone());
                if EntityContract::is_reserved_builtin(&handler.descriptor_id) {
                    return Err(LuaPluginRuntimeError::Lua(format!(
                        "entity provider family {} is reserved by Hub/Core",
                        handler.descriptor_id
                    )));
                }
                let owner_token = package_entity_owner_token(&plugin_key.0);
                EntityContract::validate_entity_type(&entity_type, Some(&owner_token))
                    .map_err(|error| LuaPluginRuntimeError::Lua(error.to_string()))?;
                let id_field = handler
                    .body
                    .get("id_field")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("id");
                EntityContract::validate_id_field(&entity_type, id_field)
                    .map_err(|error| LuaPluginRuntimeError::Lua(error.to_string()))?;
                if handler
                    .body
                    .get("entity_type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|declared| declared != handler.descriptor_id)
                {
                    return Err(LuaPluginRuntimeError::Lua(format!(
                        "entity provider descriptor id must equal entity_type {}",
                        handler.descriptor_id
                    )));
                }
                if !entity_provider_families.insert(handler.descriptor_id.clone()) {
                    return Err(LuaPluginRuntimeError::Lua(format!(
                        "duplicate entity provider family: {}",
                        handler.descriptor_id
                    )));
                }
                resources.push(PluginResourceRef {
                    plugin_key: plugin_key.clone(),
                    kind: PluginResourceKind::EntityProvider,
                    resource_id: handler.descriptor_id.clone(),
                });
            }
            if handler.kind == PluginHandlerKind::Event {
                let event_name = handler.event_name.ok_or_else(|| {
                    LuaPluginRuntimeError::Lua(
                        "event handlers require an event or event_name field".to_string(),
                    )
                })?;
                if event_name.trim().is_empty() {
                    return Err(LuaPluginRuntimeError::Lua(
                        "event handlers require a non-empty event name".to_string(),
                    ));
                }
                let event_owner = handler.event_owner.unwrap_or_default();
                if event_owner.trim().is_empty() {
                    return Err(LuaPluginRuntimeError::Lua(
                        EventPlaneStatus::RejectedInvalid.as_str().to_string(),
                    ));
                }
                event_handlers.push(HubPluginEventHandler {
                    event_owner,
                    event_name,
                    handler: handler_ref.clone(),
                });
            }
            if let Some(kind) = descriptor_kind {
                descriptors.push(PluginOwnedDescriptor {
                    descriptor: PluginDescriptorRef {
                        plugin_key: plugin_key.clone(),
                        kind,
                        descriptor_id: handler.descriptor_id.clone(),
                    },
                    handler: Some(handler_ref),
                    body: BoundaryJson(handler.body),
                });
            }
        }

        Ok(Self {
            runtime,
            handlers,
            event_handlers,
            descriptors,
            resources,
        })
    }
}

#[derive(Debug)]
pub enum LuaPluginRuntimeError {
    Load(String),
    Lua(String),
}

impl fmt::Display for LuaPluginRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(message) | Self::Lua(message) => formatter.write_str(message),
        }
    }
}

impl Error for LuaPluginRuntimeError {}

impl From<mlua::Error> for LuaPluginRuntimeError {
    fn from(error: mlua::Error) -> Self {
        Self::Lua(sanitize_lua_error(error))
    }
}

#[derive(Debug)]
struct LuaRegistration {
    tools: Vec<LuaToolRegistration>,
    handlers: Vec<LuaHandlerRegistration>,
}

#[derive(Debug)]
struct LuaToolRegistration {
    name: String,
    description: String,
    input_schema: serde_json::Value,
    handler: String,
}

#[derive(Debug)]
struct LuaHandlerRegistration {
    id: String,
    kind: PluginHandlerKind,
    descriptor_id: String,
    event_owner: Option<String>,
    event_name: Option<String>,
    body: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
    })
}

fn install_botster_api(
    lua: &Lua,
    plugin_key: PluginKey,
    host_api: LuaHostApi,
) -> Result<LuaCallbackCharge, LuaPluginRuntimeError> {
    let globals = lua.globals();
    globals.set("__botster_handlers", lua.create_table()?)?;
    globals.set("os", Value::Nil)?;
    globals.set("io", Value::Nil)?;
    globals.set("package", Value::Nil)?;

    let (event_on, register): (Function, Function) = lua
        .load(include_str!("lua_runtime/registration.lua"))
        .call((lua.globals(), lua.null()))?;
    let events = lua.create_table()?;
    events.set("on", event_on)?;
    let emit_router = host_api.package_event_router.clone();
    let emit_plugin = plugin_key.clone();
    let emit_scopes = host_api.causal_scopes.clone();
    events.set(
        "emit",
        callback::create(lua, move |lua, (name, payload): (EventName, Value)| {
            if let Some(scope_id) = current_causal_scope()
                && emit_scopes.is_live(scope_id)
            {
                return lua.to_value(&json!({
                    "status": EventPlaneStatus::RejectedCausalScope.as_str(),
                }));
            }
            let name = name.0.to_str()?;
            let payload = lua.from_value::<serde_json::Value>(payload)?;
            let status = emit_router.try_ingress(&emit_plugin.0, &name, &payload, Instant::now());
            lua.to_value(&json!({ "status": status.as_str() }))
        })?,
    )?;
    globals.set("__botster_registration", lua.create_table()?)?;
    globals.set("events", events)?;

    let botster = lua.create_table()?;
    botster.set("register", register)?;

    let capabilities_table = lua.create_table()?;
    let timer_capabilities = host_api.capabilities.clone();
    let timer_plugin_key = plugin_key.clone();
    capabilities_table.set(
        "timer_once",
        callback::create(lua, move |lua, delay_ms: u64| {
            let operation_id = CapabilityOperationId(format!("lua-timer-{delay_ms}"));
            let request = CapabilityRuntimeRequest {
                plugin_key: timer_plugin_key.clone(),
                operation_id: operation_id.clone(),
                operation: CapabilityOperation::Timer(TimerCapabilityRequest::Once { delay_ms }),
                timeout_ms: 1_000,
                callback: None,
            };
            let mut runtime = timer_capabilities.lock().map_err(|_| {
                mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
            })?;
            let handle = runtime
                .submit(request)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            let events = runtime
                .drain_events(&timer_plugin_key)
                .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?;
            lua.to_value(&json!({
                "operation_id": handle.operation_id.0,
                "resource_id": handle.resource.map(|resource| resource.resource_id),
                "event_count": events.len(),
            }))
        })?,
    )?;
    capabilities_table.set(
        "plugin_db",
        plugin_db_table(lua, plugin_key.clone(), host_api.capabilities.clone())?,
    )?;
    capabilities_table.set(
        "session_types",
        session_types_table(
            lua,
            plugin_key.clone(),
            host_api.session_types,
            host_api.spawn_targets.clone(),
            host_api.package_records,
            Some(host_api.memory.clone()),
        )?,
    )?;
    capabilities_table.set(
        "spawn_targets",
        spawn_targets_table(lua, host_api.spawn_targets.clone())?,
    )?;
    capabilities_table.set(
        "worktrees",
        worktrees_table(lua, host_api.worktrees, host_api.spawn_targets)?,
    )?;
    capabilities_table.set("config", config_table(lua, host_api.configuration)?)?;
    botster.set("capabilities", capabilities_table)?;
    let (coordination, capacity_string) = coordination_table(
        lua,
        plugin_key.clone(),
        host_api.coordination,
        host_api.memory,
    )?;
    botster.set("coordination", coordination)?;
    botster.set(
        "entity_publish",
        entity_publish_function(lua, plugin_key, host_api.entity_publish)?,
    )?;
    globals.set("botster", botster)?;
    Ok(capacity_string)
}

fn entity_publish_function(
    lua: &Lua,
    plugin_key: PluginKey,
    bridge: HubEntityPublishBridge,
) -> Result<Function, mlua::Error> {
    callback::create(lua, move |lua, args: Value| {
        let value = lua.from_value::<serde_json::Value>(args)?;
        let scope_id = current_causal_scope();
        let result = match bridge.publish(plugin_key.clone(), value, scope_id) {
            Ok(result) => result,
            Err(EntityPublishError::OwnerFinished(error)) => {
                return Err(mlua::Error::RuntimeError(error));
            }
            Err(EntityPublishError::TimeoutInFlight) => {
                return Err(mlua::Error::RuntimeError(
                    "entity publish request did not complete before timeout".to_string(),
                ));
            }
            Err(EntityPublishError::NeverQueued(error)) => {
                return Err(mlua::Error::RuntimeError(error));
            }
        };
        lua.to_value(&json!({
            "ok": result.ok,
            "status": result.status.as_str(),
            "last_accepted_seq": result.last_accepted_seq,
            "high_water_seq": result.high_water_seq,
            "resync_needed": result.resync_needed,
            "resync_degraded": result.resync_degraded,
        }))
    })
}

fn spawn_targets_table(lua: &Lua, spawn_targets: SharedSpawnTargets) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    let list_targets = spawn_targets.clone();
    table.set(
        "list",
        callback::create(lua, move |lua, ()| {
            let (_, state) = list_targets
                .try_snapshot()
                .map_err(|()| mlua::Error::RuntimeError("hub state lock poisoned".to_string()))?;
            lua.to_value(&crate::spawn_targets::list_spawn_targets(
                &state.spawn_targets,
            ))
        })?,
    )?;
    table.set(
        "validate",
        callback::create(lua, move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let target_id = value
                .get("target_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "spawn_targets.validate requires target_id".to_string(),
                    )
                })?;
            let (_, state) = spawn_targets
                .try_snapshot()
                .map_err(|()| mlua::Error::RuntimeError("hub state lock poisoned".to_string()))?;
            lua.to_value(&crate::spawn_targets::validate_spawn_target(
                &state.spawn_targets,
                target_id,
            ))
        })?,
    )?;
    Ok(table)
}

fn worktrees_table(
    lua: &Lua,
    worktrees: SharedWorktrees,
    spawn_targets: SharedSpawnTargets,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    let list_worktrees = worktrees.clone();
    let list_targets = spawn_targets.clone();
    table.set(
        "list",
        callback::create(lua, move |lua, ()| {
            let (_, state) = list_targets
                .try_snapshot()
                .map_err(|()| mlua::Error::RuntimeError("hub state lock poisoned".to_string()))?;
            debug_assert!(Arc::ptr_eq(&list_targets, &list_worktrees));
            lua.to_value(&crate::worktrees::list_worktrees(
                &state.worktrees,
                &state.spawn_targets,
            ))
        })?,
    )?;
    table.set(
        "show",
        callback::create(lua, move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let worktree_id = value
                .get("worktree_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError("worktrees.show requires worktree_id".to_string())
                })?;
            let (_, state) = spawn_targets
                .try_snapshot()
                .map_err(|()| mlua::Error::RuntimeError("hub state lock poisoned".to_string()))?;
            debug_assert!(Arc::ptr_eq(&spawn_targets, &worktrees));
            match crate::worktrees::show_worktree(
                &state.worktrees,
                &state.spawn_targets,
                worktree_id,
            ) {
                Ok(worktree) => lua.to_value(&json!({
                    "ok": true,
                    "status": worktree.status,
                    "worktree": worktree,
                })),
                Err(error) if error.kind == "not_found" => lua.to_value(&json!({
                    "ok": false,
                    "status": error.kind,
                    "worktree_id": worktree_id,
                    "message": error.message,
                })),
                Err(error) => Err(mlua::Error::RuntimeError(format!(
                    "worktrees.show failed: {error}"
                ))),
            }
        })?,
    )?;
    Ok(table)
}

fn config_table(lua: &Lua, configuration: PackageConfigurationView) -> Result<Table, mlua::Error> {
    let config = lua.create_table()?;
    let payload = json!({
        "values": configuration.effective_values,
        "missing_required": configuration.missing_required,
        "diagnostics": configuration.diagnostics,
    });
    config.set(
        "get",
        callback::create(lua, move |lua, package_name: Value| {
            if !matches!(package_name, Value::Nil) {
                return Err(mlua::Error::RuntimeError(
                    "config.get reads only the loaded plugin configuration and accepts no package name"
                        .to_string(),
                ));
            }
            lua.to_value(&payload)
        })?,
    )?;
    Ok(config)
}

/// These values live in Lua. Callback refusal does not construct a Rust error.
struct SessionTypeReadErrors {
    capacity: mlua::String,
    quota: mlua::String,
    target: mlua::String,
    session_type: mlua::String,
    state: mlua::String,
    conversion: mlua::String,
    marker: Table,
}

impl SessionTypeReadErrors {
    fn finish<T: serde::Serialize>(
        &self,
        lua: &Lua,
        result: Result<Option<T>, crate::session_types::SessionTypeError>,
    ) -> Value {
        match result {
            Ok(Some(value)) => match lua.to_value(&value) {
                Ok(value @ Value::Table(_)) => value,
                _ => Value::String(self.conversion.clone()),
            },
            Ok(None) => Value::String(self.quota.clone()),
            Err(error) => {
                // The callback charge still covers the Rust message during this copy.
                let converted = (|| -> mlua::Result<Table> {
                    let value = lua.create_table_with_capacity(2, 0)?;
                    value.raw_set(1, lua.create_string(error.kind)?)?;
                    value.raw_set(2, lua.create_string(&error.message)?)?;
                    value.set_metatable(Some(self.marker.clone()))?;
                    Ok(value)
                })();
                match converted {
                    Ok(value) => Value::Table(value),
                    Err(_) => Value::String(self.conversion.clone()),
                }
            }
        }
    }
}

fn session_type_read_callback(
    lua: &Lua,
    show: bool,
    state: SharedSpawnTargets,
    records: Vec<PackageRecord>,
    memory: Option<Arc<LuaMemoryAccount>>,
) -> mlua::Result<mlua::Function> {
    let operation = if show {
        "session_types.show"
    } else {
        "session_types.list"
    };
    let errors = SessionTypeReadErrors {
        capacity: lua.create_string("Lua callback memory capacity exhausted")?,
        quota: lua.create_string(format!(
            "{operation} exceeded the Lua callback memory limit"
        ))?,
        target: lua.create_string(format!("{operation} requires a nonblank UTF-8 target_id"))?,
        session_type: lua.create_string(format!(
            "{operation} requires a nonblank UTF-8 session_type_id"
        ))?,
        state: lua.create_string("hub state lock poisoned")?,
        conversion: lua.create_string(format!("{operation} could not allocate its Lua result"))?,
        marker: lua.create_table()?,
    };
    let marker = errors.marker.clone();
    let conversion_failure = errors.conversion.clone();

    // Only the trusted wrapper can call this function. It supplies exactly two strings.
    // Return one non-error Value so mlua cannot build a retained callback error.
    let callback = lua.create_function(
        move |lua, (target_id, session_type_id): (mlua::String, mlua::String)| {
            // Keep this named guard until result conversion and Rust destruction finish.
            let _callback_charge = match memory
                .as_ref()
                .map(LuaMemoryAccount::reserve_callback)
                .transpose()
            {
                Ok(charge) => charge,
                Err(_) => return Ok(Value::String(errors.capacity.clone())),
            };
            let target_bytes = target_id.as_bytes();
            let target_id = match std::str::from_utf8(&target_bytes) {
                Ok(value) if !value.trim().is_empty() => value,
                _ => return Ok(Value::String(errors.target.clone())),
            };
            let session_type_bytes = session_type_id.as_bytes();
            let session_type_id = match std::str::from_utf8(&session_type_bytes) {
                Ok(value) if !show || !value.trim().is_empty() => value,
                _ => return Ok(Value::String(errors.session_type.clone())),
            };
            let (_, state) = match state.try_snapshot() {
                Ok(value) => value,
                Err(()) => return Ok(Value::String(errors.state.clone())),
            };
            let limit = memory
                .as_ref()
                .map_or(usize::MAX, |account| account.limits().per_callback_bytes);
            let result = if show {
                let result = if memory.is_some() {
                    crate::session_types::show_session_type_for_target_bounded(
                        &records,
                        &state,
                        &target_id,
                        &session_type_id,
                        limit,
                    )
                } else {
                    let records = records.iter().collect::<Vec<_>>();
                    crate::session_types::show_session_type_for_target(
                        &records,
                        &state,
                        &target_id,
                        &session_type_id,
                    )
                    .map(Some)
                };
                errors.finish(lua, result)
            } else {
                let result = if memory.is_some() {
                    crate::session_types::list_session_types_for_target_bounded(
                        &records, &state, &target_id, limit,
                    )
                } else {
                    let records = records.iter().collect::<Vec<_>>();
                    crate::session_types::list_session_types_for_target(
                        &records, &state, &target_id,
                    )
                    .map(Some)
                };
                errors.finish(lua, result)
            };
            Ok(result)
        },
    )?;
    // Capture trusted functions before plugin code can change the global table.
    lua.load(
        r#"
        local callback, marker, operation, show, conversion_failure = ...
        local type, error, getmetatable, pcall = type, error, getmetatable, pcall
        local argument_error = operation .. " requires an argument table"
        local target_error = operation .. " requires target_id"
        local session_type_error = operation .. " requires session_type_id"
        local function catalog_error(result)
            return operation .. " failed: " .. result[1] .. ": " .. result[2]
        end
        return function(args)
            if type(args) ~= "table" then
                error(argument_error, 0)
            end
            local target_id = args.target_id
            if type(target_id) ~= "string" then
                error(target_error, 0)
            end
            local session_type_id = ""
            if show then
                session_type_id = args.session_type_id
                if type(session_type_id) ~= "string" then
                    error(session_type_error, 0)
                end
            end
            local result = callback(target_id, session_type_id)
            if type(result) == "string" then
                error(result, 0)
            end
            if getmetatable(result) == marker then
                local ok, message = pcall(catalog_error, result)
                error(ok and message or conversion_failure, 0)
            end
            return result
        end
        "#,
    )
    .set_name("@hub/session_type_read")
    .call((callback, marker, operation, show, conversion_failure))
}

fn session_types_table(
    lua: &Lua,
    plugin_key: PluginKey,
    session_types: SharedSessionTypeSpawner,
    state: SharedSpawnTargets,
    package_records: Vec<PackageRecord>,
    memory: Option<Arc<LuaMemoryAccount>>,
) -> Result<Table, mlua::Error> {
    let table = lua.create_table()?;
    table.set(
        "list",
        session_type_read_callback(
            lua,
            false,
            state.clone(),
            package_records.clone(),
            memory.clone(),
        )?,
    )?;
    table.set(
        "show",
        session_type_read_callback(lua, true, state, package_records.clone(), memory.clone())?,
    )?;
    let spawn_templates = session_types.clone();
    let spawn_plugin_key = plugin_key.clone();
    let spawn_records = package_records.clone();
    let spawn_memory = memory.clone();
    let spawn_conversion =
        lua.create_string("session_types.spawn could not allocate its Lua result")?;
    let spawn_capacity = lua.create_string(LUA_CALLBACK_CAPACITY_EXHAUSTED)?;
    table.set(
        "spawn",
        callback::create(lua, move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            let session_type_id = value
                .get("session_type_id")
                .or_else(|| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "session_types.spawn requires session_type_id".to_string(),
                    )
                })?;
            let request = session_type_request_from_lua(&value)?;
            match spawn_templates.spawn(
                &spawn_plugin_key,
                session_type_id,
                request,
                spawn_records.clone(),
            ) {
                Ok(result) => session_type_spawn::convert_session_type_spawned(
                    lua,
                    spawn_memory.as_ref(),
                    &spawn_templates,
                    result,
                    &spawn_conversion,
                ),
                Err(error) if error.as_ref() == LUA_CALLBACK_CAPACITY_EXHAUSTED => {
                    Ok(Value::String(spawn_capacity.clone()))
                }
                Err(error) => Err(mlua::Error::RuntimeError(format!(
                    "session_types.spawn failed: {error}"
                ))),
            }
        })?,
    )?;
    table.set(
        "ensure_worktree_and_spawn",
        callback::create(lua, move |lua, args: Value| {
            let value = lua.from_value::<serde_json::Value>(args)?;
            reject_trusted_managed_fields(&value)?;
            let target_id = required_string(
                &value,
                "target_id",
                "session_types.ensure_worktree_and_spawn",
            )?;
            let branch =
                required_string(&value, "branch", "session_types.ensure_worktree_and_spawn")?;
            let session_type_id = required_string(
                &value,
                "session_type_id",
                "session_types.ensure_worktree_and_spawn",
            )?;
            let request = managed_session_type_request_from_lua(&value)?;
            match session_types.ensure_worktree_and_spawn(
                &plugin_key,
                target_id,
                branch,
                session_type_id,
                request,
                package_records.clone(),
            ) {
                Ok(spawned) => {
                    let conversion = lua.create_string(
                        "session_types.ensure_worktree_and_spawn could not allocate its Lua result",
                    )?;
                    session_type_spawn::convert_managed_spawned(
                        lua,
                        memory.as_ref(),
                        &session_types,
                        &spawned,
                        &conversion,
                    )
                }
                Err(error) => lua.to_value(&json!({"ok": false, "error": error})),
            }
        })?,
    )?;
    Ok(table)
}

fn required_string<'a>(
    value: &'a serde_json::Value,
    key: &str,
    operation: &str,
) -> Result<&'a str, mlua::Error> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{operation} requires {key}")))
}

fn reject_trusted_managed_fields(value: &serde_json::Value) -> Result<(), mlua::Error> {
    const TOP_LEVEL: &[&str] = &[
        "cwd",
        "session_id",
        "repo_path",
        "worktree_path",
        "branch_name",
        "base_ref",
        "base_commit",
    ];
    const CONTEXT: &[&str] = &[
        "cwd",
        "repo_path",
        "worktree_path",
        "branch_name",
        "base_ref",
        "base_commit",
        "target_id",
    ];
    if TOP_LEVEL.iter().any(|key| value.get(key).is_some())
        || value
            .get("context")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|context| CONTEXT.iter().any(|key| context.contains_key(*key)))
    {
        return Err(mlua::Error::RuntimeError(
            "session_types.ensure_worktree_and_spawn rejects caller-supplied trusted fields"
                .to_string(),
        ));
    }
    Ok(())
}

fn managed_session_type_request_from_lua(
    value: &serde_json::Value,
) -> Result<ManagedSessionTypeRequest, mlua::Error> {
    let context = value.get("context");
    if context.is_some_and(|context| !context.is_object()) {
        return Err(mlua::Error::RuntimeError(
            "session_types.ensure_worktree_and_spawn context must be an object".to_string(),
        ));
    }
    Ok(ManagedSessionTypeRequest {
        environment: string_map(value.get("environment"), "environment")?,
        prompt: context.and_then(|value| optional_string(value, "prompt")),
        ticket_id: context.and_then(|value| optional_string(value, "ticket_id")),
        workspace_id: context.and_then(|value| optional_string(value, "workspace_id")),
        metadata: string_map(
            context.and_then(|value| value.get("metadata")),
            "context.metadata",
        )?,
    })
}

fn session_type_request_from_lua(
    value: &serde_json::Value,
) -> Result<SessionTypeRequest, mlua::Error> {
    Ok(SessionTypeRequest {
        target_id: optional_string(value, "target_id"),
        session_id: optional_string(value, "session_id").map(botster_core::SessionId),
        cwd: optional_string(value, "cwd"),
        environment: string_map(value.get("environment"), "environment")?,
        context: session_type_context_from_lua(value.get("context"))?,
    })
}

fn session_type_context_from_lua(
    value: Option<&serde_json::Value>,
) -> Result<SessionTypeContextInput, mlua::Error> {
    let Some(value) = value else {
        return Ok(SessionTypeContextInput::default());
    };
    if !value.is_object() {
        return Err(mlua::Error::RuntimeError(
            "session_types.spawn context must be an object".to_string(),
        ));
    }
    Ok(SessionTypeContextInput {
        worktree_path: optional_string(value, "worktree_path"),
        repo_path: optional_string(value, "repo_path"),
        branch_name: optional_string(value, "branch_name"),
        prompt: optional_string(value, "prompt"),
        ticket_id: optional_string(value, "ticket_id"),
        workspace_id: optional_string(value, "workspace_id"),
        metadata: string_map(value.get("metadata"), "context.metadata")?,
    })
}

fn optional_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn string_map(
    value: Option<&serde_json::Value>,
    label: &str,
) -> Result<BTreeMap<String, String>, mlua::Error> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let Some(object) = value.as_object() else {
        return Err(mlua::Error::RuntimeError(format!(
            "session_types.spawn {label} must be an object"
        )));
    };
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_string()))
                .ok_or_else(|| {
                    mlua::Error::RuntimeError(format!(
                        "session_types.spawn {label}.{key} must be a string"
                    ))
                })
        })
        .collect()
}

fn plugin_db_table(
    lua: &Lua,
    plugin_key: PluginKey,
    capabilities: SharedHubCapabilityRuntime,
) -> Result<Table, mlua::Error> {
    let plugin_db = lua.create_table()?;
    for (name, action) in [
        ("get", "get"),
        ("set", "set"),
        ("patch", "patch"),
        ("delete", "delete"),
        ("list", "list"),
    ] {
        let runtime = capabilities.clone();
        let key = plugin_key.clone();
        plugin_db.set(
            name,
            callback::create(lua, move |lua, args: Value| {
                let operation = plugin_store_operation_from_lua(lua, action, args)?;
                execute_plugin_store_for_lua(lua, runtime.clone(), key.clone(), operation, action)
            })?,
        )?;
    }
    let batch_runtime = capabilities.clone();
    let batch_plugin_key = plugin_key.clone();
    plugin_db.set(
        "batch",
        callback::create(lua, move |lua, args: Value| {
            execute_plugin_store_batch_for_lua(
                lua,
                batch_runtime.clone(),
                batch_plugin_key.clone(),
                args,
            )
        })?,
    )?;
    Ok(plugin_db)
}

fn execute_plugin_store_batch_for_lua(
    lua: &Lua,
    capabilities: SharedHubCapabilityRuntime,
    plugin_key: PluginKey,
    args: Value,
) -> Result<Value, mlua::Error> {
    let value = lua.from_value::<serde_json::Value>(args)?;
    let Some(object) = value.as_object() else {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch requires an object",
            ),
            None,
            None,
        ));
    };
    if object.len() != 1 || !object.contains_key("mutations") {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch accepts only mutations",
            ),
            None,
            None,
        ));
    }
    let Some(raw_mutations) = value.get("mutations").and_then(serde_json::Value::as_array) else {
        return lua.to_value(&PluginStoreBatchResult::failure(
            botster_core::CapabilityRuntimeError::new(
                CapabilityRuntimeErrorKind::InvalidRequest,
                "plugin_db.batch mutations must be an array",
            ),
            None,
            None,
        ));
    };
    let mut mutations = Vec::with_capacity(raw_mutations.len());
    for (index, raw_mutation) in raw_mutations.iter().enumerate() {
        match serde_json::from_value::<PluginStoreBatchMutation>(raw_mutation.clone()) {
            Ok(mutation) => mutations.push(mutation),
            Err(error) => {
                let key = raw_mutation
                    .get("key")
                    .and_then(serde_json::Value::as_str)
                    .map(|key| PluginStoreKey(key.to_string()));
                return lua.to_value(&PluginStoreBatchResult::failure(
                    botster_core::CapabilityRuntimeError::new(
                        CapabilityRuntimeErrorKind::InvalidRequest,
                        format!("plugin_db.batch mutations are invalid: {error}"),
                    ),
                    Some(index + 1),
                    key,
                ));
            }
        }
    }
    let prepared = {
        let runtime = capabilities.lock().map_err(|_| {
            mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
        })?;
        runtime
            .prepare_plugin_store_batch(&plugin_key, &plugin_key.0, mutations)
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
    };

    lua.to_value(&prepared.execute())
}

fn plugin_store_operation_from_lua(
    lua: &Lua,
    action: &str,
    args: Value,
) -> Result<PluginStoreOperation, mlua::Error> {
    let value = lua.from_value::<serde_json::Value>(args)?;
    let key = value
        .get("key")
        .and_then(serde_json::Value::as_str)
        .map(|key| PluginStoreKey(key.to_string()));
    match action {
        "get" => Ok(PluginStoreOperation::Get {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.get requires key".into()))?,
        }),
        "set" => Ok(PluginStoreOperation::Set {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.set requires key".into()))?,
            schema_version: value
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1),
            payload: value.get("payload").cloned().ok_or_else(|| {
                mlua::Error::RuntimeError("plugin_db.set requires payload".into())
            })?,
            expected_revision: value
                .get("expected_revision")
                .and_then(serde_json::Value::as_u64),
        }),
        "patch" => Ok(PluginStoreOperation::Patch {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.patch requires key".into()))?,
            patch: value.get("patch").cloned().ok_or_else(|| {
                mlua::Error::RuntimeError("plugin_db.patch requires patch".into())
            })?,
            expected_revision: value
                .get("expected_revision")
                .and_then(serde_json::Value::as_u64),
        }),
        "delete" => Ok(PluginStoreOperation::Delete {
            key: key
                .ok_or_else(|| mlua::Error::RuntimeError("plugin_db.delete requires key".into()))?,
        }),
        "list" => Ok(PluginStoreOperation::List {
            prefix: value
                .get("prefix")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
        }),
        _ => Err(mlua::Error::RuntimeError(
            "unsupported plugin_db operation".to_string(),
        )),
    }
}

fn execute_plugin_store_for_lua(
    lua: &Lua,
    capabilities: SharedHubCapabilityRuntime,
    plugin_key: PluginKey,
    operation: PluginStoreOperation,
    action: &str,
) -> Result<Value, mlua::Error> {
    let prepared = {
        let runtime = capabilities.lock().map_err(|_| {
            mlua::Error::RuntimeError("capability runtime lock poisoned".to_string())
        })?;
        runtime
            .prepare_plugin_store(
                &plugin_key,
                PluginStoreCapabilityRequest {
                    namespace: plugin_key.0.clone(),
                    operation,
                },
            )
            .map_err(|error| mlua::Error::RuntimeError(error.to_string()))?
    };

    match prepared.execute() {
        Ok(result) => lua.to_value(&result),
        Err(error)
            if action == "get" && error.kind == CapabilityRuntimeErrorKind::StoreNotFound =>
        {
            lua.to_value(&json!({ "kind": "record" }))
        }
        Err(error) => Err(mlua::Error::RuntimeError(format!(
            "plugin_db operation failed: {}",
            error.message
        ))),
    }
}

pub(crate) fn coordination_table(
    lua: &Lua,
    plugin_key: PluginKey,
    coordination_bridge: HubCoordinationBridge,
    memory: Arc<LuaMemoryAccount>,
) -> Result<(Table, LuaCallbackCharge), mlua::Error> {
    let coordination = lua.create_table()?;

    let publish_bridge = coordination_bridge.clone();
    let publish_plugin_key = plugin_key.clone();
    let publish_memory = Arc::clone(&memory);
    let capacity_xrc = memory
        .reserve_shared_callback_storage(2 * crate::lua_memory::layout::lua_reference_bytes())
        .map_err(|_| mlua::Error::RuntimeError("Lua callback memory capacity exhausted".into()))?;
    let publish_capacity = lua.create_string(LUA_CALLBACK_CAPACITY_EXHAUSTED)?;
    let drain_capacity = publish_capacity.clone();
    coordination.set(
        "publish",
        callback::create(lua, move |lua, args: Value| {
            match admit_publish_operation(&publish_memory, &publish_plugin_key, lua, args) {
                Ok((operation, entry)) => {
                    let outcome = match publish_bridge.submit_admitted(operation, entry) {
                        Ok(HubCoordinationResponse::Publish(outcome)) => outcome,
                        Ok(_) => {
                            return Err(mlua::Error::RuntimeError(
                                "coordination publish returned unexpected response".into(),
                            )
                            .into());
                        }
                        Err(CoordinationRequestError::Local(CoordinationLocalError::Capacity)) => {
                            return Err(callback::CallbackFailure::Raise(publish_capacity.clone()));
                        }
                        Err(error) => {
                            return Err(mlua::Error::RuntimeError(error.as_str().to_owned()).into());
                        }
                    };
                    lua.to_value(&outcome).map_err(Into::into)
                }
                Err(AdmissionError::Capacity) => {
                    Err(callback::CallbackFailure::Raise(publish_capacity.clone()))
                }
                Err(AdmissionError::Runtime(error)) => Err(error.into()),
            }
        })?,
    )?;
    let drain_bridge = coordination_bridge.clone();
    let drain_memory = Arc::clone(&memory);
    coordination.set(
        "drain",
        callback::create(lua, move |lua, args: Value| {
            match admit_drain_operation(&drain_memory, lua, args) {
                Ok((operation, entry)) => {
                    let outcome = match drain_bridge.submit_admitted(operation, entry) {
                        Ok(HubCoordinationResponse::Drain(outcome)) => outcome,
                        Ok(_) => {
                            return Err(mlua::Error::RuntimeError(
                                "coordination drain returned unexpected response".into(),
                            )
                            .into());
                        }
                        Err(CoordinationRequestError::Local(CoordinationLocalError::Capacity)) => {
                            return Err(callback::CallbackFailure::Raise(drain_capacity.clone()));
                        }
                        Err(error) => {
                            return Err(mlua::Error::RuntimeError(error.as_str().to_owned()).into());
                        }
                    };
                    lua.to_value(&outcome).map_err(Into::into)
                }
                Err(AdmissionError::Capacity) => {
                    Err(callback::CallbackFailure::Raise(drain_capacity.clone()))
                }
                Err(AdmissionError::Runtime(error)) => Err(error.into()),
            }
        })?,
    )?;

    coordination.set(
        "acknowledge",
        acknowledge_input::callback(lua, coordination_bridge, memory)?,
    )?;

    Ok((coordination, capacity_xrc))
}

#[derive(Debug)]
pub(crate) enum AdmissionError {
    Capacity,
    Runtime(mlua::Error),
}

impl From<mlua::Error> for AdmissionError {
    fn from(error: mlua::Error) -> Self {
        Self::Runtime(error)
    }
}

fn lua_table(args: Value) -> Result<Table, mlua::Error> {
    match args {
        Value::Table(table) => Ok(table),
        _ => Err(mlua::Error::RuntimeError(
            "coordination request requires a table".into(),
        )),
    }
}

fn lua_string_bytes(table: &Table, key: &str) -> Result<Option<mlua::String>, mlua::Error> {
    match table.raw_get::<Value>(key)? {
        Value::Nil => Ok(None),
        Value::String(text) => Ok(Some(text)),
        _ => Ok(None),
    }
}

fn admit_callback_bytes(
    memory: &Arc<LuaMemoryAccount>,
    payload: usize,
) -> Result<LuaCallbackCharge, AdmissionError> {
    let caller = crate::lua_memory::layout::arc_bytes::<std::sync::atomic::AtomicU8>();
    let reply = crate::lua_memory::layout::single_reply_bytes::<CoordinationReply>(true)
        .ok_or(AdmissionError::Capacity)?;
    let bytes = payload
        .checked_add(caller)
        .and_then(|bytes| bytes.checked_add(reply))
        .ok_or(AdmissionError::Capacity)?;
    memory
        .reserve_callback_total(bytes)
        .map_err(|_| AdmissionError::Capacity)
}

fn admit_publish_operation(
    memory: &Arc<LuaMemoryAccount>,
    plugin_key: &PluginKey,
    lua: &Lua,
    args: Value,
) -> Result<(PendingCoordinationOperation, LuaCallbackCharge), AdmissionError> {
    let table = lua_table(args)?;
    lua_json::value_size(memory, lua, &Value::Table(table.clone()))?;
    let id = lua_string_bytes(&table, "id")?
        .ok_or_else(|| mlua::Error::RuntimeError("coordination.publish requires id".into()))?;
    let id_bytes = id.as_bytes();
    let id_text = utf8_slice(&id_bytes)?;
    let content_type_lua = lua_string_bytes(&table, "content_type")?;
    let content_type_bytes = content_type_lua.as_ref().map(mlua::String::as_bytes);
    let content_type = match &content_type_bytes {
        Some(bytes) => Some(utf8_slice(bytes)?),
        None => None,
    };
    let content_bytes = content_type
        .map(str::len)
        .unwrap_or("application/json".len());
    let body_lua = lua_string_bytes(&table, "body")?;
    let body_bytes = body_lua.as_ref().map(mlua::String::as_bytes);
    let body = match &body_bytes {
        Some(bytes) => Some(utf8_slice(bytes)?),
        None => None,
    };
    let body_len = body.map(str::len).unwrap_or(0);
    let extension = table.raw_get::<Value>("extension")?;
    let (extension_bytes, extension_admission) = match &extension {
        Value::Nil => (0, None),
        value => {
            let admission = lua_json::value_size(memory, lua, value)?;
            (admission.json_bytes, Some(admission))
        }
    };
    let scratch_peak = extension_admission
        .as_ref()
        .map(|admission| admission.scratch_peak)
        .unwrap_or(0);
    let created_at = lua_u64(table.raw_get::<Value>("created_at")?).unwrap_or(0);
    let target_table = match table.raw_get::<Value>("target")? {
        Value::Table(target) => target,
        _ => {
            return Err(
                mlua::Error::RuntimeError("coordination.publish requires target".into()).into(),
            );
        }
    };
    let (target_kind, first, second) = lua_target_strings(&target_table)?;
    let first_bytes = first.as_bytes();
    let first_text = utf8_slice(&first_bytes)?;
    let second_bytes = second.as_ref().map(mlua::String::as_bytes);
    let second_text = match &second_bytes {
        Some(bytes) => Some(utf8_slice(bytes)?),
        None => None,
    };
    let source_len = "plugin:"
        .len()
        .checked_add(plugin_key.0.len())
        .ok_or(AdmissionError::Capacity)?;
    let payload = id_text
        .len()
        .checked_add(source_len)
        .and_then(|bytes| bytes.checked_add(content_bytes))
        .and_then(|bytes| bytes.checked_add(body_len))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<EnvelopeTarget>()))
        .and_then(|bytes| bytes.checked_add(first_text.len()))
        .and_then(|bytes| bytes.checked_add(second_text.map(str::len).unwrap_or(0)))
        .and_then(|bytes| bytes.checked_add(extension_bytes))
        .and_then(|bytes| bytes.checked_add(scratch_peak))
        .ok_or(AdmissionError::Capacity)?;
    let mut entry = admit_callback_bytes(memory, payload)?;
    let extension_scratch = match &extension_admission {
        Some(admission) => Some(admission.bind(&mut entry)?),
        None => None,
    };
    let target = envelope_target_from_parts(target_kind, first_text, second_text)?;
    let content_type = match content_type {
        Some(text) => exact_string(text),
        None => exact_string("application/json"),
    };
    let body = match body {
        Some(text) => exact_bytes(text),
        None => Vec::new(),
    };
    let extension = match extension {
        Value::Nil => None,
        value => Some(BoundaryJson(lua_json::value_build(
            lua,
            &value,
            extension_scratch.expect("sized extension has prepaid scratch"),
        )?)),
    };
    let mut targets = Vec::with_capacity(1);
    targets.push(target);
    debug_assert_eq!(targets.len(), targets.capacity());
    Ok((
        PendingCoordinationOperation::Publish {
            envelope: RoutedEnvelope::new(
                EnvelopeId(exact_string(id_text)),
                EndpointId(plugin_source(&plugin_key.0)),
                targets,
                RoutedEnvelopePayload {
                    content_type,
                    body,
                    extension,
                },
                created_at,
            ),
        },
        entry,
    ))
}

fn plugin_source(plugin_key: &str) -> String {
    let mut source = String::with_capacity("plugin:".len() + plugin_key.len());
    source.push_str("plugin:");
    source.push_str(plugin_key);
    source
}

fn exact_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    out.push_str(text);
    out
}

fn exact_bytes(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len());
    out.extend_from_slice(text.as_bytes());
    out
}

fn utf8_slice(bytes: &[u8]) -> Result<&str, mlua::Error> {
    std::str::from_utf8(bytes)
        .map_err(|_| mlua::Error::RuntimeError("coordination requires UTF-8 strings".into()))
}

fn lua_u64(value: Value) -> Option<u64> {
    match value {
        Value::Integer(value) if value >= 0 => Some(value as u64),
        Value::Number(value) if value.is_finite() && value >= 0.0 && value.fract() == 0.0 => {
            Some(value as u64)
        }
        _ => None,
    }
}

fn admit_drain_operation(
    memory: &Arc<LuaMemoryAccount>,
    lua: &Lua,
    args: Value,
) -> Result<(PendingCoordinationOperation, LuaCallbackCharge), AdmissionError> {
    let table = lua_table(args)?;
    lua_json::value_size(memory, lua, &Value::Table(table.clone()))?;
    let target_table = match table.raw_get::<Value>("target")? {
        Value::Table(target) => target,
        _ => {
            return Err(
                mlua::Error::RuntimeError("coordination.drain requires target".into()).into(),
            );
        }
    };
    let (target_kind, first, second) = lua_target_strings(&target_table)?;
    let first_bytes = first.as_bytes();
    let first_text = utf8_slice(&first_bytes)?;
    let second_bytes = second.as_ref().map(mlua::String::as_bytes);
    let second_text = match &second_bytes {
        Some(bytes) => Some(utf8_slice(bytes)?),
        None => None,
    };
    let after = lua_u64(table.raw_get::<Value>("after")?).map(EnvelopeCursor);
    let limit = match lua_u64(table.raw_get::<Value>("limit")?) {
        Some(value) => usize::try_from(value).unwrap_or(16),
        None => 16,
    };
    let payload = first_text
        .len()
        .checked_add(second_text.map(str::len).unwrap_or(0))
        .and_then(|bytes| {
            bytes.checked_add(after.map_or(0, |_| std::mem::size_of::<EnvelopeCursor>()))
        })
        .ok_or(AdmissionError::Capacity)?;
    let entry = admit_callback_bytes(memory, payload)?;
    let target = envelope_target_from_parts(target_kind, first_text, second_text)?;
    Ok((
        PendingCoordinationOperation::Drain {
            target,
            after,
            limit,
        },
        entry,
    ))
}

#[derive(Clone, Copy)]
enum TargetKind {
    Endpoint,
    Client,
    Session,
    Subscription,
    Plugin,
    Stream,
    Topic,
}

fn lua_target_strings(
    table: &Table,
) -> Result<(TargetKind, mlua::String, Option<mlua::String>), mlua::Error> {
    let kind = lua_string_bytes(table, "type")?
        .ok_or_else(|| mlua::Error::RuntimeError("coordination target.type is required".into()))?;
    let kind_bytes = kind.as_bytes();
    let kind = utf8_slice(&kind_bytes)?;
    let (target_kind, first_key, second_key) = match kind {
        "endpoint" => (TargetKind::Endpoint, "endpoint_id", None),
        "client" => (TargetKind::Client, "client_id", None),
        "session" => (TargetKind::Session, "session_id", None),
        "subscription" => (
            TargetKind::Subscription,
            "session_id",
            Some("subscription_id"),
        ),
        "plugin" => (TargetKind::Plugin, "plugin_key", None),
        "stream" => (TargetKind::Stream, "stream", None),
        "topic" => (TargetKind::Topic, "topic", None),
        _ => {
            return Err(mlua::Error::RuntimeError(
                "coordination target.type is not recognized".into(),
            ));
        }
    };
    let first = lua_string_bytes(table, first_key)?.ok_or_else(|| {
        mlua::Error::RuntimeError(format!("coordination target.{first_key} is required"))
    })?;
    let second = match second_key {
        Some(key) => Some(lua_string_bytes(table, key)?.ok_or_else(|| {
            mlua::Error::RuntimeError(format!("coordination target.{key} is required"))
        })?),
        None => None,
    };
    Ok((target_kind, first, second))
}

fn envelope_target_from_parts(
    kind: TargetKind,
    first: &str,
    second: Option<&str>,
) -> Result<EnvelopeTarget, mlua::Error> {
    Ok(match kind {
        TargetKind::Endpoint => EnvelopeTarget::Endpoint {
            endpoint_id: EndpointId(exact_string(first)),
        },
        TargetKind::Client => EnvelopeTarget::Client {
            client_id: botster_core::ClientId(exact_string(first)),
        },
        TargetKind::Session => EnvelopeTarget::Session {
            session_id: botster_core::SessionId(exact_string(first)),
        },
        TargetKind::Subscription => EnvelopeTarget::Subscription {
            session_id: botster_core::SessionId(exact_string(first)),
            subscription_id: botster_core::SubscriptionId(exact_string(second.ok_or_else(
                || {
                    mlua::Error::RuntimeError(
                        "coordination target.subscription_id is required".into(),
                    )
                },
            )?)),
        },
        TargetKind::Plugin => EnvelopeTarget::Plugin {
            plugin_key: PluginKey(exact_string(first)),
        },
        TargetKind::Stream => EnvelopeTarget::Stream {
            stream: exact_string(first),
        },
        TargetKind::Topic => EnvelopeTarget::Topic {
            topic: exact_string(first),
        },
    })
}

fn registration_from_value(
    lua: &Lua,
    value: Value,
) -> Result<LuaRegistration, LuaPluginRuntimeError> {
    let value = match value {
        Value::Nil => lua.globals().get::<Value>("__botster_registration")?,
        value => value,
    };
    let Value::Table(registration) = value else {
        return Err(LuaPluginRuntimeError::Lua(
            "plugin entrypoint must return botster.register({...})".to_string(),
        ));
    };
    let mut tools = Vec::new();
    if let Ok(tool_table) = registration.get::<Table>("tools") {
        for tool in tool_table.sequence_values::<Table>() {
            let tool = tool.map_err(LuaPluginRuntimeError::from)?;
            let input_schema = match tool.get::<Value>("input_schema") {
                Ok(value) => lua
                    .from_value::<serde_json::Value>(value)
                    .map_err(LuaPluginRuntimeError::from)?,
                Err(_) => empty_object(),
            };
            tools.push(LuaToolRegistration {
                name: tool.get("name").map_err(LuaPluginRuntimeError::from)?,
                description: tool
                    .get("description")
                    .map_err(LuaPluginRuntimeError::from)?,
                input_schema,
                handler: tool.get("handler").map_err(LuaPluginRuntimeError::from)?,
            });
        }
    }
    let mut handlers = Vec::new();
    if let Ok(handler_table) = registration.get::<Table>("handlers") {
        for handler in handler_table.sequence_values::<Table>() {
            let handler = handler.map_err(LuaPluginRuntimeError::from)?;
            let kind: String = handler.get("kind").map_err(LuaPluginRuntimeError::from)?;
            handlers.push(LuaHandlerRegistration {
                id: handler.get("id").map_err(LuaPluginRuntimeError::from)?,
                kind: handler_kind_from_lua(&kind)?,
                descriptor_id: handler
                    .get("descriptor_id")
                    .or_else(|_| handler.get("id"))
                    .map_err(LuaPluginRuntimeError::from)?,
                event_owner: handler
                    .get::<Option<String>>("event_owner")
                    .map_err(LuaPluginRuntimeError::from)?
                    .or_else(|| handler.get::<Option<String>>("owner").ok().flatten()),
                event_name: handler
                    .get::<Option<String>>("event")
                    .map_err(LuaPluginRuntimeError::from)?
                    .or_else(|| handler.get::<Option<String>>("event_name").ok().flatten()),
                body: match handler.get::<Value>("descriptor") {
                    Ok(value) => lua
                        .from_value::<serde_json::Value>(value)
                        .map_err(LuaPluginRuntimeError::from)?,
                    Err(_) => serde_json::Value::Null,
                },
            });
        }
    }
    Ok(LuaRegistration { tools, handlers })
}

fn descriptor_kind_for_handler_kind(kind: PluginHandlerKind) -> Option<PluginDescriptorKind> {
    match kind {
        PluginHandlerKind::SurfaceRoute => Some(PluginDescriptorKind::SurfaceRoute),
        PluginHandlerKind::UiAction => Some(PluginDescriptorKind::UiAction),
        PluginHandlerKind::EntityProvider => Some(PluginDescriptorKind::EntityProvider),
        _ => None,
    }
}

fn handler_kind_from_lua(kind: &str) -> Result<PluginHandlerKind, LuaPluginRuntimeError> {
    match kind {
        "ui_action" => Ok(PluginHandlerKind::UiAction),
        "session_action" => Ok(PluginHandlerKind::SessionAction),
        "command" => Ok(PluginHandlerKind::Command),
        "mcp_tool" => Ok(PluginHandlerKind::McpTool),
        "event" => Ok(PluginHandlerKind::Event),
        "hook" => Ok(PluginHandlerKind::Hook),
        "timer" => Ok(PluginHandlerKind::Timer),
        "surface_route" => Ok(PluginHandlerKind::SurfaceRoute),
        "entity_provider" => Ok(PluginHandlerKind::EntityProvider),
        other => Err(LuaPluginRuntimeError::Lua(format!(
            "unsupported lua handler kind: {other}"
        ))),
    }
}

fn failed(
    request: PluginInvocationRequest,
    kind: PluginInvocationFailureKind,
    reason: impl Into<String>,
) -> PluginInvocationResult {
    PluginInvocationResult::Failed(PluginInvocationFailure {
        request_id: request.request_id,
        handler: request.handler,
        kind,
        timeout_ms: Some(request.timeout_ms),
        reason: reason.into(),
    })
}

fn sanitize_lua_error(error: mlua::Error) -> String {
    let message = error.to_string();
    message
        .lines()
        .next()
        .unwrap_or("lua runtime error")
        .replace('\\', "/")
}

#[cfg(test)]
mod bounded_session_type_tests {
    use super::*;
    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::lua_memory::LuaMemoryLimits;
    use crate::persistence::DeviceSessionTypeSource;
    use crate::runtime::{HubSessionTypeSpawner, HubStatePublication};
    use crate::session_types::{
        PackageSessionType, PackageSessionTypeExecution, PackageSessionTypeWorkingDirectory,
    };
    use crate::spawn_targets::SpawnTarget;
    use std::collections::BTreeMap;

    fn state() -> SharedSpawnTargets {
        state_with_catalog(None, 1)
    }

    fn state_with_catalog(description: Option<String>, count: usize) -> SharedSpawnTargets {
        let root = std::env::temp_dir().join("botster-bounded-session-type-lua-test");
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let mut state = crate::persistence::HubState::from_config(&config);
        state.spawn_targets.push(SpawnTarget {
            target_id: "target".to_string(),
            label: "Target".to_string(),
            root: root.clone(),
            enabled: true,
            kind: "directory".to_string(),
            base_ref: None,
            metadata: BTreeMap::new(),
        });
        state
            .device_session_type_sources
            .push(DeviceSessionTypeSource {
                root,
                session_types: vec![PackageSessionType {
                    id: "agent".to_string(),
                    label: "Agent".to_string(),
                    description,
                    icon: None,
                    role: "botster.agent".to_string(),
                    interaction: "interactive".to_string(),
                    traits: Vec::new(),
                    lifecycle: "task".to_string(),
                    execution: PackageSessionTypeExecution::RelativeExecutable,
                    command: "bin/agent".to_string(),
                    args: Vec::new(),
                    working_directory: PackageSessionTypeWorkingDirectory::PackageRoot,
                    environment: BTreeMap::new(),
                    allowed_environment_overrides: Vec::new(),
                    context: Vec::new(),
                    target_id: None,
                }],
            });
        let definitions = &mut state.device_session_type_sources[0].session_types;
        for index in 1..count {
            let mut definition = definitions[0].clone();
            definition.id = format!("agent-{index}");
            definitions.push(definition);
        }
        Arc::new(HubStatePublication::new(state).unwrap())
    }

    fn account(callback: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: callback,
            total_callback_bytes: callback,
        })
        .unwrap()
    }

    #[test]
    fn session_type_errors_remain_lua_owned_after_callback_charge_releases() {
        let lua = Lua::new();
        lua.set_memory_limit(64 * 1024).unwrap();
        let memory = account(1);
        let table = session_types_table(
            &lua,
            PluginKey("test.plugin".to_string()),
            Arc::new(HubSessionTypeSpawner::new()),
            state(),
            Vec::new(),
            Some(Arc::clone(&memory)),
        )
        .unwrap();
        lua.globals().set("session_types", table).unwrap();

        // Retained errors must not contain Rust-backed callback failures.
        let retained: usize = lua
            .load(
                r#"
                retained_errors = {}
                for index = 1, 64 do
                    local ok, err = pcall(session_types.list, {target_id = "target"})
                    assert(not ok)
                    assert(type(err) == "string")
                    assert(string.find(tostring(err), "exceeded the Lua callback memory limit", 1, true))
                    retained_errors[index] = err
                end
                collectgarbage("collect")
                return #retained_errors
                "#,
            )
            .eval()
            .unwrap();
        assert_eq!(retained, 64);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn session_type_read_wrapper_checks_arguments_before_rust_conversion() {
        let lua = Lua::new();
        let memory = account(64 * 1024);
        let table = session_types_table(
            &lua,
            PluginKey("test.plugin".to_string()),
            Arc::new(HubSessionTypeSpawner::new()),
            state(),
            Vec::new(),
            Some(Arc::clone(&memory)),
        )
        .unwrap();
        lua.globals().set("session_types", table).unwrap();
        lua.globals()
            .set(
                "foreign_failure",
                lua.create_function(|_, ()| -> mlua::Result<()> {
                    Err(mlua::Error::RuntimeError("foreign failure".to_owned()))
                })
                .unwrap(),
            )
            .unwrap();
        lua.load(
            r#"
            local function fails(fn, args, expected)
                local ok, err = pcall(fn, args)
                assert(not ok)
                assert(type(err) == "string")
                assert(string.find(err, expected, 1, true), err)
            end
            fails(session_types.list, nil, "argument table")
            fails(session_types.list, false, "argument table")
            fails(session_types.list, {target_id = {}}, "requires target_id")
            fails(session_types.list, {target_id = 42}, "requires target_id")
            fails(session_types.list, {target_id = "\255"}, "UTF-8 target_id")
            fails(session_types.list, {target_id = "   "}, "nonblank")
            fails(session_types.show, {target_id = "target"}, "requires session_type_id")
            fails(session_types.show, {target_id = "target", session_type_id = "\255"}, "UTF-8 session_type_id")
            fails(session_types.list, {target_id = "missing"}, "session_types.list failed:")

            local ok, foreign = pcall(foreign_failure)
            assert(not ok)
            assert(type(foreign) == "userdata")
            fails(session_types.list, foreign, "argument table")
            fails(session_types.list, {target_id = foreign}, "requires target_id")
            fails(session_types.show, {target_id = "target", session_type_id = foreign}, "requires session_type_id")

            local reads = 0
            local args = setmetatable({}, {__index = function(_, key)
                reads = reads + 1
                assert(key == "target_id")
                return "target"
            end})
            assert(#session_types.list(args, {}, {}, {}) == 1)
            assert(reads == 1)

            local saved_type, saved_error, saved_getmetatable, saved_pcall = type, error, getmetatable, pcall
            type, error, getmetatable, pcall = false, false, false, false
            local success, result = saved_pcall(session_types.list, {target_id = "target"})
            local failed, message = saved_pcall(session_types.list, {target_id = "missing"})
            type, error, getmetatable, pcall = saved_type, saved_error, saved_getmetatable, saved_pcall
            assert(success and #result == 1)
            assert(not failed and string.find(message, "session_types.list failed:", 1, true))
            "#,
        )
        .exec()
        .unwrap();
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn session_type_read_conversion_failure_returns_a_lua_string() {
        let lua = Lua::new();
        let memory = account(256 * 1024);
        let table = session_types_table(
            &lua,
            PluginKey("test.plugin".to_string()),
            Arc::new(HubSessionTypeSpawner::new()),
            state_with_catalog(Some("x".repeat(1024)), 16),
            Vec::new(),
            Some(Arc::clone(&memory)),
        )
        .unwrap();
        lua.globals().set("session_types", table).unwrap();
        let call: mlua::Function = lua
            .load(
                r#"
                local args = {target_id = "target"}
                return function()
                    local ok, result = pcall(session_types.list, args)
                    return ok, type(result), result
                end
                "#,
            )
            .eval()
            .unwrap();
        let (success, _, result): (bool, mlua::String, Value) = call.call(()).unwrap();
        assert!(success, "warmup failed: {result:?}");
        drop(result);
        lua.gc_collect().unwrap();
        // Permit wrapper execution, but not all 16 KiB of result descriptions.
        lua.set_memory_limit(lua.used_memory() + 4 * 1024).unwrap();
        let result = call.call::<(bool, mlua::String, mlua::String)>(());
        lua.set_memory_limit(0).unwrap();
        let (success, kind, message) = result.unwrap();
        assert!(!success);
        assert_eq!(kind.to_str().unwrap(), "string");
        assert!(
            message
                .to_str()
                .unwrap()
                .contains("could not allocate its Lua result"),
            "unexpected failure: {message:?}"
        );
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn bounded_session_type_lua_callbacks_succeed_refuse_and_release_account() {
        let lua = Lua::new();
        let memory = account(64 * 1024);
        let table = session_types_table(
            &lua,
            PluginKey("test.plugin".to_string()),
            Arc::new(HubSessionTypeSpawner::new()),
            state(),
            Vec::new(),
            Some(Arc::clone(&memory)),
        )
        .unwrap();
        lua.globals().set("session_types", table).unwrap();
        let value: serde_json::Value = lua
            .from_value(
                lua.load(
                    r#"return {
                      list = session_types.list({target_id = "target"}),
                      shown = session_types.show({target_id = "target", session_type_id = "device/agent"})
                    }"#,
                )
                .eval::<Value>()
                .unwrap(),
            )
            .unwrap();
        assert_eq!(value["list"].as_array().unwrap().len(), 1);
        assert_eq!(value["shown"]["session_type_id"], "device/agent");
        assert_eq!(memory.usage().1, 0);

        let held = memory.reserve_callback().unwrap();
        let (success, message): (bool, mlua::String) = lua
            .load(r#"return pcall(session_types.list, {target_id = "target"})"#)
            .eval()
            .unwrap();
        assert!(!success);
        assert_eq!(
            message.to_str().unwrap(),
            "Lua callback memory capacity exhausted"
        );
        assert_eq!(memory.usage().1, 64 * 1024);
        drop(held);
        assert_eq!(memory.usage().1, 0);

        let refused = account(1);
        let table = session_types_table(
            &lua,
            PluginKey("test.plugin".to_string()),
            Arc::new(HubSessionTypeSpawner::new()),
            state(),
            Vec::new(),
            Some(Arc::clone(&refused)),
        )
        .unwrap();
        lua.globals().set("session_types", table).unwrap();
        let error = lua
            .load(r#"return session_types.list({target_id = "target"})"#)
            .eval::<Value>()
            .expect_err("one-byte callback projection must be refused");
        assert!(
            error
                .to_string()
                .contains("exceeded the Lua callback memory limit")
        );
        assert_eq!(refused.usage().1, 0);
    }
}

#[cfg(test)]
mod terminal_bridge_tests {
    use super::*;
    use crate::host_executor::{HostExecutor, HostJobIdentity};
    use crate::owner_identity::WaiterId;

    pub(super) struct PendingDropGate {
        released: Arc<(Mutex<bool>, std::sync::Condvar)>,
        entered: mpsc::Receiver<(String, bool)>,
    }

    impl PendingDropGate {
        pub(super) fn wait(&self) {
            let (worker, unlocked) = self.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(worker.starts_with("botster-hub-host"));
            assert!(
                unlocked,
                "actual pending payload destruction must follow queue unlock"
            );
        }

        pub(super) fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }

    impl Drop for PendingDropGate {
        fn drop(&mut self) {
            self.release();
        }
    }

    pub(super) fn pending_drop_gate(
        unlocked: impl FnOnce() -> bool + Send + 'static,
    ) -> (PendingDropGate, Box<dyn Send>) {
        struct Probe {
            released: Arc<(Mutex<bool>, std::sync::Condvar)>,
            entered: mpsc::Sender<(String, bool)>,
            unlocked: Option<Box<dyn FnOnce() -> bool + Send>>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let unlocked = self.unlocked.take().unwrap()();
                let _ = self.entered.send((
                    thread::current().name().unwrap_or("unnamed").to_string(),
                    unlocked,
                ));
                let mut released = self.released.0.lock().unwrap();
                while !*released {
                    released = self.released.1.wait(released).unwrap();
                }
            }
        }
        let released = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (entered, receiver) = mpsc::channel();
        let probe = Probe {
            released: released.clone(),
            entered,
            unlocked: Some(Box::new(unlocked)),
        };
        (
            PendingDropGate {
                released,
                entered: receiver,
            },
            Box::new(probe),
        )
    }

    pub(super) fn host_clear(
        clear: impl FnOnce() -> bool + Send + 'static,
    ) -> (
        HostExecutor,
        crate::host_disposal::Job,
        mpsc::Receiver<bool>,
    ) {
        struct Clear {
            action: Option<Box<dyn FnOnce() -> bool + Send>>,
            finished: mpsc::Sender<bool>,
        }
        impl Drop for Clear {
            fn drop(&mut self) {
                let cleared = self.action.take().unwrap()();
                let _ = self.finished.send(cleared);
            }
        }
        let executor = HostExecutor::new();
        let (finished, receiver) = mpsc::channel();
        let job = crate::host_disposal::Job::new(crate::host_disposal::Parts {
            storage: None,
            identity: HostJobIdentity::first(WaiterId(31)),
            permit: executor.try_reserve().unwrap(),
            model: None,
            payload: Box::new(Clear {
                action: Some(Box::new(clear)),
                finished,
            }),
        });
        (executor, job, receiver)
    }

    pub(super) fn finish_host_clear(executor: &HostExecutor, job: &mut crate::host_disposal::Job) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match job.poll() {
                crate::host_disposal::Poll::Disposed(permit) => {
                    drop(permit);
                    break;
                }
                crate::host_disposal::Poll::Pending => assert!(Instant::now() < deadline),
                _ => panic!("actual pending destruction must produce the outer disposal receipt"),
            }
            thread::yield_now();
        }
        assert_eq!(executor.outstanding(), 0);
        assert_eq!(executor.prepared_bytes(), 0);
    }

    #[test]
    fn terminal_coordination_pending_clears_actual_queue_on_host_even_after_poison() {
        for poisoned in [false, true] {
            let bridge = HubCoordinationBridge::test_new();
            let responses: Vec<_> = (0..2)
                .map(|_| {
                    bridge.test_queue_pending(PendingCoordinationOperation::Drain {
                        target: EnvelopeTarget::Topic {
                            topic: "retained-coordination-payload".repeat(256),
                        },
                        after: None,
                        limit: 7,
                    })
                })
                .collect();
            let queue = bridge.pending.clone();
            let (gate, probe) = pending_drop_gate(move || {
                !matches!(queue.try_lock(), Err(std::sync::TryLockError::WouldBlock))
            });
            bridge.test_set_pending_drop_probe(probe);
            if poisoned {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _guard = bridge.pending.lock().unwrap();
                        panic!("retained coordination queue poison");
                    }))
                    .is_err()
                );
            }
            assert_eq!(bridge.test_pending_count(), 2);
            let worker_bridge = bridge.clone();
            let (executor, mut job, finished) =
                host_clear(move || worker_bridge.dispose_terminal_pending());
            gate.wait();
            assert_eq!(bridge.test_pending_count(), 0);
            assert!(matches!(job.poll(), crate::host_disposal::Poll::Pending));
            assert!(matches!(
                finished.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            for response in &responses {
                assert!(
                    matches!(response.try_recv(), Err(mpsc::TryRecvError::Empty)),
                    "the extracted requests still own their response senders at the payload gate"
                );
            }
            assert_eq!(executor.outstanding(), 1);
            gate.release();
            assert!(finished.recv_timeout(Duration::from_secs(5)).unwrap());
            finish_host_clear(&executor, &mut job);
            for response in responses {
                assert!(matches!(
                    response.try_recv(),
                    Err(mpsc::TryRecvError::Disconnected)
                ));
            }
            assert_eq!(
                bridge.test_pending_count(),
                0,
                "a live Owner alias observes actual clearing, not last-Arc drop"
            );
            assert_eq!(
                bridge.pending.is_poisoned(),
                poisoned,
                "terminal cleanup does not repair ordinary operation"
            );
        }
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use botster_core::{
        PluginCompletion, PluginInvocationClass, PluginInvocationContext, RequestId,
    };

    fn invoke_lua(source: &str, origin: &str, kind: PluginHandlerKind) -> PluginInvocationResult {
        invoke_lua_with_setup(source, origin, kind, |_| {})
    }

    fn invoke_lua_with_setup(
        source: &str,
        origin: &str,
        kind: PluginHandlerKind,
        setup: impl FnOnce(&Lua),
    ) -> PluginInvocationResult {
        let memory = LuaMemoryAccount::new(crate::config::lua_memory_limits()).unwrap();
        let state = LuaState::new(memory.reserve_vm().unwrap()).unwrap();
        let lua = state.lua();
        lua.set_memory_limit(memory.limits().per_vm_bytes).unwrap();
        setup(lua);
        let handlers = lua.create_table().expect("create handler registry");
        let handler = lua
            .load(source)
            .eval::<Function>()
            .expect("load Lua handler");
        handlers.set("run", handler).expect("register Lua handler");
        lua.globals()
            .set("__botster_handlers", handlers)
            .expect("install handler registry");
        let plugin_key = PluginKey("completion-test".to_string());
        let runtime = LuaPluginRuntime {
            plugin_key: plugin_key.clone(),
            lua: Mutex::new(state),
            instruction_budget: Arc::new(AtomicU64::new(DEFAULT_INSTRUCTION_BUDGET)),
            stopped: AtomicBool::new(false),
        };
        runtime.invoke(
            PluginInvocationRequest {
                request_id: RequestId("completion-test-request".to_string()),
                handler: PluginHandlerRef {
                    plugin_key,
                    kind,
                    handler_id: "run".to_string(),
                },
                timeout_ms: 1_000,
                context: PluginInvocationContext {
                    client_id: None,
                    session_id: None,
                    subscription_id: None,
                    surface_id: None,
                    origin: Some(origin.to_string()),
                    metadata: None,
                },
                payload: BoundaryJson(json!({})),
            },
            PluginCancellationToken::new(),
        )
    }

    #[test]
    fn callback_errors_preserve_root_messages_and_request_identity() {
        for message in ["host callback failure", "runtime error: actual plugin text"] {
            let mut failures = Vec::new();
            for previous in [true, false] {
                let result = invoke_lua_with_setup(
                    "return function() callback() end",
                    "request-response",
                    PluginHandlerKind::McpTool,
                    |lua| {
                        let function = move |_: &Lua, ()| -> mlua::Result<Value> {
                            Err(mlua::Error::RuntimeError(message.to_owned()))
                        };
                        let callback = if previous {
                            lua.create_function(function).unwrap()
                        } else {
                            callback::create(lua, function).unwrap()
                        };
                        lua.globals().set("callback", callback).unwrap();
                    },
                );
                let PluginInvocationResult::Failed(failure) = result else {
                    panic!("callback failure must reach the invocation result");
                };
                assert_eq!(failure.kind, PluginInvocationFailureKind::HandlerFailed);
                assert_eq!(failure.request_id.0, "completion-test-request");
                assert_eq!(failure.handler.handler_id, "run");
                assert_eq!(failure.reason, format!("runtime error: {message}"));
                failures.push(failure.reason);
            }
            assert_eq!(failures[0], failures[1]);
        }
    }

    #[test]
    fn large_background_returns_produce_unit_acknowledgements() {
        for origin in [
            PACKAGE_EVENT_INVOCATION_ORIGIN,
            SESSION_FAMILY_INVOCATION_ORIGIN,
        ] {
            let result = invoke_lua(
                "return function() return { value = string.rep('x', 2 * 1024 * 1024) } end",
                origin,
                PluginHandlerKind::Event,
            );
            assert!(matches!(
                &result,
                PluginInvocationResult::Completed(PluginInvocationSuccess { payload: None, .. })
            ));
            let completion = PluginCompletion {
                class: PluginInvocationClass::Background,
                result,
            };
            assert!(
                serde_json::to_vec(&completion)
                    .expect("encode acknowledgement")
                    .len()
                    < 4 * 1024
            );
        }
    }

    #[test]
    fn background_lua_errors_remain_handler_failures() {
        for origin in [
            PACKAGE_EVENT_INVOCATION_ORIGIN,
            SESSION_FAMILY_INVOCATION_ORIGIN,
        ] {
            let result = invoke_lua(
                "return function() error('background execution failed') end",
                origin,
                PluginHandlerKind::Event,
            );
            let PluginInvocationResult::Failed(failure) = result else {
                panic!("a Lua error must remain a failure");
            };
            assert_eq!(failure.kind, PluginInvocationFailureKind::HandlerFailed);
            assert!(failure.reason.contains("background execution failed"));
        }
    }

    #[test]
    fn request_response_lua_results_keep_their_payloads() {
        for (origin, kind) in [
            ("request-response", PluginHandlerKind::McpTool),
            ("request-response", PluginHandlerKind::Event),
            (PACKAGE_EVENT_INVOCATION_ORIGIN, PluginHandlerKind::McpTool),
        ] {
            let result = invoke_lua(
                "return function() return { value = string.rep('x', 8192), count = 7 } end",
                origin,
                kind,
            );
            let PluginInvocationResult::Completed(success) = result else {
                panic!("the request must succeed");
            };
            assert_eq!(
                success.payload.expect("retain the response payload").0,
                json!({ "value": "x".repeat(8192), "count": 7 })
            );
        }
    }
}

#[cfg(test)]
mod collection_capacity_tests;
#[cfg(test)]
mod coordination_lifecycle_tests;
