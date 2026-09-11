//! Input admission for `coordination.acknowledge`.

use std::sync::Arc;

use botster_core::{
    ClientId, EndpointId, EnvelopeId, EnvelopeTarget, PluginKey, SessionId, SubscriptionId,
};
use botster_core_daemon::{AcknowledgeRoutedEnvelopeRequest, CoreDaemon};
use mlua::{Function, Lua, LuaSerdeExt, Table, Value};

use super::HubCoordinationBridge;
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryAccount};

// Each constructor requires charges that the producer has already admitted.
pub(crate) mod ownership {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use botster_core::{RoutedEnvelopeDrainOutcome, RoutedEnvelopePublishOutcome};
    use botster_core_daemon::RoutedEnvelopeDeliveryStateResult;

    use crate::lua_memory::{
        LuaCallbackAdmissionError, LuaCallbackCharge, LuaCallbackStorageLease, LuaMemoryAccount,
    };

    /// Each field specifies an allocation allowance for one acknowledgement.
    /// The caller must supply every field. No production values are selected here.
    pub(crate) struct AcknowledgeSizing {
        pub(crate) input: usize,
        pub(crate) lookup: usize,
        pub(crate) result: usize,
        pub(crate) error_string: usize,
        pub(crate) conversion: usize,
        pub(crate) core_request: usize,
        pub(crate) core_reply: usize,
        pub(crate) callback_reply: usize,
        pub(crate) caller: usize,
        pub(crate) continuation: usize,
        pub(crate) disposal: usize,
    }

    impl AcknowledgeSizing {
        pub(crate) fn for_input(input: usize) -> Option<Self> {
            use crate::lua_memory::layout;
            use crate::lua_runtime::PendingCoordinationOperation;

            let core_error = core_error_bytes();
            let host_error = CoordinationRefusal::ALL.iter()
                .map(|refusal| refusal.message().len()).max()?;
            Some(Self {
                input,
                lookup: input,
                result: input,
                error_string: core_error.checked_add(host_error)?,
                conversion: core_conversion_bytes()?.checked_add(17)?,
                core_request: crate::data_plane::driver::retained_request_bytes::<
                    CoordinationReply, PendingCoordinationOperation,
                >(),
                core_reply: crate::data_plane::driver::retained_reply_bytes::<CoordinationReply>()?,
                callback_reply: layout::single_reply_bytes::<CoordinationReply>(true)?,
                caller: layout::arc_bytes::<AtomicU8>().checked_add(layout::lease_bytes())?,
                continuation: crate::daemon::control::coordination::continuation_bytes(),
                disposal: crate::daemon::control::coordination::disposal_bytes()?,
            })
        }

        pub(crate) fn total(&self) -> Option<usize> {
            self.input
                .checked_add(self.lookup)?
                .checked_add(self.result)?
                .checked_add(self.error_string)?
                .checked_add(self.conversion)?
                .checked_add(self.core_request)?
                .checked_add(self.core_reply)?
                .checked_add(self.callback_reply)?
                .checked_add(self.caller)?
                .checked_add(self.continuation)?
                .checked_add(self.disposal)
        }

        pub(crate) fn admit(
            &self,
            memory: &Arc<LuaMemoryAccount>,
        ) -> Result<AcknowledgeCharges, LuaCallbackAdmissionError> {
            let bytes = self.total().ok_or(LuaCallbackAdmissionError::Quota)?;
            let mut admitted = memory.reserve_callback_total(bytes)?;
            Ok(AcknowledgeCharges {
                input: admitted.split(self.input).expect("checked input segment"),
                lookup: admitted.split(self.lookup).expect("checked lookup segment"),
                result: admitted.split(self.result).expect("checked result segment"),
                error_string: admitted.split(self.error_string).expect("checked error segment"),
                conversion: admitted.split(self.conversion).expect("checked conversion segment"),
                core_request: admitted.split(self.core_request).expect("checked Core request segment"),
                core_reply: admitted.split(self.core_reply).expect("checked Core reply segment"),
                callback_reply: admitted.split(self.callback_reply).expect("checked callback reply segment"),
                caller: admitted.split(self.caller).expect("checked caller segment"),
                continuation: admitted.split(self.continuation).expect("checked continuation segment"),
                disposal: admitted,
            })
        }
    }

    pub(crate) struct AcknowledgeCharges {
        pub(crate) input: LuaCallbackCharge,
        pub(crate) lookup: LuaCallbackCharge,
        pub(crate) result: LuaCallbackCharge,
        pub(crate) error_string: LuaCallbackCharge,
        pub(crate) conversion: LuaCallbackCharge,
        pub(crate) core_request: LuaCallbackCharge,
        pub(crate) core_reply: LuaCallbackCharge,
        pub(crate) callback_reply: LuaCallbackCharge,
        pub(crate) caller: LuaCallbackCharge,
        pub(crate) continuation: LuaCallbackCharge,
        pub(crate) disposal: LuaCallbackCharge,
    }

    pub(crate) const fn core_error_bytes() -> usize {
        // CoreDaemon::acknowledge_routed_envelope can return only Shutdown.
        // Display grows its String to at most twice the fixed message length.
        2 * "daemon is shut down".len()
    }

    pub(crate) fn core_conversion_bytes() -> Option<usize> {
        // Four temporary handles overlap the state-owned root result handle.
        4usize.checked_mul(crate::lua_memory::layout::lua_reference_bytes())?
            .checked_add(17)
    }

    pub(crate) struct CoordinationStorage {
        pub(crate) core: crate::data_plane::driver::CoreSubmissionStorage,
        pub(crate) continuation: LuaCallbackCharge,
        pub(crate) disposal: LuaCallbackCharge,
    }

    pub(crate) struct AcknowledgeTransport {
        pub(crate) work: CoordinationStorage,
        pub(crate) reply: LuaCallbackCharge,
        pub(crate) caller: LuaCallbackCharge,
        pub(crate) error: LuaCallbackCharge,
        pub(crate) conversion: LuaCallbackCharge,
    }

    /// The result remains owned until Lua conversion or local destruction ends.
    #[derive(Debug)]
    pub(crate) struct AcknowledgeOutcome {
        outcome: RoutedEnvelopeDeliveryStateResult,
        // Destroy the payload before releasing its allocation allowance.
        _charge: LuaCallbackCharge,
        _conversion_charge: LuaCallbackCharge,
    }

    impl AcknowledgeOutcome {
        pub(crate) fn new(
            outcome: RoutedEnvelopeDeliveryStateResult,
            charge: LuaCallbackCharge,
            conversion_charge: LuaCallbackCharge,
        ) -> Self {
            Self {
                outcome,
                _charge: charge,
                _conversion_charge: conversion_charge,
            }
        }

        pub(crate) fn outcome(&self) -> &RoutedEnvelopeDeliveryStateResult {
            &self.outcome
        }
    }

    /// The error keeps its allowance through conversion and failed delivery.
    #[derive(Debug)]
    pub(crate) struct AcknowledgeFailure {
        message: String,
        _charge: LuaCallbackCharge,
        _conversion_charge: LuaCallbackCharge,
    }

    impl AcknowledgeFailure {
        pub(crate) fn new(
            message: String,
            charge: LuaCallbackCharge,
            conversion_charge: LuaCallbackCharge,
        ) -> Self {
            Self {
                message,
                _charge: charge,
                _conversion_charge: conversion_charge,
            }
        }

        pub(crate) fn message(&self) -> &str {
            &self.message
        }
    }

    /// This candidate replaces the shared live enum when connection is authorized.
    #[derive(Debug)]
    pub(crate) enum CoordinationOutcome {
        Publish(RoutedEnvelopePublishOutcome),
        Drain(RoutedEnvelopeDrainOutcome),
        Acknowledge(AcknowledgeOutcome),
    }

    #[derive(Debug)]
    pub(crate) enum CoordinationFailure {
        NonAcknowledge(String),
        Acknowledge(AcknowledgeFailure),
    }

    pub(crate) type CoordinationReply = Result<CoordinationOutcome, CoordinationFailure>;

    impl CoordinationFailure {
        pub(crate) fn message(&self) -> &str {
            match self {
                Self::NonAcknowledge(message) => message,
                Self::Acknowledge(failure) => failure.message(),
            }
        }
    }

    pub(crate) enum CoordinationDelivery {
        Reply(CoordinationReply),
        Refused(CoordinationRefusal),
    }

    #[derive(Clone, Copy)]
    pub(crate) enum CoordinationRefusal {
        Registration,
        Abandoned,
        Full,
        Stopped,
        Lost,
        Unexpected,
        HelperStopped,
        HelperFull,
    }

    impl CoordinationRefusal {
        pub(crate) const ALL: [Self; 8] = [
            Self::Registration, Self::Abandoned, Self::Full, Self::Stopped,
            Self::Lost, Self::Unexpected, Self::HelperStopped, Self::HelperFull,
        ];

        pub(crate) const fn message(self) -> &'static str {
            match self {
                Self::Registration => "coordination completion registration was refused",
                Self::Abandoned => "coordination callback ended before admission",
                Self::Full => "coordination Core queue is full",
                Self::Stopped => "coordination Core driver stopped before admission",
                Self::Lost => "coordination Core result was lost",
                Self::Unexpected => "coordination acknowledge returned unexpected response",
                Self::HelperStopped => "core data-plane driver stopped",
                Self::HelperFull => "core request queue is full",
            }
        }
    }

    impl From<CoordinationReply> for CoordinationDelivery {
        fn from(reply: CoordinationReply) -> Self {
            Self::Reply(reply)
        }
    }

    /// The acknowledgement endpoint requires its admitted error and channel storage.
    pub(crate) enum CoordinationReplySender {
        NonAcknowledge(mpsc::Sender<CoordinationReply>),
        Acknowledge {
            sender: AcknowledgeReplySender,
            error: LuaCallbackCharge,
            conversion: LuaCallbackCharge,
        },
    }

    impl CoordinationReplySender {
        pub(crate) fn send(
            self,
            delivery: impl Into<CoordinationDelivery>,
        ) -> Result<(), mpsc::SendError<CoordinationReply>> {
            let delivery = delivery.into();
            match self {
                Self::NonAcknowledge(sender) => sender.send(match delivery {
                    CoordinationDelivery::Reply(reply) => reply,
                    CoordinationDelivery::Refused(message) => {
                        Err(CoordinationFailure::NonAcknowledge(message.message().to_owned()))
                    }
                }),
                Self::Acknowledge { sender, error, conversion } => {
                    let message = match delivery {
                        CoordinationDelivery::Reply(Ok(CoordinationOutcome::Acknowledge(outcome))) => {
                            return sender.send(Ok(outcome));
                        }
                        CoordinationDelivery::Reply(Err(CoordinationFailure::Acknowledge(failure))) => {
                            return sender.send(Err(failure));
                        }
                        CoordinationDelivery::Refused(message) => message.message(),
                        CoordinationDelivery::Reply(_) => {
                            CoordinationRefusal::Unexpected.message()
                        }
                    };
                    sender.send(Err(AcknowledgeFailure::new(
                        message.to_owned(),
                        error,
                        conversion,
                    )))
                }
            }
        }
    }

    pub(crate) struct AcknowledgeReplySender {
        sender: mpsc::SyncSender<CoordinationReply>,
        // Free this endpoint before releasing its storage lease.
        _storage: LuaCallbackStorageLease,
    }

    pub(crate) struct AcknowledgeReplyReceiver {
        receiver: mpsc::Receiver<CoordinationReply>,
        _storage: LuaCallbackStorageLease,
    }

    /// The charge covers the channel, message storage, selector, and lease Arc.
    pub(crate) fn reply_channel(
        charge: LuaCallbackCharge,
    ) -> (AcknowledgeReplySender, AcknowledgeReplyReceiver) {
        let storage = LuaCallbackStorageLease::new(charge);
        // The channel starts empty. This private, non-cloneable endpoint sends once.
        let (sender, receiver) = mpsc::sync_channel(1);
        (
            AcknowledgeReplySender {
                sender,
                _storage: storage.clone(),
            },
            AcknowledgeReplyReceiver {
                receiver,
                _storage: storage,
            },
        )
    }

    impl AcknowledgeReplySender {
        /// This endpoint cannot send an unfunded acknowledgement result or error.
        pub(crate) fn send(
            self,
            result: Result<AcknowledgeOutcome, AcknowledgeFailure>,
        ) -> Result<(), mpsc::SendError<CoordinationReply>> {
            self.sender.send(
                result
                    .map(CoordinationOutcome::Acknowledge)
                    .map_err(CoordinationFailure::Acknowledge),
            )
        }
    }

    impl AcknowledgeReplyReceiver {
        pub(crate) fn recv_timeout(
            &self,
            timeout: Duration,
        ) -> Result<CoordinationReply, mpsc::RecvTimeoutError> {
            self.receiver.recv_timeout(timeout)
        }
    }

    /// Every caller handle retains the allocation allowance until its Arc drops.
    #[derive(Clone)]
    pub(crate) struct AcknowledgeCaller {
        state: Arc<AtomicU8>,
        _storage: LuaCallbackStorageLease,
    }

    impl AcknowledgeCaller {
        /// The charge covers both the state Arc and the lease Arc.
        pub(crate) fn new(charge: LuaCallbackCharge) -> Self {
            let storage = LuaCallbackStorageLease::new(charge);
            Self {
                state: Arc::new(AtomicU8::new(0)),
                _storage: storage,
            }
        }

        pub(crate) fn claim(&self) -> bool {
            self.state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }

        #[cfg(test)]
        #[allow(dead_code)]
        pub(crate) fn guard(&self) -> AcknowledgeCallerGuard {
            AcknowledgeCallerGuard(self.clone())
        }

        pub(crate) fn finish(&self) {
            self.state.fetch_or(2, Ordering::AcqRel);
        }

        #[cfg(test)]
        pub(crate) fn test_bits(&self) -> u8 {
            self.state.load(Ordering::Acquire)
        }
    }

    pub(crate) struct AcknowledgeCallerGuard(AcknowledgeCaller);

    impl Drop for AcknowledgeCallerGuard {
        fn drop(&mut self) {
            self.0.finish();
        }
    }
}

pub(crate) struct AcknowledgeInput {
    pub(crate) input: AcknowledgeOperation,
    pub(crate) transport: ownership::AcknowledgeTransport,
}

/// The operation owns its input, lookup, result, and conversion allowances.
pub(crate) struct AcknowledgeOperation {
    request: AcknowledgeRoutedEnvelopeRequest,
    // Rust drops fields in declaration order. Free the strings before the charge.
    charge: LuaCallbackCharge,
    lookup: LuaCallbackCharge,
    result: LuaCallbackCharge,
    error: LuaCallbackCharge,
    conversion: LuaCallbackCharge,
}

impl AcknowledgeOperation {
    pub(crate) fn acknowledge(
        self,
        daemon: &mut CoreDaemon,
    ) -> Result<ownership::AcknowledgeOutcome, ownership::AcknowledgeFailure> {
        let Self { request, charge, lookup, result, error, conversion } = self;
        let outcome = daemon.acknowledge_routed_envelope(request);
        drop(charge);
        drop(lookup);
        match outcome {
            Ok(outcome) => Ok(ownership::AcknowledgeOutcome::new(outcome, result, conversion)),
            Err(failure) => Err(ownership::AcknowledgeFailure::new(
                failure.to_string(), error, conversion,
            )),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AdmissionError {
    Quota,
    Capacity,
    InvalidTarget,
}

pub(crate) fn admit(
    memory: &Arc<LuaMemoryAccount>,
    kind: &str,
    first: &str,
    second: &str,
    envelope_id: &str,
) -> Result<AcknowledgeInput, AdmissionError> {
    let bytes = first
        .len()
        .checked_add(second.len())
        .and_then(|bytes| bytes.checked_add(envelope_id.len()))
        .ok_or(AdmissionError::Quota)?;
    let sizing = ownership::AcknowledgeSizing::for_input(bytes).ok_or(AdmissionError::Quota)?;
    let mut charges = sizing.admit(memory).map_err(|error| match error {
        crate::lua_memory::LuaCallbackAdmissionError::Quota => AdmissionError::Quota,
        crate::lua_memory::LuaCallbackAdmissionError::Capacity(_) => AdmissionError::Capacity,
    })?;
    let core_error = charges.error_string.split(ownership::core_error_bytes())
        .expect("admitted Core error segment");
    let core_conversion = charges.conversion.split(
        ownership::core_conversion_bytes().expect("admitted conversion size"),
    ).expect("admitted Core conversion segment");
    // The trusted Lua wrapper supplies a known variant and only its consumed fields.
    // Each String requests its exact byte length after admission.
    let target = match kind {
        "endpoint" => EnvelopeTarget::Endpoint {
            endpoint_id: EndpointId(first.to_owned()),
        },
        "client" => EnvelopeTarget::Client {
            client_id: ClientId(first.to_owned()),
        },
        "session" => EnvelopeTarget::Session {
            session_id: SessionId(first.to_owned()),
        },
        "subscription" => EnvelopeTarget::Subscription {
            session_id: SessionId(first.to_owned()),
            subscription_id: SubscriptionId(second.to_owned()),
        },
        "plugin" => EnvelopeTarget::Plugin {
            plugin_key: PluginKey(first.to_owned()),
        },
        "stream" => EnvelopeTarget::Stream {
            stream: first.to_owned(),
        },
        "topic" => EnvelopeTarget::Topic {
            topic: first.to_owned(),
        },
        _ => return Err(AdmissionError::InvalidTarget),
    };
    Ok(AcknowledgeInput {
        input: AcknowledgeOperation {
            request: AcknowledgeRoutedEnvelopeRequest {
                target,
                envelope_id: EnvelopeId(envelope_id.to_owned()),
            },
            charge: charges.input,
            lookup: charges.lookup,
            result: charges.result,
            error: core_error,
            conversion: core_conversion,
        },
        transport: ownership::AcknowledgeTransport {
            work: ownership::CoordinationStorage {
                core: crate::data_plane::driver::CoreSubmissionStorage {
                    request: charges.core_request,
                    reply: charges.core_reply,
                },
                continuation: charges.continuation,
                disposal: charges.disposal,
            },
            reply: charges.callback_reply,
            caller: charges.caller,
            error: charges.error_string,
            conversion: charges.conversion,
        },
    })
}

pub(super) fn callback(
    lua: &Lua,
    bridge: HubCoordinationBridge,
    memory: Arc<LuaMemoryAccount>,
) -> mlua::Result<Function> {
    let quota =
        lua.create_string("coordination.acknowledge exceeded the Lua callback memory limit")?;
    let capacity = lua.create_string("Lua callback memory capacity exhausted")?;
    let conversion =
        lua.create_string("coordination.acknowledge could not allocate its Lua result")?;
    let invalid_target =
        lua.create_string("coordination.acknowledge requires a recognized target.type")?;
    let invalid_utf8 = lua.create_string("coordination.acknowledge requires UTF-8 strings")?;
    let owner_thread = lua.create_string(
        "botster.coordination is only available during handler invocation, not at plugin load",
    )?;
    let queue_poisoned = lua.create_string("coordination queue lock poisoned")?;
    let ingress_sealed = lua.create_string("coordination ingress is sealed")?;
    let timeout = lua.create_string("coordination request did not complete before timeout")?;
    let unexpected =
        lua.create_string("coordination acknowledge returned unexpected response")?;
    // Only the trusted wrapper can call this function. Foreign values never enter Rust.
    let callback = lua.create_function(
        move |lua,
              (kind, first, second, envelope_id): (
            mlua::String,
            mlua::String,
            mlua::String,
            mlua::String,
        )| {
            let kind_bytes = kind.as_bytes();
            let first_bytes = first.as_bytes();
            let second_bytes = second.as_bytes();
            let envelope_bytes = envelope_id.as_bytes();
            let strings = (
                std::str::from_utf8(&kind_bytes),
                std::str::from_utf8(&first_bytes),
                std::str::from_utf8(&second_bytes),
                std::str::from_utf8(&envelope_bytes),
            );
            let (Ok(kind), Ok(first), Ok(second), Ok(envelope_id)) = strings else {
                return Ok(Value::String(invalid_utf8.clone()));
            };
            let input = match admit(&memory, kind, first, second, envelope_id) {
                Ok(input) => input,
                Err(AdmissionError::Quota) => return Ok(Value::String(quota.clone())),
                Err(AdmissionError::Capacity) => return Ok(Value::String(capacity.clone())),
                Err(AdmissionError::InvalidTarget) => {
                    return Ok(Value::String(invalid_target.clone()));
                }
            };
            // Bridge storage and result storage have separate owners and accounting work.
            let result = match bridge.acknowledge(input) {
                Ok(outcome) => lua.to_value(outcome.outcome()),
                Err(error) => match error {
                    super::CoordinationRequestError::Local(kind) => Ok(Value::String(match kind {
                        super::CoordinationLocalError::OwnerThread => owner_thread.clone(),
                        super::CoordinationLocalError::QueuePoisoned => queue_poisoned.clone(),
                        super::CoordinationLocalError::IngressSealed => ingress_sealed.clone(),
                        super::CoordinationLocalError::Timeout => timeout.clone(),
                        super::CoordinationLocalError::Unexpected => unexpected.clone(),
                    })),
                    other => lua.create_string(other.as_str()).map(Value::String),
                },
            };
            Ok(result.unwrap_or_else(|_| Value::String(conversion.clone())))
        },
    )?;
    wrap(lua, callback)
}

fn wrap(lua: &Lua, callback: Function) -> mlua::Result<Function> {
    let array_metatable = lua.array_metatable();
    // Lua cannot inspect the protected array metatable. This callback receives only tables.
    let is_array = lua.create_function(move |_, table: Table| {
        Ok(table.raw_len() > 0
            || table
                .metatable()
                .is_some_and(|mt| mt.to_pointer() == array_metatable.to_pointer()))
    })?;
    lua.load(r#"
        local callback, is_array = ...
        local type, rawget, error, utf8_len = type, rawget, error, utf8.len
        local fields = {
            endpoint = "endpoint_id", client = "client_id", session = "session_id",
            subscription = "session_id", plugin = "plugin_key", stream = "stream", topic = "topic"
        }
        local function string_field(value, path)
            if type(value) ~= "string" or utf8_len(value) == nil then
                error("coordination.acknowledge requires " .. path .. " as a UTF-8 string", 0)
            end
            return value
        end
        return function(args)
            if type(args) ~= "table" or is_array(args) or rawget(args, "target") == nil then
                error("coordination target is required", 0)
            end
            local target = rawget(args, "target")
            if type(target) ~= "table" or is_array(target) then
                error("coordination.acknowledge requires target as a table", 0)
            end
            local kind = string_field(rawget(target, "type"), "target.type")
            local field = rawget(fields, kind)
            if field == nil then
                error("coordination.acknowledge requires target.type as endpoint, client, session, subscription, plugin, stream, or topic", 0)
            end
            local first = rawget(target, field)
            local second = ""
            if kind == "subscription" then
                second = rawget(target, "subscription_id")
                -- Serde checks present fields before it reports missing fields.
                if first ~= nil then string_field(first, "target.session_id") end
                if second ~= nil then string_field(second, "target.subscription_id") end
            end
            first = string_field(first, "target." .. field)
            if kind == "subscription" then second = string_field(second, "target.subscription_id") end
            local envelope_id = string_field(rawget(args, "envelope_id"), "envelope_id")
            local result = callback(kind, first, second, envelope_id)
            if type(result) == "string" then error(result, 0) end
            return result
        end
    "#).set_name("@hub/acknowledge_input").call((callback, is_array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;
    use crate::lua_runtime::{
        HubCoordinationResponse, PendingCoordinationOperation, PendingCoordinationRequest,
    };
    use std::time::{Duration, Instant};

    fn account(per: usize, total: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: per,
            total_callback_bytes: total,
        })
        .unwrap()
    }

    fn sizing_total(first: &str, second: &str, envelope_id: &str) -> usize {
        ownership::AcknowledgeSizing::for_input(
            first
                .len()
                .checked_add(second.len())
                .and_then(|bytes| bytes.checked_add(envelope_id.len()))
                .unwrap(),
        )
        .and_then(|sizing| sizing.total())
        .unwrap()
    }

    fn ample_account() -> Arc<LuaMemoryAccount> {
        let total = sizing_total("subscription", "session", "envelope").saturating_mul(4);
        account(total, total.saturating_mul(8))
    }

    fn validation_lua() -> Lua {
        let lua = Lua::new();
        let memory = ample_account();
        let accept = lua
            .create_function(
                move |lua,
                 (kind, first, second, envelope_id): (
                    mlua::String,
                    mlua::String,
                    mlua::String,
                    mlua::String,
                )| {
                    let input = admit(
                        &memory,
                        &kind.to_str()?,
                        &first.to_str()?,
                        &second.to_str()?,
                        &envelope_id.to_str()?,
                    )
                    .unwrap();
                    lua.to_value(&input.input.request)
                },
            )
            .unwrap();
        lua.globals()
            .set("ack", wrap(&lua, accept).unwrap())
            .unwrap();
        lua.globals().set("null", lua.null()).unwrap();
        lua.globals()
            .set("array_mt", lua.array_metatable())
            .unwrap();
        lua
    }

    fn old(lua: &Lua, value: Value) -> Result<AcknowledgeRoutedEnvelopeRequest, String> {
        let value = lua
            .from_value::<serde_json::Value>(value)
            .map_err(|e| e.to_string())?;
        let target =
            super::super::target_from_json(value.get("target")).map_err(|e| e.to_string())?;
        let envelope_id = value
            .get("envelope_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "coordination.acknowledge requires envelope_id".to_owned())?;
        Ok(AcknowledgeRoutedEnvelopeRequest {
            target,
            envelope_id: EnvelopeId(envelope_id.to_owned()),
        })
    }

    #[test]
    fn all_variants_preserve_consumed_values() {
        let lua = validation_lua();
        let ack: Function = lua.globals().get("ack").unwrap();
        for (kind, field) in [
            ("endpoint", "endpoint_id"),
            ("client", "client_id"),
            ("session", "session_id"),
            ("subscription", "session_id"),
            ("plugin", "plugin_key"),
            ("stream", "stream"),
            ("topic", "topic"),
        ] {
            for value in ["", " ", "a\0b", "caf\u{e9}"] {
                let target = lua.create_table().unwrap();
                target.set("type", kind).unwrap();
                target.set(field, value).unwrap();
                if kind == "subscription" {
                    target.set("subscription_id", value).unwrap();
                }
                let args = lua.create_table().unwrap();
                args.set("target", target).unwrap();
                args.set("envelope_id", value).unwrap();
                let expected = old(&lua, Value::Table(args.clone())).unwrap();
                let actual = ack.call::<Value>(args).unwrap();
                assert_eq!(
                    lua.from_value::<AcknowledgeRoutedEnvelopeRequest>(actual)
                        .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn invalid_consumed_fields_preserve_classification_and_order() {
        let lua = validation_lua();
        let check: Function = lua.load("return function(args) local ok, value = pcall(ack, args); assert(not ok and type(value) == 'string'); return value end").eval().unwrap();
        for (source, old_fragment, new_fragment) in [
            (
                "nil",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "false",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "{1, target={type='topic',topic='t'},envelope_id='e'}",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "setmetatable({target={type='topic',topic='t'},envelope_id='e'},array_mt)",
                "coordination target is required",
                "coordination target is required",
            ),
            (
                "{target=null}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target={1,type='topic',topic='t'}}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target=setmetatable({type='topic',topic='t'},array_mt)}",
                "invalid coordination target",
                "target as a table",
            ),
            (
                "{target={}}",
                "missing field `type`",
                "target.type as a UTF-8 string",
            ),
            (
                "{target={type=1}}",
                "invalid coordination target",
                "target.type as a UTF-8 string",
            ),
            (
                "{target={type='unknown'}}",
                "unknown variant",
                "target.type as endpoint",
            ),
            (
                "{target={type='topic',topic=false}}",
                "invalid coordination target",
                "target.topic as a UTF-8 string",
            ),
            (
                "{target={type='subscription',subscription_id=false}}",
                "invalid coordination target",
                "target.subscription_id as a UTF-8 string",
            ),
            (
                "{target={type='subscription'}}",
                "missing field `session_id`",
                "target.session_id as a UTF-8 string",
            ),
            (
                "{target={type='subscription',session_id='s'}}",
                "missing field `subscription_id`",
                "target.subscription_id as a UTF-8 string",
            ),
            (
                "{target={type='topic',topic='t'},envelope_id=false}",
                "requires envelope_id",
                "envelope_id as a UTF-8 string",
            ),
            (
                "setmetatable({}, {__index={target={type='topic',topic='t'},envelope_id='e'}})",
                "coordination target is required",
                "coordination target is required",
            ),
        ] {
            let value: Value = lua.load(format!("return {source}")).eval().unwrap();
            let previous = old(&lua, value.clone()).unwrap_err();
            let current: String = check.call(value).unwrap();
            assert!(previous.contains(old_fragment), "{source}: {previous}");
            assert!(current.contains(new_fragment), "{source}: {current}");
            eprintln!("acknowledge diagnostic: {source}\nold: {previous}\nnew: {current}");
        }
        lua.load(
            r#"
            local invalid = {false, 1, {}, function() end, null, string.char(255)}
            for _, value in ipairs(invalid) do
                local ok, message = pcall(ack, {target={type='topic',topic=value}})
                assert(not ok and type(message)=='string' and message:find('target.topic', 1, true))
                ok, message = pcall(ack, {target={type='topic',topic='t'},envelope_id=value})
                assert(not ok and type(message)=='string' and message:find('envelope_id', 1, true))
            end
        "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn unused_values_are_ignored_without_metamethods() {
        let lua = validation_lua();
        let args: Value = lua
            .load(
                r#"
            local cycle = {}; cycle.self = cycle
            return {target={type='topic',topic='t',unused=function() end,[{}]=cycle},
                    envelope_id='e',unused=cycle}
        "#,
            )
            .eval()
            .unwrap();
        assert!(old(&lua, args.clone()).is_err());
        lua.globals()
            .get::<Function>("ack")
            .unwrap()
            .call::<Value>(args)
            .unwrap();
        lua.load(r#"
            local function fail() error('metamethod ran') end
            local target = setmetatable({type='topic',topic='t'}, {__index=fail,__pairs=fail,__len=fail,__eq=fail,__metatable=false})
            local args = setmetatable({target=target,envelope_id='e'}, {__index=fail,__pairs=fail,__len=fail})
            assert(ack(args).envelope_id=='e')
            local saved = ack
            type, rawget, error, utf8.len = fail, fail, fail, fail
            assert(saved(args).target.topic=='t')
        "#).exec().unwrap();
    }

    #[test]
    fn exact_input_charge_distinguishes_quota_and_capacity() {
        let admitted = sizing_total("ab", "cd", "ef");
        let memory = account(admitted, admitted);
        assert!(matches!(
            admit(&memory, "subscription", "ab", "cd", "efg"),
            Err(AdmissionError::Quota)
        ));
        assert_eq!(memory.usage().1, 0);
        let input = admit(&memory, "subscription", "ab", "cd", "ef").unwrap();
        assert_eq!(memory.usage().1, admitted);
        assert!(matches!(
            admit(&memory, "topic", "t", "", "e"),
            Err(AdmissionError::Capacity)
        ));
        assert_eq!(memory.usage().1, admitted);
        assert_eq!(input.input.request.envelope_id.0, "ef");
        drop(input);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn closure_disposal_and_owner_refusal_release_input() {
        let admitted = sizing_total("t", "", "e");
        let memory = account(admitted, admitted);
        let input = admit(&memory, "topic", "t", "", "e").unwrap();
        let operation = move || drop(input);
        assert_eq!(memory.usage().1, admitted);
        drop(operation);
        assert_eq!(memory.usage().1, 0);
        let bridge = HubCoordinationBridge::new();
        let input = admit(&memory, "topic", "t", "", "e").unwrap();
        assert!(
            bridge
                .acknowledge(input)
                .unwrap_err()
                .as_str()
                .contains("not at plugin load")
        );
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn wrapper_refusals_return_lua_strings_without_retaining_input() {
        let admitted = sizing_total("t", "", "e");
        let memory = account(admitted, admitted);
        let lua = Lua::new();
        lua.globals()
            .set(
                "ack",
                callback(
                    &lua,
                    HubCoordinationBridge::new(),
                    Arc::clone(&memory),
                )
                .unwrap(),
            )
            .unwrap();
        let held = memory.reserve_callback_total(admitted).unwrap();
        lua.load(
            r#"
            local function fails(args, fragment)
                local ok, message = pcall(ack, args)
                assert(not ok and type(message)=='string' and message:find(fragment, 1, true))
            end
            fails({target={type='topic',topic='t'},envelope_id='e'}, 'capacity exhausted')
            fails({target={type='topic',topic='tt'},envelope_id='e'}, 'memory limit')
            fails({target={type='topic',topic=function() end}}, 'target.topic')
        "#,
        )
        .exec()
        .unwrap();
        assert_eq!(memory.usage().1, admitted);
        drop(held);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn timeout_retains_input_until_terminal_disposal() {
        let admitted = sizing_total("t", "", "e");
        let memory = account(admitted, admitted);
        let bridge = HubCoordinationBridge::new();
        let input = admit(&memory, "topic", "t", "", "e").unwrap();
        let producer = bridge.clone();
        let result = std::thread::spawn(move || producer.acknowledge(input))
            .join()
            .unwrap();
        assert!(
            result
                .unwrap_err()
                .as_str()
                .contains("did not complete before timeout")
        );
        assert_eq!(bridge.test_pending_count(), 1);
        assert_eq!(memory.usage().1, admitted);
        assert!(bridge.dispose_terminal_pending());
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn real_wrapper_queues_charged_input_and_returns_delivery_state() {
        let admitted = sizing_total("t", "", "e");
        let memory = account(admitted, admitted);
        let bridge = HubCoordinationBridge::new();
        let producer = bridge.clone();
        let callback_memory = Arc::clone(&memory);
        let worker = std::thread::spawn(move || {
            let lua = Lua::new();
            lua.globals()
                .set(
                    "ack",
                    callback(&lua, producer, callback_memory).unwrap(),
                )
                .unwrap();
            lua.globals().set("null", lua.null()).unwrap();
            lua.load("local result=ack({target={type='topic',topic='t'},envelope_id='e'}); assert(result.state == null)").exec().unwrap();
        });
        // This poll schedules the test consumer. It does not prove a production wake.
        // The test must reply before the bridge's existing 1000 ms timeout.
        let deadline = Instant::now() + Duration::from_millis(500);
        let pending = loop {
            if let Some(pending) = bridge.take_pending() {
                break pending;
            }
            assert!(
                Instant::now() < deadline,
                "the wrapper did not queue its request"
            );
            std::thread::yield_now();
        };
        assert_eq!(memory.usage().1, admitted);
        let PendingCoordinationRequest {
            operation,
            response,
            storage,
            caller,
            ..
        } = pending;
        assert!(storage.is_some());
        let PendingCoordinationOperation::Acknowledge { input } = operation else {
            panic!("wrong operation")
        };
        assert_eq!(input.request.envelope_id.0, "e");
        let AcknowledgeOperation {
            request,
            charge,
            lookup,
            result,
            error,
            conversion,
        } = input;
        drop((request, charge, lookup, error));
        assert!(
            response
                .send(Ok(HubCoordinationResponse::Acknowledge(
                    ownership::AcknowledgeOutcome::new(
                        botster_core_daemon::RoutedEnvelopeDeliveryStateResult { state: None },
                        result,
                        conversion,
                    ),
                )))
                .is_ok()
        );
        worker.join().unwrap();
        drop((storage, caller));
        assert_eq!(memory.usage().1, 0);
    }
}
