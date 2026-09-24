//! A retained owner row for one ordinary session-type spawn.

use botster_hub_client::DaemonResponse;

use std::time::Instant;

use crate::client_api::HubClientSession;
use crate::client_api_dto::response::daemon_spawned;
use crate::client_api_dto::session::daemon_session_from_client;
use crate::daemon::control::pending::{
    ControlContinuation, ControlPoll, OwnerRequestCompletion, PendingControlRequest,
    READY_INITIAL, mark_owner_ready,
};
use crate::daemon::control::reply::ControlReply;
use crate::daemon::control::sessions::core_operator_error;
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::ReadyClass;
use crate::data_plane::driver::{ChargedCoreTicket, CoreTicketPoll, CoreWaiterRetirement};
use crate::owner_identity::WaiterId;
use crate::runtime::{
    AdmittedSpawnDelivery, PluginSpawnPoll, SessionSpawnCleanupPoll, SessionTypeSpawnStart,
    SpawnConversionOutcome, SpawnDeliveryOutcome, SpawnReplySender,
};
use crate::session_types::ChargedSessionTypeMaterialization;
use crate::host_executor::{HostCommand, HostJobIdentity};
use crate::HubDaemon;

/// Consume one front item and install its owner row before Host work starts.
pub(crate) fn accept_one(daemon: &mut HubDaemon, state: &mut DaemonControlState) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    let Some(pending) = runtime.take_pending_spawn_for_owner() else {
        return;
    };
    if matches!(&pending.response, crate::runtime::OrdinarySpawnReply::Legacy(_)) {
        runtime.accept_legacy_session_type_spawn(pending);
        return;
    }
    let crate::runtime::OrdinarySpawnReply::Admitted(response) = pending.response else {
        unreachable!("the admitted queue reader selected one admitted response");
    };
    let Some(mut parent) = pending.parent else {
        send_unavailable(response, "admitted spawn has no callback charge");
        return;
    };
    let Some(owner_permit) = state.budget.reserve() else {
        send_unavailable(response, "the Hub owner has no available operation slot");
        return;
    };
    let Some(waiter_id) = state.waiter_ids.next() else {
        state.budget.release(owner_permit);
        send_unavailable(response, "the Hub owner exhausted operation identifiers");
        return;
    };
    let Some(host_permit) = runtime.host_executor().try_reserve() else {
        state.budget.release(owner_permit);
        send_unavailable(response, "the Host executor has no available operation slot");
        return;
    };
    let retirement = runtime.coordination_retirement(waiter_id);
    let Some(reply_bytes) = crate::data_plane::driver::retained_reply_bytes::<()>() else {
        state.budget.release(owner_permit);
        send_unavailable(response, "Host reply size overflow");
        return;
    };
    if parent.grow(reply_bytes).is_err() {
        state.budget.release(owner_permit);
        send_unavailable(response, "Host reply capacity exhausted");
        return;
    }
    let reply_charge = parent
        .split_fixed(reply_bytes)
        .expect("the parent admitted the Host reply");
    let Ok((ticket, receipt)) = runtime.session_spawn_host_reply(&retirement, reply_charge) else {
        state.budget.release(owner_permit);
        send_unavailable(response, "Host reply registration refused");
        return;
    };
    let operation_bytes = std::mem::size_of::<SessionTypeSpawnOperation>();
    if parent.grow(operation_bytes).is_err() {
        state.budget.release(owner_permit);
        send_unavailable(response, "owner operation capacity exhausted");
        return;
    }
    let operation_storage = parent
        .split_fixed(operation_bytes)
        .expect("the parent admitted the boxed owner operation");
    let work = match crate::session_types::SpawnHostWork::new_ordinary(
        parent,
        receipt,
        runtime.config(),
        runtime.state(),
        pending.package_records,
        pending.plugin_key,
        pending.session_type_id,
        pending.request,
    ) {
        Ok(work) => work,
        Err((reason, _parent, _receipt)) => {
            state.budget.release(owner_permit);
            send_unavailable(response, reason);
            return;
        }
    };
    let operation = ChargedSessionTypeOperation::new(
        SessionTypeSpawnOperation::waiting_for_host(
            waiter_id,
            String::new(),
            retirement,
            response,
            ticket,
        ),
        operation_storage,
    );
    state.pending_requests.insert(
        waiter_id,
        PendingControlRequest {
            waiter_id,
            ready_class: ReadyClass::HostCompletion,
            ready_key: None,
            deadline_key: None,
            last_core_phase: 0,
            last_host_phase: 0,
            completion: OwnerRequestCompletion::default(),
            reply_tx: crate::daemon::control::message::ControlReplySender::absent(),
            response_delivery_rx: None,
            grant_id: None,
            client: None,
            core_retirement: None,
            permit: Some(owner_permit),
            must_finish: true,
            past_deadline: false,
            continuation: ControlContinuation::SessionType(operation),
            retire: None,
        },
    );
    let now = Instant::now();
    let deadline = now + crate::daemon::owner_budget::RETAINED_OPERATION_DEADLINE;
    let arm = state
        .deadlines
        .arm(waiter_id, deadline, now)
        .expect("the admitted spawn deadline must make progress");
    state
        .pending_requests
        .get_mut(&waiter_id)
        .expect("the admitted spawn row was inserted")
        .deadline_key = Some(arm.key());
    if runtime
        .host_executor()
        .submit(
            HostJobIdentity { waiter_id, phase: 1 },
            HostCommand::MaterializeOrdinarySessionType(work),
            host_permit,
        )
        .is_err()
        && let Some(entry) = state.pending_requests.get_mut(&waiter_id)
        && let ControlContinuation::SessionType(operation) = &mut entry.continuation
    {
        operation.send_unavailable("the Host executor refused materialization");
        operation.phase = Phase::Done;
    }
    mark_owner_ready(state, waiter_id, ReadyClass::HostCompletion, READY_INITIAL);
}

fn send_unavailable(
    response: SpawnReplySender<AdmittedSpawnDelivery>,
    reason: &'static str,
) {
    if let Err(refusal) = response.try_send(AdmittedSpawnDelivery::Unavailable(reason)) {
        drop(refusal);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Host,
    Ready,
    Core,
    Delivery,
    Conversion,
    Cleanup,
    Fault,
    Done,
}

enum AdmittedFailure {
    Refused(String, crate::lua_memory::LuaCallbackCharge),
    Unavailable(&'static str),
}

impl AdmittedFailure {
    fn into_delivery(self) -> AdmittedSpawnDelivery {
        match self {
            Self::Refused(message, variable) => AdmittedSpawnDelivery::refused(message, variable),
            Self::Unavailable(reason) => AdmittedSpawnDelivery::Unavailable(reason),
        }
    }
}

/// A confirmed Core cleanup can answer the worker without a daemon reply channel.
fn deliver_confirmed_admitted_failure(
    response: &mut Option<SpawnReplySender<AdmittedSpawnDelivery>>,
    failure: &mut Option<AdmittedFailure>,
) -> bool {
    let Some(response) = response.take() else {
        return false;
    };
    let delivery = failure
        .take()
        .unwrap_or(AdmittedFailure::Unavailable("session type spawn failed"))
        .into_delivery();
    if let Err(refusal) = response.try_send(delivery) {
        drop(refusal);
    }
    true
}

/// The pending request row owns this value from admission through retirement.
/// Admission must insert the row before its first poll starts Core work.
pub(crate) struct SessionTypeSpawnOperation {
    waiter_id: WaiterId,
    request_id: String,
    product: Option<ChargedSessionTypeMaterialization>,
    start: Option<SessionTypeSpawnStart>,
    delivery: Option<ChargedCoreTicket<SpawnDeliveryOutcome>>,
    conversion: Option<ChargedCoreTicket<SpawnConversionOutcome>>,
    host_receipt: Option<ChargedCoreTicket<()>>,
    plugin_response: Option<SpawnReplySender<AdmittedSpawnDelivery>>,
    admitted_failure: Option<AdmittedFailure>,
    failure: Option<DaemonResponse>,
    phase: Phase,
    // Drop the retirement after every ticket and Core stage.
    retirement: CoreWaiterRetirement,
}

/// Keep the Box allowance outside the Box until its deallocation finishes.
pub(crate) struct ChargedSessionTypeOperation {
    operation: Box<SessionTypeSpawnOperation>,
    _storage: crate::lua_memory::LuaCallbackCharge,
}

impl ChargedSessionTypeOperation {
    fn new(
        operation: SessionTypeSpawnOperation,
        storage: crate::lua_memory::LuaCallbackCharge,
    ) -> Self {
        Self {
            operation: Box::new(operation),
            _storage: storage,
        }
    }
}

impl std::ops::Deref for ChargedSessionTypeOperation {
    type Target = SessionTypeSpawnOperation;

    fn deref(&self) -> &Self::Target {
        &self.operation
    }
}

impl std::ops::DerefMut for ChargedSessionTypeOperation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.operation
    }
}

#[allow(dead_code)] // Production admission waits for the charged parser proof.
impl SessionTypeSpawnOperation {
    pub(crate) fn waits_for_host(&self) -> bool {
        self.phase == Phase::Host
    }

    /// Host destroys the completed payload before the Owner releases this row.
    pub(crate) fn take_terminal_parts(
        &mut self,
        completion: &mut Option<crate::host_executor::HostCompletion>,
    ) -> Option<crate::host_disposal::Parts> {
        if self.phase != Phase::Host {
            return None;
        }
        let (identity, result, permit) = completion.take()?.into_parts();
        Some(crate::host_disposal::Parts {
            identity,
            permit,
            payload: Box::new(result),
            model: None,
            storage: None,
        })
    }

    pub(crate) fn new(
        waiter_id: WaiterId,
        request_id: String,
        retirement: CoreWaiterRetirement,
        product: ChargedSessionTypeMaterialization,
    ) -> Self {
        Self {
            waiter_id,
            request_id,
            product: Some(product),
            start: None,
            delivery: None,
            conversion: None,
            host_receipt: None,
            plugin_response: None,
            admitted_failure: None,
            failure: None,
            phase: Phase::Ready,
            retirement,
        }
    }

    pub(crate) fn new_plugin(
        waiter_id: WaiterId,
        request_id: String,
        retirement: CoreWaiterRetirement,
        product: ChargedSessionTypeMaterialization,
        response: SpawnReplySender<AdmittedSpawnDelivery>,
    ) -> Self {
        let mut operation = Self::new(waiter_id, request_id, retirement, product);
        operation.plugin_response = Some(response);
        operation
    }

    pub(crate) fn waiting_for_host(
        waiter_id: WaiterId,
        request_id: String,
        retirement: CoreWaiterRetirement,
        response: SpawnReplySender<AdmittedSpawnDelivery>,
        host_receipt: ChargedCoreTicket<()>,
    ) -> Self {
        Self {
            waiter_id,
            request_id,
            product: None,
            start: None,
            delivery: None,
            conversion: None,
            host_receipt: Some(host_receipt),
            plugin_response: Some(response),
            admitted_failure: None,
            failure: None,
            phase: Phase::Host,
            retirement,
        }
    }

    fn send_unavailable(&mut self, reason: &'static str) {
        if let Some(response) = self.plugin_response.take()
            && let Err(refusal) =
                response.try_send(AdmittedSpawnDelivery::Unavailable(reason))
        {
            drop(refusal);
        }
    }

    pub(crate) fn poll(
        &mut self,
        daemon: &mut HubDaemon,
        _state: &mut DaemonControlState,
    ) -> ControlPoll {
        let Some(runtime) = daemon.runtime() else {
            self.phase = Phase::Fault;
            return ControlPoll::Pending;
        };
        loop {
            match self.phase {
                Phase::Host => {
                    let Some(completion) = _state.host_completions.remove(&self.waiter_id) else {
                        return ControlPoll::Pending;
                    };
                    let (_, result, permit) = completion.into_parts();
                    drop(permit);
                    match result {
                        crate::host_executor::HostResult::OrdinarySessionTypeMaterialized(
                            completed,
                        ) => {
                            completed.receipt.publish(());
                            self.host_receipt = None;
                            match completed.result {
                                Ok(product) => {
                                    self.product = Some(product);
                                    self.phase = Phase::Ready;
                                    return ControlPoll::Again;
                                }
                                Err(crate::session_types::ChargedMaterializationFailure::Semantic(
                                    failure,
                                )) => {
                                    if let Some(response) = self.plugin_response.take() {
                                        let (error, variable) = failure.into_parts();
                                        let delivery = AdmittedSpawnDelivery::refused(
                                            error.message,
                                            variable,
                                        );
                                        if let Err(refusal) = response.try_send(delivery) {
                                            drop(refusal);
                                        }
                                    }
                                }
                                Err(crate::session_types::ChargedMaterializationFailure::Capacity(
                                    reason,
                                )
                                | crate::session_types::ChargedMaterializationFailure::Unavailable(
                                    reason,
                                )) => {
                                    self.send_unavailable(reason);
                                }
                            }
                        }
                        _ => self.send_unavailable("Host materialization did not complete"),
                    }
                    self.phase = Phase::Done;
                    return ControlPoll::FinishedInternal;
                }
                Phase::Ready => {
                    let product = self.product.take().expect("charged spawn product exists");
                    self.start =
                        Some(runtime.begin_session_type_spawn_for_owner(self.waiter_id, product));
                    self.phase = Phase::Core;
                }
                Phase::Core => {
                    let start = self.start.as_mut().expect("Core spawn stage exists");
                    match start.poll(runtime) {
                        PluginSpawnPoll::Pending => return ControlPoll::Pending,
                        PluginSpawnPoll::Ready(Err(failure)) => {
                            if self.plugin_response.is_some() {
                                self.admitted_failure = Some(
                                    start
                                        .charged_plugin_failure(&failure)
                                        .map(|(message, charge)| {
                                            AdmittedFailure::Refused(message, charge)
                                        })
                                        .unwrap_or(AdmittedFailure::Unavailable(
                                            crate::lua_runtime::LUA_CALLBACK_CAPACITY_EXHAUSTED,
                                        )),
                                );
                            } else {
                                self.failure = Some(core_operator_error(
                                    "spawn_session_type",
                                    &self.request_id,
                                    &failure.error,
                                ));
                            }
                            self.phase = Phase::Cleanup;
                            return ControlPoll::Again;
                        }
                        PluginSpawnPoll::Ready(Ok(session)) => {
                            if self.plugin_response.is_some() {
                                let Some((result_charge, ticket_charge)) =
                                    start.charged_plugin_response(&session)
                                else {
                                    self.admitted_failure = Some(AdmittedFailure::Unavailable(
                                        crate::lua_runtime::LUA_CALLBACK_CAPACITY_EXHAUSTED,
                                    ));
                                    self.phase = Phase::Cleanup;
                                    return ControlPoll::Again;
                                };
                                let Ok((ticket, conversion)) = runtime
                                    .session_spawn_conversion_reply(&self.retirement, ticket_charge)
                                else {
                                    self.admitted_failure = Some(AdmittedFailure::Unavailable(
                                        "conversion receipt registration refused",
                                    ));
                                    self.phase = Phase::Cleanup;
                                    return ControlPoll::Again;
                                };
                                let response = self
                                    .plugin_response
                                    .take()
                                    .expect("the admitted sender remains until delivery");
                                let spawned = runtime
                                    .finish_session_type_spawn(start, Ok(session))
                                    .expect("the completed Core spawn has a result");
                                let delivery = AdmittedSpawnDelivery::spawned(
                                    spawned,
                                    conversion,
                                    result_charge,
                                );
                                self.conversion = Some(ticket);
                                self.phase = Phase::Conversion;
                                if let Err(refusal) = response.try_send(delivery) {
                                    drop(refusal);
                                    self.phase = Phase::Cleanup;
                                    return ControlPoll::Again;
                                }
                                return ControlPoll::Pending;
                            }
                            let Some(charge) = runtime.daemon_session_spawn_delivery_charge()
                            else {
                                self.phase = Phase::Cleanup;
                                return ControlPoll::Again;
                            };
                            let Ok((ticket, receipt)) =
                                runtime.session_spawn_delivery_reply(&self.retirement, charge)
                            else {
                                // A registration fault must not become success or refusal.
                                self.phase = Phase::Cleanup;
                                return ControlPoll::Again;
                            };
                            let response = daemon_spawned(
                                daemon_session_from_client(HubClientSession::from(session)),
                                Vec::new(),
                            );
                            self.delivery = Some(ticket);
                            self.phase = Phase::Delivery;
                            return ControlPoll::DeliverSessionType(ControlReply::spawn_delivery(
                                Ok(response),
                                receipt,
                            ));
                        }
                    }
                }
                Phase::Delivery => {
                    let ticket = self.delivery.as_mut().expect("delivery ticket exists");
                    match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Ready(SpawnDeliveryOutcome::Delivered) => {
                            self.delivery = None;
                            self.phase = Phase::Done;
                            return ControlPoll::FinishedInternal;
                        }
                        CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                            // Lost is visible only after the owner collects its identity.
                            // Refused never registered one. No prior identity blocks cleanup.
                            self.delivery = None;
                            self.phase = Phase::Cleanup;
                            return ControlPoll::Again;
                        }
                    }
                }
                Phase::Conversion => {
                    let ticket = self.conversion.as_mut().expect("conversion ticket exists");
                    match ticket.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Ready(SpawnConversionOutcome::Converted) => {
                            self.conversion = None;
                            self.phase = Phase::Done;
                            return ControlPoll::FinishedInternal;
                        }
                        CoreTicketPoll::Ready(SpawnConversionOutcome::Abandoned)
                        | CoreTicketPoll::Lost
                        | CoreTicketPoll::Refused => {
                            self.conversion = None;
                            self.phase = Phase::Cleanup;
                            return ControlPoll::Again;
                        }
                    }
                }
                Phase::Cleanup => {
                    let start = self.start.as_mut().expect("cleanup owns the Core stage");
                    match start.abandon_and_poll_cleanup(runtime) {
                        SessionSpawnCleanupPoll::Pending | SessionSpawnCleanupPoll::Unresolved => {
                            return ControlPoll::Pending
                        }
                        SessionSpawnCleanupPoll::Confirmed => {
                            self.phase = Phase::Done;
                            if deliver_confirmed_admitted_failure(
                                &mut self.plugin_response,
                                &mut self.admitted_failure,
                            ) {
                                return ControlPoll::FinishedInternal;
                            }
                            return match self.failure.take() {
                                Some(response) => ControlPoll::Ready(Ok(response)),
                                None => ControlPoll::FinishedInternal,
                            };
                        }
                    }
                }
                Phase::Fault => return ControlPoll::Pending,
                Phase::Done => return ControlPoll::FinishedInternal,
            }
        }
    }

    /// Terminal drain retains this row while Core still owns an effect.
    pub(crate) fn poll_terminal(&mut self, runtime: &crate::HubRuntime) -> bool {
        runtime.take_terminal_coordination_completion(self.waiter_id);
        if let Some(ticket) = self.delivery.as_mut() {
            match ticket.poll() {
                CoreTicketPoll::Pending => return false,
                CoreTicketPoll::Ready(SpawnDeliveryOutcome::Delivered) => {
                    self.delivery = None;
                    self.phase = Phase::Done;
                    return true;
                }
                CoreTicketPoll::Lost | CoreTicketPoll::Refused => {
                    self.delivery = None;
                    self.phase = Phase::Cleanup;
                }
            }
        }
        if let Some(ticket) = self.conversion.as_mut() {
            match ticket.poll() {
                CoreTicketPoll::Pending => return false,
                CoreTicketPoll::Ready(SpawnConversionOutcome::Converted) => {
                    self.conversion = None;
                    self.phase = Phase::Done;
                    return true;
                }
                CoreTicketPoll::Ready(SpawnConversionOutcome::Abandoned)
                | CoreTicketPoll::Lost
                | CoreTicketPoll::Refused => {
                    self.conversion = None;
                    self.phase = Phase::Cleanup;
                }
            }
        }
        if self.phase == Phase::Done {
            return true;
        }
        let Some(start) = self.start.as_mut() else {
            // A product without a started Core effect can be destroyed here.
            self.product = None;
            self.phase = Phase::Done;
            return true;
        };
        match start.abandon_and_poll_cleanup(runtime) {
            SessionSpawnCleanupPoll::Confirmed => {
                self.phase = Phase::Done;
                true
            }
            SessionSpawnCleanupPoll::Pending | SessionSpawnCleanupPoll::Unresolved => false,
        }
    }
}

#[cfg(test)]
mod admitted_failure_tests {
    use super::*;
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits, layout};
    use std::sync::Arc;
    use std::time::Duration;

    fn memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap()
    }

    #[test]
    fn confirmed_core_failure_delivers_funded_error() {
        let memory = memory();
        let bytes = layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
        let (sender, receiver) = crate::runtime::spawn_reply_channel(
            memory.reserve_callback_total(bytes).unwrap(),
        )
        .unwrap();
        let message = "session type spawn failed: occupied".to_string();
        let charge = memory.reserve_callback_total(message.len()).unwrap();
        let mut response = Some(sender);
        let mut failure = Some(AdmittedFailure::Refused(message.clone(), charge));
        assert!(deliver_confirmed_admitted_failure(&mut response, &mut failure));
        assert!(response.is_none());
        assert!(failure.is_none());
        let delivery = receiver.recv_timeout(Duration::ZERO).unwrap();
        let AdmittedSpawnDelivery::Refused { message: actual, _variable } = delivery else {
            panic!("Core failure must deliver its charged text");
        };
        assert_eq!(actual, message);
        drop(actual);
        drop(_variable);
        drop(receiver);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn confirmed_post_success_refusals_deliver_typed_failure() {
        for reason in [
            crate::lua_runtime::LUA_CALLBACK_CAPACITY_EXHAUSTED,
            "conversion receipt registration refused",
        ] {
            let memory = memory();
            let bytes = layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
            let (sender, receiver) = crate::runtime::spawn_reply_channel(
                memory.reserve_callback_total(bytes).unwrap(),
            )
            .unwrap();
            let mut response = Some(sender);
            let mut failure = Some(AdmittedFailure::Unavailable(reason));
            assert!(deliver_confirmed_admitted_failure(&mut response, &mut failure));
            let delivery = receiver.recv_timeout(Duration::ZERO).unwrap();
            assert!(matches!(delivery, AdmittedSpawnDelivery::Unavailable(actual) if actual == reason));
            drop(receiver);
            assert_eq!(memory.usage().1, 0);
        }
    }
}
