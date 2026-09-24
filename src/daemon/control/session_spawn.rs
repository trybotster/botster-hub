//! A retained owner row for one ordinary session-type spawn.

use botster_core::SessionReservationRelease;
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
        runtime.startup_materialization_paths(),
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
    HostReceipt,
    Ready,
    Core,
    Delivery,
    Conversion,
    Cleanup,
    Fault,
    Done,
}

enum AdmittedFailure {
    Refused(String, crate::lua_memory::LuaCallbackCharge, crate::lua_memory::LuaCallbackCharge),
    Unavailable(&'static str),
}

impl AdmittedFailure {
    fn into_delivery(self) -> AdmittedSpawnDelivery {
        match self {
            Self::Refused(message, variable, lua_render) => {
                AdmittedSpawnDelivery::refused(message, variable, lua_render)
            }
            Self::Unavailable(reason) => AdmittedSpawnDelivery::Unavailable(reason),
        }
    }
}

/// The caller has confirmed cleanup or has a typed RetainedUnconfirmed failure
/// while the owner row keeps its exact Core stage.
fn deliver_admitted_failure(
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
    #[cfg(test)]
    pub(crate) fn test_reservation_identity(
        &self,
    ) -> Option<botster_core::SessionReservationIdentity> {
        self.start
            .as_ref()
            .and_then(SessionTypeSpawnStart::test_reservation_identity)
    }

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
                            match completed.result {
                                Ok(product) => {
                                    self.product = Some(product);
                                    self.phase = Phase::HostReceipt;
                                    return ControlPoll::Pending;
                                }
                                Err(crate::session_types::ChargedMaterializationFailure::Semantic(
                                    failure,
                                )) => {
                                    if let Some(response) = self.plugin_response.take() {
                                        let (error, variable, lua_render) = failure.into_parts();
                                        let delivery = AdmittedSpawnDelivery::refused(
                                            error.message,
                                            variable,
                                            lua_render,
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
                Phase::HostReceipt => {
                    let receipt = self.host_receipt.as_mut().expect("Host receipt exists");
                    match receipt.poll() {
                        CoreTicketPoll::Pending => return ControlPoll::Pending,
                        CoreTicketPoll::Ready(()) => {
                            self.host_receipt = None;
                            self.phase = Phase::Ready;
                            return ControlPoll::Again;
                        }
                        CoreTicketPoll::Refused | CoreTicketPoll::Lost => {
                            self.product = None;
                            self.host_receipt = None;
                            self.send_unavailable("Host completion receipt unavailable");
                            self.phase = Phase::Done;
                            return ControlPoll::FinishedInternal;
                        }
                    }
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
                            let retained_unconfirmed = failure.disposition
                                == Some(SessionReservationRelease::RetainedUnconfirmed);
                            if self.plugin_response.is_some() {
                                self.admitted_failure = Some(
                                    start
                                        .charged_plugin_failure(&failure)
                                        .map(|(message, charge, lua_render)| {
                                            AdmittedFailure::Refused(message, charge, lua_render)
                                        })
                                        .unwrap_or_else(|| {
                                            AdmittedFailure::Unavailable(
                                                crate::lua_runtime::LUA_CALLBACK_CAPACITY_EXHAUSTED,
                                            )
                                        }),
                                );
                            } else {
                                self.failure = Some(core_operator_error(
                                    "spawn_session_type",
                                    &self.request_id,
                                    &failure.error,
                                ));
                            }
                            if retained_unconfirmed && self.plugin_response.is_some() {
                                deliver_admitted_failure(
                                    &mut self.plugin_response,
                                    &mut self.admitted_failure,
                                );
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
                                    return ControlPoll::Pending;
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
                            if deliver_admitted_failure(
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
        let render_bytes = crate::session_types::lua_spawn_refusal_render_bytes(&message).unwrap();
        let mut parent = memory
            .reserve_callback_total(message.len() + render_bytes)
            .unwrap();
        let charge = parent.split_fixed(message.len()).unwrap();
        let lua_render = parent.split_fixed(render_bytes).unwrap();
        drop(parent);
        let mut response = Some(sender);
        let mut failure = Some(AdmittedFailure::Refused(message, charge, lua_render));
        assert!(deliver_admitted_failure(&mut response, &mut failure));
        assert!(response.is_none());
        assert!(failure.is_none());
        let delivery = receiver.recv_timeout(Duration::ZERO).unwrap();
        let AdmittedSpawnDelivery::Refused { message: actual, _variable, _lua_render } = delivery else {
            panic!("Core failure must deliver its charged text");
        };
        assert_eq!(actual, "session type spawn failed: occupied");
        assert_eq!(_lua_render.bytes(), crate::session_types::lua_spawn_refusal_render_bytes(&actual).unwrap());
        drop(actual);
        drop(_variable);
        drop(_lua_render);
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
            assert!(deliver_admitted_failure(&mut response, &mut failure));
            let delivery = receiver.recv_timeout(Duration::ZERO).unwrap();
            assert!(matches!(delivery, AdmittedSpawnDelivery::Unavailable(actual) if actual == reason));
            drop(receiver);
            assert_eq!(memory.usage().1, 0);
        }
    }

    #[test]
    fn dropped_receiver_disposes_one_charged_refusal_without_dropping_owner_charge() {
        let memory = memory();
        let owner_charge = memory.reserve_callback_total(73).unwrap();
        let reply_bytes = layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
        let (sender, receiver) = crate::runtime::spawn_reply_channel(
            memory.reserve_callback_total(reply_bytes).unwrap(),
        )
        .unwrap();
        let message = "cleanup_unconfirmed: worker exited".to_string();
        let render_bytes = crate::session_types::lua_spawn_refusal_render_bytes(&message).unwrap();
        let mut parent = memory
            .reserve_callback_total(message.len() + render_bytes)
            .unwrap();
        let variable = parent.split_fixed(message.len()).unwrap();
        let lua_render = parent.split_fixed(render_bytes).unwrap();
        drop(parent);
        drop(receiver);
        let mut response = Some(sender);
        let mut failure = Some(AdmittedFailure::Refused(message, variable, lua_render));
        assert!(deliver_admitted_failure(&mut response, &mut failure));
        assert!(!deliver_admitted_failure(&mut response, &mut failure));
        assert_eq!(memory.usage().1, owner_charge.bytes());
        drop(owner_charge);
        assert_eq!(memory.usage().1, 0);
    }
}

#[cfg(test)]
mod owner_conversion_lifecycle_tests {
    use super::*;
    use std::ops::{Deref, DerefMut};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use botster_core::{SessionId, SessionReservationRelease};
    use botster_core_daemon::CoreCompletion;

    use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
    use crate::data_plane::driver::{CoreTicketPoll, retained_reply_bytes};
    use crate::lua_memory::{LuaMemoryAccount, layout};
    use crate::owner_identity::{OwnerWorkIdentity, WaiterId};
    use crate::runtime::{SpawnConversionOutcome, spawn_reply_channel};

    #[derive(Clone, Copy)]
    enum Case {
        ExplicitAbandonment,
        RegistrationRefusal,
        DroppedReceiver,
    }

    struct TestDaemon {
        daemon: HubDaemon,
        root: PathBuf,
        session_id: SessionId,
    }

    impl Deref for TestDaemon {
        type Target = HubDaemon;

        fn deref(&self) -> &Self::Target {
            &self.daemon
        }
    }

    impl DerefMut for TestDaemon {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.daemon
        }
    }

    impl Drop for TestDaemon {
        fn drop(&mut self) {
            if let Some(runtime) = self.daemon.runtime() {
                let _ = runtime.shutdown_session_for_test(self.session_id.clone());
            }
            self.daemon.stop();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn collect(
        runtime: &crate::HubRuntime,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        waiter_id: WaiterId,
        phases: &[u64],
    ) {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let mut found = executor
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let mut found = Vec::new();
                    while found.len() < phases.len() {
                        let identities = runtime.take_owner_core_completions(64);
                        for identity in identities {
                            if identity.waiter_id == waiter_id {
                                found.push(identity);
                            }
                        }
                        if found.len() < phases.len() {
                            receiver.recv().await.expect("Core must wake its owner");
                        }
                    }
                    found
                })
                .await
                .expect("Core must complete the expected phase")
            });
        found.sort();
        assert_eq!(
            found,
            phases
                .iter()
                .map(|phase| OwnerWorkIdentity {
                    waiter_id,
                    phase: *phase,
                })
                .collect::<Vec<_>>(),
        );
    }

    fn finish_cleanup(
        operation: &mut SessionTypeSpawnOperation,
        daemon: &mut HubDaemon,
        state: &mut DaemonControlState,
        wake: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        waiter_id: WaiterId,
    ) {
        let mut phase = 6;
        for _ in 0..12 {
            match operation.poll(daemon, state) {
                ControlPoll::Again => {}
                ControlPoll::Pending => {
                    collect(
                        daemon.runtime().expect("runtime remains live"),
                        wake,
                        waiter_id,
                        &[phase, phase + 1],
                    );
                    phase += 2;
                }
                ControlPoll::FinishedInternal => return,
                _ => panic!("owner cleanup must not report success"),
            }
        }
        panic!("owner cleanup did not confirm exact Core release");
    }

    #[test]
    fn occupied_reservation_waits_for_confirmed_cleanup_before_refusal() {
        let name = format!("ordinary-owner-occupied-{}", std::process::id());
        let root = std::env::temp_dir().join(&name);
        let session_id = SessionId(name);
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let mut daemon = TestDaemon {
            daemon: HubDaemon::start(config).unwrap(),
            root,
            session_id: session_id.clone(),
        };
        let runtime = daemon.runtime().unwrap();
        let memory = runtime.test_lua_memory();
        let (wake_sender, mut wake) = tokio::sync::mpsc::channel(64);
        runtime.bind_data_plane_owner_wake(wake_sender);
        let first_waiter = runtime.next_waiter_id().unwrap();
        let mut first = runtime.begin_reserve_session_for_owner(first_waiter, session_id.clone());
        collect(runtime, &mut wake, first_waiter, &[1, 2]);
        let CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
            result: Ok(reservation),
            ..
        })) = first.poll(runtime)
        else {
            panic!("the first reservation must occupy the ID");
        };

        let waiter_id = runtime.next_waiter_id().unwrap();
        let retirement = runtime.coordination_retirement(waiter_id);
        let bytes = layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
        let (sender, receiver) = spawn_reply_channel(
            memory.reserve_callback_total(bytes).unwrap(),
        )
        .unwrap();
        let parent = memory.reserve_callback_total(0).unwrap();
        let start = runtime.test_begin_ordinary_owner_spawn(waiter_id, session_id, parent);
        let mut operation = SessionTypeSpawnOperation {
            waiter_id,
            request_id: String::new(),
            product: None,
            start: Some(start),
            delivery: None,
            conversion: None,
            host_receipt: None,
            plugin_response: Some(sender),
            admitted_failure: None,
            failure: None,
            phase: Phase::Core,
            retirement,
        };
        let mut state = DaemonControlState::default();
        collect(runtime, &mut wake, waiter_id, &[1, 2]);
        assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Again));
        assert!(matches!(
            receiver.recv_timeout(Duration::ZERO),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::FinishedInternal));
        let AdmittedSpawnDelivery::Refused { message, _variable, _lua_render } =
            receiver.recv_timeout(Duration::ZERO).unwrap()
        else {
            panic!("confirmed cleanup must deliver the occupied refusal");
        };
        assert!(message.to_ascii_lowercase().contains("occupied"));
        drop(message);
        drop(_variable);
        drop(_lua_render);
        drop(receiver);

        let mut release = daemon
            .runtime()
            .unwrap()
            .begin_release_session_reservation_for_owner(first_waiter, reservation);
        collect(daemon.runtime().unwrap(), &mut wake, first_waiter, &[3, 4]);
        assert!(matches!(
            release.poll(daemon.runtime().unwrap()),
            CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                result: Ok(SessionReservationRelease::Released),
                ..
            }))
        ));
    }

    fn run(case: Case) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let number = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!("ordinary-owner-conversion-{}-{number}", std::process::id());
        let root = std::env::temp_dir().join(&name);
        let session_id = SessionId(name);
        let config = HubStartupOptions {
            data_directory: DataDirectoryOption::Explicit(root.clone()),
            ..HubStartupOptions::default()
        }
        .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
        .unwrap();
        let mut daemon = TestDaemon {
            daemon: HubDaemon::start(config).expect("start isolated owner runtime"),
            root,
            session_id: session_id.clone(),
        };
        let runtime = daemon.runtime().unwrap();
        let memory: Arc<LuaMemoryAccount> = runtime.test_lua_memory();
        let waiter_id = runtime.next_waiter_id().unwrap();
        let context_id = format!("ctx-{}", session_id.0);
        let (wake_sender, mut wake) = tokio::sync::mpsc::channel(64);
        runtime.bind_data_plane_owner_wake(wake_sender);
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let reply_bytes = layout::single_reply_bytes::<AdmittedSpawnDelivery>(true).unwrap();
        parent.grow(reply_bytes).unwrap();
        let reply_charge = parent.split_fixed(reply_bytes).unwrap();
        let (sender, receiver) = spawn_reply_channel(reply_charge).unwrap();
        let retirement = runtime.coordination_retirement(waiter_id);
        let start = runtime.test_begin_ordinary_owner_spawn(waiter_id, session_id.clone(), parent);
        let mut operation = SessionTypeSpawnOperation {
            waiter_id,
            request_id: String::new(),
            product: None,
            start: Some(start),
            delivery: None,
            conversion: None,
            host_receipt: None,
            plugin_response: Some(sender),
            admitted_failure: None,
            failure: None,
            phase: Phase::Core,
            retirement,
        };
        let mut state = DaemonControlState::default();
        collect(daemon.runtime().unwrap(), &mut wake, waiter_id, &[1, 2]);
        assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Pending));
        collect(daemon.runtime().unwrap(), &mut wake, waiter_id, &[3, 4]);
        assert_eq!(
            daemon.runtime().unwrap().session_context(&context_id).unwrap().session_id,
            session_id,
        );
        let original = operation
            .start
            .as_ref()
            .unwrap()
            .test_reservation_identity()
            .expect("the Core reservation exists");

        match case {
            Case::ExplicitAbandonment => {
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Pending));
                let delivery = receiver.recv_timeout(Duration::ZERO).unwrap();
                let AdmittedSpawnDelivery::Spawned {
                    result,
                    conversion,
                    _variable,
                } = delivery else {
                    panic!("Core success must reach Lua conversion")
                };
                assert_eq!(result.reservation_identity, Some(original));
                conversion.abandon();
                drop(result);
                drop(_variable);
                drop(receiver);
                collect(daemon.runtime().unwrap(), &mut wake, waiter_id, &[5]);
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Again));
            }
            Case::RegistrationRefusal => {
                let bytes = retained_reply_bytes::<SpawnConversionOutcome>().unwrap();
                let charge = memory.reserve_callback_total(bytes).unwrap();
                let (blocker, receipt) = daemon
                    .runtime()
                    .unwrap()
                    .session_spawn_conversion_reply(&operation.retirement, charge)
                    .unwrap();
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Again));
                assert!(matches!(receiver.recv_timeout(Duration::ZERO), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
                receipt.abandon();
                collect(daemon.runtime().unwrap(), &mut wake, waiter_id, &[5]);
                drop(blocker);
                finish_cleanup(&mut operation, &mut daemon, &mut state, &mut wake, waiter_id);
                let AdmittedSpawnDelivery::Unavailable(reason) =
                    receiver.recv_timeout(Duration::ZERO).unwrap()
                else {
                    panic!("confirmed cleanup must deliver an unavailable reason")
                };
                assert_eq!(reason, "conversion receipt registration refused");
                drop(receiver);
            }
            Case::DroppedReceiver => {
                drop(receiver);
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Pending));
                assert!(
                    matches!(operation.phase, Phase::Conversion),
                    "a lost delivery must wait for its conversion receipt before Core cleanup",
                );
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Pending));
                collect(daemon.runtime().unwrap(), &mut wake, waiter_id, &[5]);
                assert!(matches!(operation.poll(&mut daemon, &mut state), ControlPoll::Again));
            }
        }
        if !matches!(case, Case::RegistrationRefusal) {
            finish_cleanup(&mut operation, &mut daemon, &mut state, &mut wake, waiter_id);
        }
        assert!(matches!(operation.phase, Phase::Done));
        assert_eq!(daemon.runtime().unwrap().session_context(&context_id), None);
        assert_eq!(daemon.runtime().unwrap().session_context(&session_id.0), None);
        assert_eq!(
            operation.start.as_ref().unwrap().test_reservation_identity(),
            Some(original),
            "cleanup must retain the exact released generation until retirement",
        );
        let next_waiter = daemon.runtime().unwrap().next_waiter_id().unwrap();
        let next_retirement = daemon.runtime().unwrap().coordination_retirement(next_waiter);
        let mut next_reserve = daemon
            .runtime()
            .unwrap()
            .begin_reserve_session_for_owner(next_waiter, session_id);
        collect(daemon.runtime().unwrap(), &mut wake, next_waiter, &[1, 2]);
        let CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
            result: Ok(next_reservation),
            ..
        })) = next_reserve.poll(daemon.runtime().unwrap()) else {
            panic!("the next waiter must reserve the released session id");
        };
        assert_ne!(next_reservation.identity(), original);
        let mut next_release = daemon
            .runtime()
            .unwrap()
            .begin_release_session_reservation_for_owner(next_waiter, next_reservation);
        collect(daemon.runtime().unwrap(), &mut wake, next_waiter, &[3, 4]);
        assert!(matches!(
            next_release.poll(daemon.runtime().unwrap()),
            CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                result: Ok(SessionReservationRelease::Released),
                ..
            }))
        ));
        drop(next_retirement);
        drop(operation);
        drop(next_reserve);
        drop(next_release);
        drop(daemon);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn explicit_conversion_abandonment_confirms_owner_cleanup() {
        run(Case::ExplicitAbandonment);
    }

    #[test]
    fn registration_refusal_confirms_cleanup_before_unavailable_delivery() {
        run(Case::RegistrationRefusal);
    }

    #[test]
    fn dropped_delivery_receiver_confirms_owner_cleanup() {
        run(Case::DroppedReceiver);
    }
}
