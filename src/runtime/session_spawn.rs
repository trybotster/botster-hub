//! Ordinary session spawn stages shared by runtime callers.

use super::*;

mod reply;
pub(crate) use reply::{SpawnReplySender, spawn_reply_channel};

struct CountedFailure(usize);

impl std::fmt::Write for CountedFailure {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

struct BoundedFailure {
    value: String,
    limit: usize,
}

impl std::fmt::Write for BoundedFailure {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let next = self.value.len().checked_add(text.len()).ok_or(std::fmt::Error)?;
        if next > self.limit {
            return Err(std::fmt::Error);
        }
        self.value.push_str(text);
        Ok(())
    }
}

/// The plugin reports this outcome only after it attempts Lua conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // The owner continuation will collect this outcome.
pub(crate) enum SpawnConversionOutcome {
    Converted,
    Abandoned,
}

/// The local transport reports success only after its complete response write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpawnDeliveryOutcome {
    Delivered,
}

/// Dropping this receipt closes its charged ticket and wakes the exact owner.
#[derive(Debug)]
pub(crate) struct SpawnDeliveryReceipt {
    publisher: Option<crate::data_plane::driver::CoreReplyPublisher<SpawnDeliveryOutcome>>,
}

impl SpawnDeliveryReceipt {
    pub(crate) fn new(
        publisher: crate::data_plane::driver::CoreReplyPublisher<SpawnDeliveryOutcome>,
    ) -> Self {
        Self {
            publisher: Some(publisher),
        }
    }

    pub(crate) fn delivered(mut self) {
        if let Some(publisher) = self.publisher.take() {
            publisher.publish(SpawnDeliveryOutcome::Delivered);
        }
    }
}

/// A dropped receipt reports loss through the registered owner phase.
/// The publisher retains its funded channel storage until both endpoints end.
#[derive(Debug)]
#[allow(dead_code)] // Host delivery will carry this receipt to the plugin.
pub(crate) struct SpawnConversionReceipt {
    publisher: crate::data_plane::driver::CoreReplyPublisher<SpawnConversionOutcome>,
}

#[allow(dead_code)] // The owner continuation will construct this receipt.
impl SpawnConversionReceipt {
    pub(crate) fn new(
        publisher: crate::data_plane::driver::CoreReplyPublisher<SpawnConversionOutcome>,
    ) -> Self {
        Self { publisher }
    }

    pub(crate) fn converted(self) {
        self.publisher.publish(SpawnConversionOutcome::Converted);
    }

    pub(crate) fn abandon(self) {
        self.publisher.publish(SpawnConversionOutcome::Abandoned);
    }
}

enum CoreBinding {
    Ownerless,
    ClientOwner(crate::owner_identity::WaiterId),
    Owner {
        waiter_id: crate::owner_identity::WaiterId,
        allowance: crate::session_types::ChargedMaterializationAllowance,
    },
}

impl CoreBinding {
    fn begin(&self, runtime: &HubRuntime, operation: CoreOperation) -> CoreOperationTracker {
        let ticket = match self {
            Self::Ownerless => runtime.core_daemon.begin(operation),
            Self::ClientOwner(waiter_id) => runtime.core_daemon.begin_for_owner(*waiter_id, operation),
            Self::Owner { waiter_id, .. } => {
                runtime.core_daemon.begin_for_owner(*waiter_id, operation)
            }
        };
        CoreOperationTracker::new(ticket)
    }

    fn context_charge(
        &mut self,
        runtime: &HubRuntime,
        context: &HubSessionContext,
    ) -> Option<crate::lua_memory::LuaCallbackCharge> {
        let bytes = stored_context_bytes(context)?;
        match self {
            Self::Owner { allowance, .. } => {
                allowance.parent.grow(bytes).ok()?;
                allowance.parent.split_fixed(bytes)
            }
            Self::Ownerless | Self::ClientOwner(_) => {
                runtime.lua_memory.reserve_callback_total(bytes).ok()
            }
        }
    }
}

/// One session-type spawn in flight on the Core owner thread.
pub(crate) struct SessionTypeSpawnStart {
    tracker: CoreOperationTracker,
    stage: PluginSpawnStage,
    reservation: Option<SessionReservation>,
    spawn: SpawnSessionRequest,
    spawn_error: Option<CoreDaemonError>,
    reserve_operation_id: Option<PendingOperationId>,
    retry_tokens: Vec<SessionReservation>,
    retry_keep: Vec<SessionReservation>,
    context_published: bool,
    context: Option<HubSessionContext>,
    session_type_id: String,
    context_id: String,
    context_keys: Vec<String>,
    abandon_requested: bool,
    cleanup: CleanupStage,
    // Destroy every payload before the owner binding releases its allowance.
    binding: CoreBinding,
}

#[derive(Clone, Copy)]
enum CleanupStage {
    SpawnPending,
    Installed,
    Shutdown,
    Remove,
    Release,
    Confirmed,
    Unresolved,
}

/// Confirmed establishes Core cleanup. Context retirement remains separate.
pub(crate) enum SessionSpawnCleanupPoll {
    Pending,
    Confirmed,
    Unresolved,
}

impl HubRuntime {
    pub(crate) fn session_spawn_host_reply(
        &self,
        retirement: &crate::data_plane::driver::CoreWaiterRetirement,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<
        (
            crate::data_plane::driver::ChargedCoreTicket<()>,
            crate::data_plane::driver::CoreReplyPublisher<()>,
        ),
        crate::lua_memory::LuaCallbackCharge,
    > {
        self.core_daemon.local_reply_for_owner(retirement, charge)
    }

    pub(crate) fn session_spawn_conversion_reply(
        &self,
        retirement: &crate::data_plane::driver::CoreWaiterRetirement,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<
        (
            crate::data_plane::driver::ChargedCoreTicket<SpawnConversionOutcome>,
            SpawnConversionReceipt,
        ),
        crate::lua_memory::LuaCallbackCharge,
    > {
        self.core_daemon
            .local_reply_for_owner(retirement, charge)
            .map(|(ticket, publisher)| (ticket, SpawnConversionReceipt::new(publisher)))
    }

    /// Daemon requests fund their local delivery ticket from the callback pool.
    /// Plugin requests must split the original admitted callback parent instead.
    pub(crate) fn daemon_session_spawn_delivery_charge(
        &self,
    ) -> Option<crate::lua_memory::LuaCallbackCharge> {
        let bytes = crate::data_plane::driver::retained_reply_bytes::<SpawnDeliveryOutcome>()?;
        self.lua_memory.reserve_callback_total(bytes).ok()
    }

    pub(crate) fn session_spawn_delivery_reply(
        &self,
        retirement: &crate::data_plane::driver::CoreWaiterRetirement,
        charge: crate::lua_memory::LuaCallbackCharge,
    ) -> Result<
        (
            crate::data_plane::driver::ChargedCoreTicket<SpawnDeliveryOutcome>,
            SpawnDeliveryReceipt,
        ),
        crate::lua_memory::LuaCallbackCharge,
    > {
        self.core_daemon
            .local_reply_for_owner(retirement, charge)
            .map(|(ticket, publisher)| (ticket, SpawnDeliveryReceipt::new(publisher)))
    }

    pub(super) fn fulfill_session_type_spawn(
        &self,
        pending: &PendingSessionTypeSpawn,
    ) -> Result<SessionTypeSpawnStart, String> {
        if !package_allows_session_type_spawn(&pending.package_records, &pending.plugin_key) {
            return Err("plugin package lacks session_type_spawn capability".to_string());
        }

        let records = pending.package_records.iter().collect::<Vec<_>>();
        let state = self.state();
        let mut materialized = materialize_session_type(
            &self.config,
            &records,
            &state,
            &pending.session_type_id,
            pending.request.clone(),
        )
        .map_err(|error| format!("{}: {}", error.kind, error.message))?;
        drop(state);
        materialized.metadata =
            session_type_plugin_metadata(materialized.metadata, &pending.plugin_key);
        Ok(self.begin_materialized_session_type_spawn(materialized, CoreBinding::Ownerless))
    }

    /// Start the existing Core stages after the Host worker returns its product.
    pub(crate) fn begin_session_type_spawn_for_owner(
        &self,
        waiter_id: crate::owner_identity::WaiterId,
        product: crate::session_types::ChargedSessionTypeMaterialization,
    ) -> SessionTypeSpawnStart {
        let (materialized, allowance) = product.into_parts();
        self.begin_materialized_session_type_spawn(
            materialized,
            CoreBinding::Owner {
                waiter_id,
                allowance,
            },
        )
    }

    /// Direct client callers use the same reservation and context stages.
    pub(crate) fn begin_client_session_type_spawn(
        &self,
        materialized: crate::session_types::MaterializedSessionType,
        waiter_id: crate::owner_identity::WaiterId,
    ) -> SessionTypeSpawnStart {
        self.begin_materialized_session_type_spawn(materialized, CoreBinding::ClientOwner(waiter_id))
    }

    fn begin_materialized_session_type_spawn(
        &self,
        materialized: crate::session_types::MaterializedSessionType,
        binding: CoreBinding,
    ) -> SessionTypeSpawnStart {
        let spawn = SpawnSessionRequest {
            request: materialized.spawn_request,
            metadata: materialized.metadata,
        };
        let session_id = spawn.request.session_id.clone();
        let retry_tokens = match &binding {
            CoreBinding::Ownerless => self.take_retained_reservations(),
            // An owner row must not take another operation's cleanup authority.
            CoreBinding::ClientOwner(_) | CoreBinding::Owner { .. } => Vec::new(),
        };
        let stage = if retry_tokens.is_empty() {
            PluginSpawnStage::Reserve
        } else {
            PluginSpawnStage::RetryRetained
        };
        let operation = if matches!(stage, PluginSpawnStage::Reserve) {
            CoreOperation::ReserveSession(session_id)
        } else {
            CoreOperation::ReleaseSessionReservation(retry_tokens[0].clone())
        };
        let tracker = binding.begin(self, operation);
        SessionTypeSpawnStart {
            binding,
            tracker,
            stage,
            reservation: None,
            spawn,
            spawn_error: None,
            reserve_operation_id: None,
            retry_tokens,
            retry_keep: Vec::new(),
            context_published: false,
            context: Some(materialized.context),
            session_type_id: materialized.resolved.session_type.session_type_id,
            context_id: materialized.resolved.context_id,
            context_keys: materialized.resolved.context_keys,
            abandon_requested: false,
            cleanup: CleanupStage::SpawnPending,
        }
    }

    pub(crate) fn finish_session_type_spawn(
        &self,
        start: &SessionTypeSpawnStart,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<PluginSessionTypeSpawned, String> {
        let outcome = result.map_err(|failure| {
            if start.context_published
                && failure.disposition == Some(SessionReservationRelease::Released)
                && let Some(reservation) = start.reservation.as_ref()
            {
                self.retract_spawn_context_aliases(
                    &start.context_id,
                    &start.spawn.request.session_id.0,
                    reservation.identity(),
                );
            }
            match failure.disposition {
                Some(SessionReservationRelease::RetainedUnconfirmed) => {
                    format!("cleanup_unconfirmed: {}", failure.error)
                }
                _ => format!("session type spawn failed: {}", failure.error),
            }
        })?;
        Ok(PluginSessionTypeSpawned {
            session_id: outcome.session_id.0,
            lifecycle: session_lifecycle_label(outcome.lifecycle).to_string(),
            session_type_id: start.session_type_id.clone(),
            context_id: start.context_id.clone(),
            context_keys: start.context_keys.clone(),
            reservation_identity: start.reservation.as_ref().map(SessionReservation::identity),
        })
    }

    pub(crate) fn finish_client_session_type_spawn(
        &self,
        start: &SessionTypeSpawnStart,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<CoreSession, CoreDaemonError> {
        result.map_err(|failure| {
            if start.context_published
                && failure.disposition == Some(SessionReservationRelease::Released)
                && let Some(reservation) = start.reservation.as_ref()
            {
                self.retract_spawn_context_aliases(
                    &start.context_id,
                    &start.spawn.request.session_id.0,
                    reservation.identity(),
                );
            }
            failure.error
        })
    }
}

impl SessionTypeSpawnStart {
    /// Fund the plugin error before formatting it while Core failure data is live.
    pub(crate) fn charged_plugin_failure(
        &mut self,
        failure: &PluginSpawnFailure,
    ) -> Option<(String, crate::lua_memory::LuaCallbackCharge, crate::lua_memory::LuaCallbackCharge)> {
        use std::fmt::Write;

        let CoreBinding::Owner { allowance, .. } = &mut self.binding else {
            return None;
        };
        let prefix = if failure.disposition == Some(SessionReservationRelease::RetainedUnconfirmed)
        {
            "cleanup_unconfirmed: "
        } else {
            "session type spawn failed: "
        };
        let mut counted = CountedFailure(prefix.len());
        write!(&mut counted, "{}", failure.error).ok()?;
        let render_bytes = crate::session_types::LUA_SPAWN_REFUSAL_PREFIX
            .len()
            .checked_add(counted.0)?;
        let total_bytes = counted.0.checked_add(render_bytes)?;
        allowance.parent.grow(total_bytes).ok()?;
        let charge = allowance.parent.split_fixed(counted.0)?;
        let render_charge = allowance.parent.split_fixed(render_bytes)?;
        let mut message = BoundedFailure {
            value: String::with_capacity(counted.0),
            limit: counted.0,
        };
        message.write_str(prefix).ok()?;
        write!(&mut message, "{}", failure.error).ok()?;
        Some((message.value, charge, render_charge))
    }

    /// Reserve response copies before finish_session_type_spawn constructs them.
    pub(crate) fn charged_plugin_response(
        &mut self,
        session: &CoreSession,
    ) -> Option<(
        crate::lua_memory::LuaCallbackCharge,
        crate::lua_memory::LuaCallbackCharge,
    )> {
        let CoreBinding::Owner { allowance, .. } = &mut self.binding else {
            return None;
        };
        let keys = self.context_keys.len();
        let key_slots = keys
            .checked_mul(std::mem::size_of::<String>())?
            .checked_mul(3)?;
        let key_strings = self
            .context_keys
            .iter()
            .try_fold(0usize, |sum, key| sum.checked_add(key.len()))?;
        let response_bytes = session
            .session_id
            .0
            .len()
            .checked_add(session_lifecycle_label(session.lifecycle.clone()).len())?
            .checked_add(self.session_type_id.len())?
            .checked_add(self.context_id.len())?
            .checked_add(key_slots)?
            .checked_add(key_strings)?;
        let ticket_bytes =
            crate::data_plane::driver::retained_reply_bytes::<SpawnConversionOutcome>()?;
        allowance
            .parent
            .grow(response_bytes.checked_add(ticket_bytes)?)
            .ok()?;
        let ticket = allowance.parent.split_fixed(ticket_bytes)?;
        let response = allowance.parent.split_fixed(response_bytes)?;
        Some((response, ticket))
    }

    /// Collect the waiter's exact phase before advancing terminal cleanup.
    /// Unresolved retains the complete stage; it never authorizes disposal.
    pub(crate) fn abandon_and_poll_cleanup(
        &mut self,
        runtime: &HubRuntime,
    ) -> SessionSpawnCleanupPoll {
        let CoreBinding::Owner { .. } = &self.binding else {
            return SessionSpawnCleanupPoll::Unresolved;
        };
        self.abandon_requested = true;
        loop {
            match self.cleanup {
                CleanupStage::SpawnPending => match self.poll(runtime) {
                    PluginSpawnPoll::Pending => return SessionSpawnCleanupPoll::Pending,
                    PluginSpawnPoll::Ready(_) => {
                        // A failure without a definitive receipt retains authority.
                        if matches!(self.cleanup, CleanupStage::SpawnPending) {
                            self.cleanup = CleanupStage::Unresolved;
                        }
                    }
                },
                CleanupStage::Installed => {
                    self.tracker = self.binding.begin(
                        runtime,
                        CoreOperation::ShutdownSession(self.spawn.request.session_id.clone()),
                    );
                    self.cleanup = CleanupStage::Shutdown;
                }
                CleanupStage::Shutdown => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return SessionSpawnCleanupPoll::Pending,
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ShutdownSession {
                        result: Ok(()),
                        ..
                    })) => {
                        self.tracker = self.binding.begin(
                            runtime,
                            CoreOperation::RemoveSession(self.spawn.request.session_id.clone()),
                        );
                        self.cleanup = CleanupStage::Remove;
                    }
                    _ => self.cleanup = CleanupStage::Unresolved,
                },
                CleanupStage::Remove => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return SessionSpawnCleanupPoll::Pending,
                    CoreTicketPoll::Ready(Ok(CoreCompletion::RemoveSession {
                        result: Ok(true),
                        ..
                    })) => {
                        let Some(reservation) = self.reservation.as_ref() else {
                            self.cleanup = CleanupStage::Unresolved;
                            continue;
                        };
                        self.tracker = self.binding.begin(
                            runtime,
                            CoreOperation::ReleaseSessionReservation(reservation.clone()),
                        );
                        self.cleanup = CleanupStage::Release;
                    }
                    // False means the session is still live, not absent.
                    _ => self.cleanup = CleanupStage::Unresolved,
                },
                CleanupStage::Release => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return SessionSpawnCleanupPoll::Pending,
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result: Ok(SessionReservationRelease::Released),
                        ..
                    })) => self.cleanup = CleanupStage::Confirmed,
                    _ => self.cleanup = CleanupStage::Unresolved,
                },
                CleanupStage::Confirmed => {
                    if self.context_published && let Some(reservation) = self.reservation.as_ref() {
                        runtime.retract_spawn_context_aliases(
                            &self.context_id,
                            &self.spawn.request.session_id.0,
                            reservation.identity(),
                        );
                        self.context_published = false;
                    }
                    return SessionSpawnCleanupPoll::Confirmed;
                }
                CleanupStage::Unresolved => return SessionSpawnCleanupPoll::Unresolved,
            }
        }
    }

    fn poll_core(
        &mut self,
        runtime: &HubRuntime,
    ) -> CoreTicketPoll<Result<CoreCompletion, CoreDaemonError>> {
        if self.abandon_requested {
            self.tracker.poll_terminal()
        } else {
            self.tracker.poll(runtime)
        }
    }

    pub(crate) fn poll(&mut self, runtime: &HubRuntime) -> PluginSpawnPoll {
        loop {
            match self.stage {
                PluginSpawnStage::RetryRetained => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        if !self.retry_tokens.is_empty() {
                            self.retry_keep.push(self.retry_tokens.remove(0));
                        }
                        self.continue_retry_or_reserve(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let held = self.retry_tokens.remove(0);
                        match result {
                            Ok(SessionReservationRelease::Released) => {}
                            Ok(_) | Err(_) => self.retry_keep.push(held),
                        }
                        self.continue_retry_or_reserve(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Reserve => match {
                    let result = self.poll_core(runtime);
                    // Poll can accept begin and lose completion in the same call.
                    self.reserve_operation_id = self.tracker.accepted_id();
                    result
                } {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        self.cleanup = CleanupStage::Confirmed;
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost => {
                        let Some(reserve_id) = self.reserve_operation_id else {
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        };
                        self.tracker = self.binding.begin(
                            runtime,
                            CoreOperation::LookupSessionReservation {
                                session_id: self.spawn.request.session_id.clone(),
                                reserve_operation_id: reserve_id,
                            },
                        );
                        self.stage = PluginSpawnStage::Lookup;
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        return spawn_fail(error, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReserveSession {
                        result, ..
                    })) => match result {
                        Ok(reserved) => {
                            self.reservation = Some(reserved.clone());
                            if self.abandon_requested {
                                self.spawn_error = Some(CoreDaemonError::Shutdown);
                                self.release_or_retain(runtime);
                                continue;
                            }
                            let published = self
                                .binding
                                .context_charge(
                                    runtime,
                                    self.context.as_ref().expect("context precedes publication"),
                                )
                                .ok_or(())
                                .and_then(|charge| {
                                    let context = self.context.take().ok_or(())?;
                                    runtime
                                        .publish_spawn_context(context, &reserved, charge)
                                        .map_err(|_| ())
                                });
                            if published.is_err() {
                                self.spawn_error = Some(CoreDaemonError::Shutdown);
                                self.release_or_retain(runtime);
                                continue;
                            }
                            self.context_published = true;
                            self.tracker = self.binding.begin(
                                runtime,
                                CoreOperation::SpawnReserved {
                                    reservation: reserved,
                                    request: self.spawn.clone(),
                                },
                            );
                            self.stage = PluginSpawnStage::SpawnReserved;
                        }
                        Err(error) => {
                            self.cleanup = CleanupStage::Confirmed;
                            return spawn_fail(error, None);
                        }
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Lookup => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        return spawn_fail(core_bridge_error(CoreTicketError::Overloaded), None);
                    }
                    CoreTicketPoll::Lost | CoreTicketPoll::Ready(Err(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::LookupSessionReservation {
                        result,
                        ..
                    })) => match result {
                        Ok(Some(reserved)) => {
                            self.reservation = Some(reserved.clone());
                            self.spawn_error = Some(CoreDaemonError::Shutdown);
                            self.tracker = self
                                .binding
                                .begin(runtime, CoreOperation::ReleaseSessionReservation(reserved));
                            self.stage = PluginSpawnStage::Release;
                        }
                        Ok(None) => {
                            self.cleanup = CleanupStage::Confirmed;
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        }
                        Err(error) => return spawn_fail(error, None),
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::SpawnReserved => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
                        self.spawn_error = Some(core_bridge_error(CoreTicketError::Overloaded));
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Lost => {
                        self.spawn_error = Some(CoreDaemonError::Shutdown);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Err(error)) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Installed { session },
                        ..
                    })) => {
                        self.cleanup = CleanupStage::Installed;
                        return PluginSpawnPoll::Ready(Ok(session));
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::Refused { error },
                        ..
                    }))
                    | CoreTicketPoll::Ready(Ok(CoreCompletion::SpawnReserved {
                        result: ReservedSpawnResult::AdmittedFailure { error, .. },
                        ..
                    })) => {
                        self.spawn_error = Some(error);
                        self.release_or_retain(runtime);
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Release => match self.poll_core(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused
                    | CoreTicketPoll::Lost
                    | CoreTicketPoll::Ready(Err(_)) => {
                        self.keep_reservation(runtime);
                        return spawn_fail(
                            self.spawn_error.take().unwrap_or(CoreDaemonError::Shutdown),
                            Some(SessionReservationRelease::RetainedUnconfirmed),
                        );
                    }
                    CoreTicketPoll::Ready(Ok(CoreCompletion::ReleaseSessionReservation {
                        result,
                        ..
                    })) => {
                        let error = self.spawn_error.take().unwrap_or(CoreDaemonError::Shutdown);
                        match result {
                            Ok(SessionReservationRelease::Released) => {
                                self.cleanup = CleanupStage::Confirmed;
                                return spawn_fail(
                                    error,
                                    Some(SessionReservationRelease::Released),
                                );
                            }
                            Ok(disposition) => {
                                self.keep_reservation(runtime);
                                return spawn_fail(error, Some(disposition));
                            }
                            Err(_) => {
                                self.keep_reservation(runtime);
                                return spawn_fail(
                                    error,
                                    Some(SessionReservationRelease::RetainedUnconfirmed),
                                );
                            }
                        }
                    }
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
            }
        }
    }

    fn release_or_retain(&mut self, runtime: &HubRuntime) {
        if let Some(held) = self.reservation.clone() {
            self.tracker = self
                .binding
                .begin(runtime, CoreOperation::ReleaseSessionReservation(held));
            self.stage = PluginSpawnStage::Release;
        }
    }

    fn keep_reservation(&mut self, runtime: &HubRuntime) {
        // The daemon row retains the complete unresolved operation.
        // The synchronous path still uses its existing reservation store.
        if matches!(&self.binding, CoreBinding::Ownerless)
            && let Some(held) = self.reservation.take()
        {
            runtime.retain_reservation(held);
        }
    }

    fn continue_retry_or_reserve(&mut self, runtime: &HubRuntime) {
        if self.retry_tokens.is_empty() {
            runtime.merge_retained_reservations(std::mem::take(&mut self.retry_keep));
            self.tracker = self.binding.begin(
                runtime,
                CoreOperation::ReserveSession(self.spawn.request.session_id.clone()),
            );
            self.stage = PluginSpawnStage::Reserve;
        } else {
            self.tracker = self.binding.begin(
                runtime,
                CoreOperation::ReleaseSessionReservation(self.retry_tokens[0].clone()),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner_identity::{OwnerWorkIdentity, WaiterId};

    fn collect_phases(
        runtime: &HubRuntime,
        receiver: &mut tokio::sync::mpsc::Receiver<crate::daemon::control::message::ControlMessage>,
        waiter_id: WaiterId,
        phases: [u64; 2],
    ) {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let mut collected = executor.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut collected = Vec::new();
                while collected.len() < 2 {
                    receiver.recv().await.expect("Core must publish its wake");
                    collected.extend(runtime.take_owner_core_completions(2));
                }
                collected
            })
            .await
            .expect("the live Core must complete both phases")
        });
        collected.sort();
        assert_eq!(
            collected,
            phases.map(|phase| OwnerWorkIdentity { waiter_id, phase })
        );
    }

    #[test]
    fn ordinary_reserve_loss_in_first_poll_looks_up_original_live_reservation() {
        let runtime = super::super::tests::family_runtime("ordinary-reserve-first-poll-loss");
        let waiter_id = runtime.next_waiter_id().unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(64);
        runtime.bind_data_plane_owner_wake(sender);
        let retirement = runtime.coordination_retirement(waiter_id);
        runtime
            .core_daemon
            .test_lose_reserve_completion_for(waiter_id);
        let session_id = SessionId("ordinary-reserve-first-poll-loss".into());
        let binding = CoreBinding::Owner {
            waiter_id,
            allowance: crate::session_types::ChargedMaterializationAllowance::empty_for_test(
                runtime.lua_memory.reserve_callback_total(0).unwrap(),
            ),
        };
        let tracker = binding.begin(&runtime, CoreOperation::ReserveSession(session_id.clone()));
        let mut start = SessionTypeSpawnStart {
            tracker,
            stage: PluginSpawnStage::Reserve,
            reservation: None,
            spawn: SpawnSessionRequest {
                request: crate::client_api::spawn_request(
                    &runtime,
                    RequestId("ordinary-reserve-first-poll-loss".into()),
                    session_id.clone(),
                    "exit 0".into(),
                ),
                metadata: crate::client_api::client_session_metadata(),
            },
            spawn_error: None,
            reserve_operation_id: None,
            retry_tokens: Vec::new(),
            retry_keep: Vec::new(),
            context_published: false,
            context: Some(HubSessionContext {
                context_id: "context-first-poll-loss".into(),
                session_id: session_id.clone(),
                values: BTreeMap::new(),
            }),
            session_type_id: "fixture".into(),
            context_id: "context-first-poll-loss".into(),
            context_keys: Vec::new(),
            abandon_requested: false,
            cleanup: CleanupStage::SpawnPending,
            binding,
        };
        collect_phases(&runtime, &mut receiver, waiter_id, [1, 2]);
        assert_eq!(start.tracker.pending_id(), None);
        assert_eq!(start.tracker.accepted_id(), None);
        assert!(matches!(start.poll(&runtime), PluginSpawnPoll::Pending));
        let accepted = start
            .reserve_operation_id
            .expect("begin accepted the reserve");
        assert!(matches!(start.stage, PluginSpawnStage::Lookup));

        collect_phases(&runtime, &mut receiver, waiter_id, [3, 4]);
        assert!(matches!(start.poll(&runtime), PluginSpawnPoll::Pending));
        assert!(matches!(start.stage, PluginSpawnStage::Release));
        let reservation = start
            .reservation
            .as_ref()
            .expect("lookup found the reservation");
        assert_eq!(reservation.session_id(), &session_id);
        assert_eq!(reservation.request_id(), Some(accepted.0));
        assert!(!start.context_published);

        collect_phases(&runtime, &mut receiver, waiter_id, [5, 6]);
        assert!(matches!(
            start.poll(&runtime),
            PluginSpawnPoll::Ready(Err(PluginSpawnFailure {
                disposition: Some(SessionReservationRelease::Released),
                ..
            }))
        ));
        drop(start);
        drop(retirement);
    }
}
