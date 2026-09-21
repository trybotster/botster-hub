//! Ordinary session spawn stages shared by runtime callers.

use super::*;

#[derive(Clone, Copy)]
enum CoreBinding {
    Ownerless,
    // The daemon constructor will supply its admitted row identity.
    #[allow(dead_code)]
    Owner(crate::owner_identity::WaiterId),
}

impl CoreBinding {
    fn begin(self, runtime: &HubRuntime, operation: CoreOperation) -> CoreOperationTracker {
        let ticket = match self {
            Self::Ownerless => runtime.core_daemon.begin(operation),
            Self::Owner(waiter_id) => runtime.core_daemon.begin_for_owner(waiter_id, operation),
        };
        CoreOperationTracker::new(ticket)
    }
}

/// One session-type spawn in flight on the Core owner thread.
pub(super) struct SessionTypeSpawnStart {
    binding: CoreBinding,
    tracker: CoreOperationTracker,
    stage: PluginSpawnStage,
    reservation: Option<SessionReservation>,
    spawn: SpawnSessionRequest,
    spawn_error: Option<CoreDaemonError>,
    reserve_operation_id: Option<PendingOperationId>,
    retry_tokens: Vec<SessionReservation>,
    retry_keep: Vec<SessionReservation>,
    context_published: bool,
    context: HubSessionContext,
    session_type_id: String,
    context_id: String,
    context_keys: Vec<String>,
}

impl HubRuntime {
    pub(super) fn fulfill_session_type_spawn(
        &self,
        pending: &PendingSessionTypeSpawn,
    ) -> Result<SessionTypeSpawnStart, String> {
        if !package_allows_session_type_spawn(&pending.package_records, &pending.plugin_key) {
            return Err("plugin package lacks session_type_spawn capability".to_string());
        }

        let records = pending.package_records.iter().collect::<Vec<_>>();
        let state = self.state();
        let materialized = materialize_session_type(
            &self.config,
            &records,
            &state,
            &pending.session_type_id,
            pending.request.clone(),
        )
        .map_err(|error| format!("{}: {}", error.kind, error.message))?;
        drop(state);
        let context = materialized.context.clone();
        let metadata = session_type_plugin_metadata(materialized.metadata, &pending.plugin_key);
        let spawn = SpawnSessionRequest {
            request: materialized.spawn_request,
            metadata,
        };
        let session_id = spawn.request.session_id.clone();
        let retry_tokens = self.take_retained_reservations();
        let stage = if retry_tokens.is_empty() {
            PluginSpawnStage::Reserve
        } else {
            PluginSpawnStage::RetryRetained
        };
        let binding = CoreBinding::Ownerless;
        let operation = if matches!(stage, PluginSpawnStage::Reserve) {
            CoreOperation::ReserveSession(session_id)
        } else {
            CoreOperation::ReleaseSessionReservation(retry_tokens[0].clone())
        };
        let tracker = binding.begin(self, operation);
        Ok(SessionTypeSpawnStart {
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
            context,
            session_type_id: materialized.resolved.session_type.session_type_id,
            context_id: materialized.resolved.context_id,
            context_keys: materialized.resolved.context_keys,
        })
    }

    pub(super) fn finish_session_type_spawn(
        &self,
        start: &SessionTypeSpawnStart,
        result: Result<CoreSession, PluginSpawnFailure>,
    ) -> Result<PluginSessionTypeSpawned, String> {
        let context = &start.context;
        let outcome = result.map_err(|failure| {
            if start.context_published {
                self.retract_spawn_context(context);
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
        })
    }
}

impl SessionTypeSpawnStart {
    pub(super) fn poll(&mut self, runtime: &HubRuntime) -> PluginSpawnPoll {
        loop {
            if let Some(pending_id) = self.tracker.pending_id()
                && matches!(self.stage, PluginSpawnStage::Reserve)
            {
                self.reserve_operation_id = Some(pending_id);
            }
            match self.stage {
                PluginSpawnStage::RetryRetained => match self.tracker.poll(runtime) {
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
                PluginSpawnStage::Reserve => match self.tracker.poll(runtime) {
                    CoreTicketPoll::Pending => return PluginSpawnPoll::Pending,
                    CoreTicketPoll::Refused => {
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
                            if runtime.publish_spawn_context(&self.context).is_err() {
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
                        Err(error) => return spawn_fail(error, None),
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::Lookup => match self.tracker.poll(runtime) {
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
                            return spawn_fail(CoreDaemonError::Shutdown, None);
                        }
                        Err(error) => return spawn_fail(error, None),
                    },
                    CoreTicketPoll::Ready(Ok(_)) => {
                        return spawn_fail(CoreDaemonError::Shutdown, None);
                    }
                },
                PluginSpawnStage::SpawnReserved => match self.tracker.poll(runtime) {
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
                PluginSpawnStage::Release => match self.tracker.poll(runtime) {
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
        if let Some(held) = self.reservation.take() {
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
