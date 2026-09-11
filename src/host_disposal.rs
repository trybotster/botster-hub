//! Terminal payload disposal uses the original Host slot and the original Owner row.

use std::sync::{Arc, Mutex, TryLockError};

use crate::host_executor::HostWorkPermit;
use crate::lua_memory::LuaCallbackStorageLease;
use crate::runtime::entity_model::Work as ModelWork;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Pending,
    Disposed,
    Retired,
    PartialDestruction,
    SharedCleanupFault,
}

struct State {
    payload: Option<Box<dyn Send>>,
    model: Option<ModelWork>,
    client_cleanup: Option<(
        Arc<crate::package_event_router::PackageEventRouter>,
        crate::subscription::package_events::ClientCleanupWork,
    )>,
    plugin_bridges: Option<crate::runtime::TerminalPluginBridges>,
    permit: Option<HostWorkPermit>,
    outcome: Outcome,
}

pub(crate) fn boxed_payload_storage_bytes() -> Option<usize> {
    use crate::lua_memory::layout;
    // Work boxes the supplied Box. Owner and Host can initialize its mutex concurrently.
    std::mem::size_of::<Box<dyn Send>>()
        .checked_add(layout::arc_bytes::<Mutex<State>>())?
        .checked_add(2usize.checked_mul(layout::lazy_mutex_bytes())?)?
        .checked_add(layout::lease_bytes())
}

/// This handle stays in the original request or recovery row until disposal finishes.
#[derive(Clone)]
pub(crate) struct Work(
    Arc<Mutex<State>>,
    #[allow(dead_code)] // storage lease retained until Host disposal work drops
    Option<LuaCallbackStorageLease>,
);

impl std::fmt::Debug for Work {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HostDisposalWork")
    }
}

pub(crate) enum Poll {
    Pending,
    Disposed(HostWorkPermit),
    Retired,
    PartialDestruction,
    SharedCleanupFault,
}

/// A terminal split moves payload fields and retains causal fields in their original Owner record.
pub(crate) struct Parts {
    pub(crate) identity: crate::host_executor::HostJobIdentity,
    pub(crate) permit: HostWorkPermit,
    pub(crate) payload: Box<dyn Send>,
    pub(crate) model: Option<ModelWork>,
    // The payload and all disposal handles retain this admitted storage.
    pub(crate) storage: Option<LuaCallbackStorageLease>,
}

/// The original Owner row retains this record through submission and destruction.
pub(crate) struct Job {
    work: Work,
    failure: Option<crate::host_executor::HostSubmissionFailure>,
}

impl Job {
    pub(crate) fn new(parts: Parts) -> Self {
        let work = Work::with_storage(parts.payload, parts.model, parts.storage);
        Self::submit(work, parts.identity, parts.permit)
    }

    pub(crate) fn new_client(
        parts: Parts,
        router: Arc<crate::package_event_router::PackageEventRouter>,
        cleanup: crate::subscription::package_events::ClientCleanupWork,
    ) -> Self {
        let work = Work::with_storage(parts.payload, parts.model, parts.storage);
        work.0.lock().expect("new disposal record").client_cleanup = Some((router, cleanup));
        Self::submit(work, parts.identity, parts.permit)
    }

    pub(crate) fn new_plugin_bridges(
        parts: Parts,
        bridges: crate::runtime::TerminalPluginBridges,
    ) -> Self {
        let work = Work::with_storage(parts.payload, parts.model, parts.storage);
        work.0.lock().expect("new disposal record").plugin_bridges = Some(bridges);
        Self::submit(work, parts.identity, parts.permit)
    }

    fn submit(
        work: Work,
        identity: crate::host_executor::HostJobIdentity,
        permit: HostWorkPermit,
    ) -> Self {
        let failure = permit
            .dispose(
                identity,
                crate::host_executor::HostCommand::TerminalDispose(work.clone()),
            )
            .err();
        Self { work, failure }
    }

    pub(crate) fn poll(&mut self) -> Poll {
        if let Some(failure) = self.failure.take() {
            self.failure = failure
                .permit
                .dispose(failure.identity, failure.command)
                .err();
        }
        self.work.poll()
    }

    pub(crate) fn refused(&self) -> Option<crate::host_executor::HostSubmitError> {
        self.failure.as_ref().map(|failure| failure.error)
    }

    #[cfg(test)]
    pub(crate) fn test_disposed(&self) -> bool {
        self.work
            .0
            .try_lock()
            .is_ok_and(|state| state.outcome == Outcome::Disposed)
    }

    #[cfg(test)]
    pub(crate) fn test_failure_mut(
        &mut self,
    ) -> Option<&mut crate::host_executor::HostSubmissionFailure> {
        self.failure.as_mut()
    }
}

impl Parts {
    pub(crate) fn with_payload(self, payload: impl Send + 'static) -> Self {
        Self {
            payload: Box::new((self.payload, payload)),
            ..self
        }
    }
}

impl Work {
    #[allow(dead_code)] // constructs disposal work without a storage lease
    pub(crate) fn new(payload: impl Send + 'static, model: Option<ModelWork>) -> Self {
        Self::with_storage(payload, model, None)
    }

    fn with_storage(
        payload: impl Send + 'static,
        model: Option<ModelWork>,
        storage: Option<LuaCallbackStorageLease>,
    ) -> Self {
        Self(Arc::new(Mutex::new(State {
            payload: Some(Box::new(payload)),
            model,
            client_cleanup: None,
            plugin_bridges: None,
            permit: None,
            outcome: Outcome::Pending,
        })), storage)
    }

    /// The existing worker calls this in disposal mode, including after normal execution stops.
    pub(crate) fn run(&self, permit: HostWorkPermit) {
        let publish_receipt = permit.disposal_notifier();
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            state.outcome,
            Outcome::Pending,
            "terminal disposal executes once"
        );
        assert!(
            state.permit.is_none(),
            "the worker returns the original slot once"
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some((router, cleanup)) = state.client_cleanup.take() {
                match cleanup.run(&router) {
                    Ok(done) => assert!(done.closed, "terminal cleanup closes the connection"),
                    Err(failure) => {
                        state.client_cleanup = Some((router, failure.work));
                        return false;
                    }
                }
            }
            if let Some(bridges) = state.plugin_bridges.as_ref()
                && !bridges.dispose()
            {
                return false;
            }
            drop(state.plugin_bridges.take());
            if let Some(model) = state.model.as_ref() {
                model.dispose_terminal_payload();
            }
            drop(state.payload.take());
            drop(state.model.take());
            true
        }));
        state.outcome = match result {
            Ok(true) => Outcome::Disposed,
            Ok(false) => Outcome::SharedCleanupFault,
            Err(_) => {
                // A destructor may have consumed part of the payload. This is not a retryable refusal.
                Outcome::PartialDestruction
            }
        };
        state.permit = Some(permit);
        drop(state);
        publish_receipt();
    }

    pub(crate) fn poll(&self) -> Poll {
        let mut state = match self.0.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Poll::Pending,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        match state.outcome {
            Outcome::Pending => Poll::Pending,
            Outcome::Disposed => {
                state.outcome = Outcome::Retired;
                Poll::Disposed(
                    state
                        .permit
                        .take()
                        .expect("disposal retains its original slot"),
                )
            }
            Outcome::Retired => Poll::Retired,
            Outcome::PartialDestruction => Poll::PartialDestruction,
            Outcome::SharedCleanupFault => Poll::SharedCleanupFault,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{HostExecutor, HostJobIdentity};
    use crate::owner_identity::WaiterId;
    use std::time::{Duration, Instant};

    #[test]
    fn storage_lease_outlives_the_payload_and_last_work_handle() {
        use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};

        struct Payload(Arc<LuaMemoryAccount>);
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.0.usage().1, 64);
            }
        }

        let memory = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 64,
            total_callback_bytes: 64,
        })
        .unwrap();
        let storage = LuaCallbackStorageLease::new(memory.reserve_callback_total(64).unwrap());
        let owner = Work::with_storage(Payload(memory.clone()), None, Some(storage));
        let host = owner.clone();
        drop(owner);
        assert_eq!(memory.usage().1, 64);
        drop(host);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn terminal_destructor_panic_keeps_the_original_slot_without_a_disposal_receipt() {
        struct PanicDrop(std::sync::mpsc::Sender<String>);
        impl Drop for PanicDrop {
            fn drop(&mut self) {
                self.0
                    .send(
                        std::thread::current()
                            .name()
                            .unwrap_or("unnamed")
                            .to_string(),
                    )
                    .unwrap();
                panic!("terminal destructor failure");
            }
        }
        let executor = HostExecutor::new();
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut job = Job::new(Parts {
            storage: None,
            identity: HostJobIdentity::first(WaiterId(19)),
            permit: executor.try_reserve().unwrap(),
            payload: Box::new(PanicDrop(sender)),
            model: None,
        });
        assert!(
            receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .starts_with("botster-hub-host")
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match job.poll() {
                Poll::PartialDestruction => break,
                Poll::Pending => assert!(Instant::now() < deadline),
                _ => panic!("partial destruction must not report disposal"),
            }
            std::thread::yield_now();
        }
        assert_eq!(executor.outstanding(), 1);
        assert!(matches!(job.poll(), Poll::PartialDestruction));
        assert!(
            job.refused().is_none(),
            "a destructor panic is not a submission refusal"
        );
        let mut sibling = Job::new(Parts {
            storage: None,
            identity: HostJobIdentity::first(WaiterId(20)),
            permit: executor.try_reserve().unwrap(),
            payload: Box::new(()),
            model: None,
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Poll::Disposed(permit) = sibling.poll() {
                drop(permit);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the worker remains available after a destructor panic"
            );
            std::thread::yield_now();
        }
        assert_eq!(executor.outstanding(), 1);
    }
}
