//! Bounded publication requests shared by Lua workers and the runtime owner.

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError, Weak, mpsc};
use std::thread;
use std::time::Duration;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::package_entity_fanout::{
    PackageEntityMutation, PackageEntityPublishResult, prepare_publish_mutation,
};
use botster_core::PluginKey;

const PUBLICATION_CAPACITY: usize = 256;
const REQUEST_BYTE_LIMIT: usize = 1024 * 1024;
const PUBLICATION_BYTE_CAPACITY: usize = 8 * 1024 * 1024;
type PublishResult = Result<PackageEntityPublishResult, String>;

/// One publication charge remains live until its last descendant releases it.
#[derive(Clone)]
pub struct EntityPublishPermit(Arc<PublicationCharge>);

struct PublicationCharge {
    account: Weak<Shared>,
    bytes: usize,
}

impl std::fmt::Debug for EntityPublishPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntityPublishPermit")
            .field("bytes", &self.0.bytes)
            .finish()
    }
}

impl PartialEq for EntityPublishPermit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for EntityPublishPermit {}

impl Drop for PublicationCharge {
    fn drop(&mut self) {
        if let Some(account) = self.account.upgrade() {
            account
                .retained_bytes
                .fetch_sub(self.bytes, Ordering::SeqCst);
            account.retained_count.fetch_sub(1, Ordering::SeqCst);
            account.notify();
        }
    }
}

#[derive(Clone)]
pub struct HubEntityPublishBridge {
    owner_thread: thread::ThreadId,
    shared: Arc<Shared>,
    registrations: crate::lifecycle::EntityProviderRegistrations,
}

struct Shared {
    queue: Mutex<Queue>,
    count: AtomicUsize,
    retained_count: AtomicUsize,
    retained_bytes: AtomicUsize,
    blocked: AtomicBool,
    interest: AtomicBool,
    faulted: AtomicBool,
    progress: AtomicBool,
    owner: OnceLock<ControlSender>,
    reject_next: AtomicBool,
}

struct Queue {
    pending: VecDeque<PendingEntityPublishRequest>,
    next_token: u64,
}

pub(crate) struct PendingEntityPublishRequest {
    pub(crate) token: u64,
    pub(crate) registration: crate::lifecycle::EntityProviderRegistration,
    pub(crate) mutation: PackageEntityMutation,
    pub(crate) scope_id: Option<u64>,
    pub(crate) response: mpsc::Sender<PublishResult>,
    pub(crate) identity: crate::package_event_router::LeaseIdentity,
}

pub(super) enum EntityPublishError {
    NeverQueued(String),
    OwnerFinished(String),
    TimeoutInFlight,
}

/// Declare this notice before the guard. Its destructor runs after unlock.
struct UnlockNotice<'a> {
    shared: &'a Shared,
    acquired: Cell<bool>,
    changed: Cell<bool>,
}

impl Drop for UnlockNotice<'_> {
    fn drop(&mut self) {
        if !self.acquired.get() {
            return;
        }
        let fault =
            self.shared.queue.is_poisoned() && !self.shared.faulted.swap(true, Ordering::SeqCst);
        self.shared.blocked.store(false, Ordering::SeqCst);
        if self.shared.interest.swap(false, Ordering::SeqCst) || self.changed.get() || fault {
            self.shared.notify();
        }
    }
}

impl Shared {
    fn notify(&self) {
        if !self.progress.swap(true, Ordering::SeqCst) {
            self.doorbell();
        }
    }

    fn doorbell(&self) {
        if let Some(owner) = self.owner.get() {
            let _ = owner.try_send(ControlMessage::EntityPublishProgress);
        }
    }

    fn try_guard<'a>(&'a self, notice: &UnlockNotice<'_>) -> Result<MutexGuard<'a, Queue>, ()> {
        match self.queue.try_lock() {
            Ok(guard) => {
                notice.acquired.set(true);
                Ok(guard)
            }
            Err(TryLockError::WouldBlock) => Err(()),
            Err(TryLockError::Poisoned(_)) => {
                if !self.faulted.swap(true, Ordering::SeqCst) {
                    self.notify();
                }
                Err(())
            }
        }
    }
}

impl HubEntityPublishBridge {
    pub(crate) fn new(registrations: crate::lifecycle::EntityProviderRegistrations) -> Self {
        Self {
            owner_thread: thread::current().id(),
            registrations,
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue {
                    pending: VecDeque::new(),
                    next_token: 1,
                }),
                count: AtomicUsize::new(0),
                retained_count: AtomicUsize::new(0),
                retained_bytes: AtomicUsize::new(0),
                blocked: AtomicBool::new(false),
                interest: AtomicBool::new(false),
                faulted: AtomicBool::new(false),
                progress: AtomicBool::new(false),
                owner: OnceLock::new(),
                reject_next: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        if let Err(sender) = self.shared.owner.set(sender) {
            assert!(self.shared.owner.get().unwrap().same_channel(&sender));
        }
        if self.shared.progress.load(Ordering::SeqCst) {
            self.shared.doorbell();
        }
    }

    pub(crate) fn take_progress_notification(&self) -> bool {
        self.shared.progress.swap(false, Ordering::SeqCst)
    }

    /// The preparation callback must latch faults before it releases the bridge guard.
    pub(crate) fn retain_faulted(&self) {
        if !self.shared.faulted.swap(true, Ordering::SeqCst) {
            self.shared.notify();
        }
    }

    pub(crate) fn is_faulted(&self) -> bool {
        self.shared.faulted.load(Ordering::SeqCst) || self.shared.queue.is_poisoned()
    }

    pub(crate) fn ready(&self) -> bool {
        self.pending_publish_count() > 0
            && !self.shared.blocked.load(Ordering::SeqCst)
            && !self.shared.faulted.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn reject_next_publish(&self) {
        self.shared.reject_next.store(true, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn pending_publish_count(&self) -> usize {
        self.shared.count.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn retained_counts(&self) -> (usize, usize) {
        (
            self.shared.retained_count.load(Ordering::SeqCst),
            self.shared.retained_bytes.load(Ordering::SeqCst),
        )
    }

    pub(crate) fn prepare_registration(
        &self,
        package: &str,
        mutation: &PackageEntityMutation,
    ) -> Result<crate::lifecycle::EntityProviderRegistration, String> {
        let registration = self.registrations.select(package, mutation.entity_type())?;
        let entity_kind = botster_core::EntityKind(mutation.entity_type().to_string());
        let owner_token = crate::lifecycle::package_entity_owner_token(package);
        botster_core::EntityContract::validate_entity_type(&entity_kind, Some(&owner_token))
            .map_err(|error| error.to_string())?;
        Ok(registration)
    }

    #[cfg(test)]
    pub(crate) fn for_test(package: &str, family: &str) -> Self {
        let registrations = crate::lifecycle::EntityProviderRegistrations::default();
        registrations.test_register(package, family);
        Self::new(registrations)
    }

    #[cfg(test)]
    pub(crate) fn test_queue_stale_publish(
        &self,
        plugin_key: PluginKey,
        frame: serde_json::Value,
        scope_id: Option<u64>,
    ) -> mpsc::Receiver<PublishResult> {
        let family = frame["entity_type"].as_str().unwrap();
        self.registrations.test_register(&plugin_key.0, family);
        let package = plugin_key.0.clone();
        let response = self.test_queue_publish(plugin_key, frame, scope_id);
        self.registrations.test_retire(&package);
        response
    }

    fn enqueue(
        &self,
        plugin_key: PluginKey,
        frame: serde_json::Value,
        scope_id: Option<u64>,
    ) -> Result<(u64, mpsc::Receiver<PublishResult>), EntityPublishError> {
        let fail = |message: &str| EntityPublishError::NeverQueued(message.into());
        if self.shared.reject_next.swap(false, Ordering::SeqCst) {
            return Err(fail("entity publish rejected before queue"));
        }
        let key_bytes = plugin_key
            .0
            .len()
            .checked_mul(2)
            .filter(|bytes| *bytes <= REQUEST_BYTE_LIMIT)
            .ok_or_else(|| fail("entity publish byte count exhausted"))?;
        let frame_bytes = crate::bounded_json::encoded_len(&frame, REQUEST_BYTE_LIMIT - key_bytes)
            .map_err(|_| {
                fail("entity_publish request exceeds frame limit (entity_provider_frame_too_large)")
            })?;
        let request_bytes = key_bytes + frame_bytes;
        // The Lua worker parses, validates, and destroys rejected frames here.
        let mut mutation =
            prepare_publish_mutation(frame).map_err(EntityPublishError::NeverQueued)?;
        let registration = self
            .prepare_registration(&plugin_key.0, &mutation)
            .map_err(EntityPublishError::NeverQueued)?;
        let notice = UnlockNotice {
            shared: &self.shared,
            acquired: Cell::new(false),
            changed: Cell::new(false),
        };
        if self.shared.faulted.load(Ordering::SeqCst) {
            return Err(fail("entity publish queue requires recovery"));
        }
        let mut queue = self
            .shared
            .try_guard(&notice)
            .map_err(|_| fail("entity publish queue unavailable"))?;
        if self.shared.faulted.load(Ordering::SeqCst) {
            return Err(fail("entity publish queue requires recovery"));
        }
        let bytes = self
            .shared
            .retained_bytes
            .load(Ordering::SeqCst)
            .checked_add(request_bytes)
            .ok_or_else(|| fail("entity publish byte count exhausted"))?;
        if self.shared.retained_count.load(Ordering::SeqCst) >= PUBLICATION_CAPACITY
            || bytes > PUBLICATION_BYTE_CAPACITY
        {
            return Err(fail("entity publish capacity exhausted"));
        }
        let token = queue.next_token;
        queue.next_token = token
            .checked_add(1)
            .ok_or_else(|| fail("entity publish token exhausted"))?;
        let (response, receiver) = mpsc::channel();
        let identity = crate::package_event_router::LeaseIdentity::PendingEntityPublish {
            publication_token: token,
        };
        self.shared.retained_count.fetch_add(1, Ordering::SeqCst);
        self.shared
            .retained_bytes
            .fetch_add(request_bytes, Ordering::SeqCst);
        mutation.set_admission(Some(EntityPublishPermit(Arc::new(PublicationCharge {
            account: Arc::downgrade(&self.shared),
            bytes: request_bytes,
        }))));
        queue.pending.push_back(PendingEntityPublishRequest {
            token,
            registration,
            mutation,
            scope_id,
            response,
            identity,
        });
        self.shared
            .count
            .store(queue.pending.len(), Ordering::SeqCst);
        notice.changed.set(true);
        Ok((token, receiver))
    }

    #[doc(hidden)]
    pub fn test_queue_publish(
        &self,
        plugin_key: PluginKey,
        frame: serde_json::Value,
        scope_id: Option<u64>,
    ) -> mpsc::Receiver<PublishResult> {
        match self.enqueue(plugin_key, frame, scope_id) {
            Ok((_, receiver)) => receiver,
            Err(EntityPublishError::NeverQueued(error)) => {
                let (sender, receiver) = mpsc::channel();
                let _ = sender.send(Err(error));
                receiver
            }
            _ => unreachable!(),
        }
    }

    pub(super) fn publish(
        &self,
        plugin_key: PluginKey,
        frame: serde_json::Value,
        scope_id: Option<u64>,
    ) -> Result<PackageEntityPublishResult, EntityPublishError> {
        if thread::current().id() == self.owner_thread {
            return Err(EntityPublishError::NeverQueued("botster.entity_publish is only available during handler invocation, not at plugin load".into()));
        }
        let (token, receiver) = self.enqueue(plugin_key, frame, scope_id)?;
        match receiver.recv_timeout(Duration::from_millis(
            super::ENTITY_PUBLISH_REQUEST_TIMEOUT_MS,
        )) {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(EntityPublishError::OwnerFinished(error)),
            Err(_) if self.try_retract(token) => Err(EntityPublishError::NeverQueued(
                "entity publish request did not complete before timeout".into(),
            )),
            Err(_) => Err(EntityPublishError::TimeoutInFlight),
        }
    }

    fn try_retract(&self, token: u64) -> bool {
        let removed;
        {
            let notice = UnlockNotice {
                shared: &self.shared,
                acquired: Cell::new(false),
                changed: Cell::new(false),
            };
            if self.shared.faulted.load(Ordering::SeqCst) {
                return false;
            }
            let Ok(mut queue) = self.shared.try_guard(&notice) else {
                return false;
            };
            if self.shared.faulted.load(Ordering::SeqCst) {
                return false;
            }
            let Some(index) = queue
                .pending
                .iter()
                .position(|request| request.token == token)
            else {
                return false;
            };
            removed = queue.pending.remove(index).unwrap();
            self.shared
                .count
                .store(queue.pending.len(), Ordering::SeqCst);
            notice.changed.set(true);
        }
        drop(removed);
        true
    }

    #[cfg(test)]
    pub(crate) fn test_retract(&self, token: u64) -> bool {
        self.try_retract(token)
    }

    /// Keep the exact head until the owner reserves and acquires its transition.
    pub(crate) fn take_if<T>(
        &self,
        prepare: impl FnOnce(&PendingEntityPublishRequest) -> Option<T>,
    ) -> Option<(PendingEntityPublishRequest, T)> {
        if !self.ready() {
            return None;
        }
        let notice = UnlockNotice {
            shared: &self.shared,
            acquired: Cell::new(false),
            changed: Cell::new(false),
        };
        let mut queue = match self.shared.try_guard(&notice) {
            Ok(guard) => guard,
            Err(()) => {
                if self.shared.faulted.load(Ordering::SeqCst) {
                    return None;
                }
                self.shared.blocked.store(true, Ordering::SeqCst);
                self.shared.interest.store(true, Ordering::SeqCst);
                match self.shared.try_guard(&notice) {
                    Ok(guard) => guard,
                    Err(()) => return None,
                }
            }
        };
        let prepared = prepare(queue.pending.front()?)?;
        let request = queue.pending.pop_front().unwrap();
        self.shared
            .count
            .store(queue.pending.len(), Ordering::SeqCst);
        Some((request, prepared))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publish_frame(bytes: usize) -> serde_json::Value {
        serde_json::json!({"type": "entity_patch", "entity_type": "p.item", "snapshot_seq": 1,
            "id": "item", "patch": {"body": "x".repeat(bytes)}})
    }

    #[test]
    fn provider_registration_refuses_invalid_ownership_before_queue_admission() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        let frame = publish_frame(0);
        assert!(matches!(
            bridge.enqueue(PluginKey("other".into()), frame, Some(42)),
            Err(EntityPublishError::NeverQueued(_))
        ));
        // A declared family must also satisfy the format and owner namespace contract.
        for family in ["other.item", "p:item"] {
            bridge.registrations.test_register("p", family);
            let frame = serde_json::json!({"type": "entity_remove", "entity_type": family,
                "snapshot_seq": 1, "id": "item"});
            assert!(matches!(
                bridge.enqueue(PluginKey("p".into()), frame, Some(42)),
                Err(EntityPublishError::NeverQueued(_))
            ));
        }
        assert_eq!(bridge.pending_publish_count(), 0);
        assert_eq!(bridge.retained_counts(), (0, 0));
        assert!(bridge.take_if(|_| Some(())).is_none());
    }

    #[test]
    fn lifetime_budget_keeps_unscoped_payloads_charged_after_queue_removal() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        let mut payloads = Vec::new();
        for _ in 0..PUBLICATION_CAPACITY {
            bridge
                .enqueue(PluginKey("p".into()), publish_frame(0), None)
                .ok()
                .unwrap();
            let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
            payloads.push(request.mutation);
        }
        assert_eq!(bridge.pending_publish_count(), 0);
        assert_eq!(bridge.retained_counts().0, PUBLICATION_CAPACITY);
        assert!(matches!(
            bridge.enqueue(PluginKey("p".into()), publish_frame(0), None),
            Err(EntityPublishError::NeverQueued(_))
        ));
        let payload = payloads.pop().unwrap();
        let copy = payload.clone();
        drop(payload);
        assert_eq!(bridge.retained_counts().0, PUBLICATION_CAPACITY);
        drop(copy);
        assert_eq!(bridge.retained_counts().0, PUBLICATION_CAPACITY - 1);
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        drop(payloads);
        let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
        drop(request);
        assert_eq!(bridge.retained_counts(), (0, 0));
    }

    #[test]
    fn lifetime_budget_keeps_original_bytes_and_returns_each_completed_charge() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        let mut payloads = Vec::new();
        for _ in 0..8 {
            let mut frame = publish_frame(0);
            frame["ignored"] = serde_json::json!("x".repeat(REQUEST_BYTE_LIMIT - 256));
            bridge
                .enqueue(PluginKey("p".into()), frame, None)
                .ok()
                .unwrap();
            let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
            payloads.push(request.mutation);
        }
        assert_eq!(bridge.pending_publish_count(), 0);
        assert_eq!(bridge.retained_counts().0, 8);
        assert!(matches!(
            bridge.enqueue(
                PluginKey("p".into()),
                publish_frame(REQUEST_BYTE_LIMIT - 256),
                None
            ),
            Err(EntityPublishError::NeverQueued(_))
        ));
        drop(payloads);
        assert_eq!(bridge.retained_counts(), (0, 0));
        for _ in 0..(PUBLICATION_CAPACITY * 2) {
            bridge
                .enqueue(PluginKey("p".into()), publish_frame(0), None)
                .ok()
                .unwrap();
            let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
            drop(request);
            assert_eq!(bridge.retained_counts(), (0, 0));
        }
    }

    #[test]
    fn lifetime_budget_has_no_cycle_when_the_bridge_is_destroyed() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        let account = Arc::downgrade(&bridge.shared);
        let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        drop(bridge);
        assert!(account.upgrade().is_none());
        drop(request);
    }

    #[test]
    fn queue_limits_and_exact_retraction_preserve_other_requests() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        let mut tokens = Vec::new();
        for _ in 0..PUBLICATION_CAPACITY {
            let (token, _) = bridge
                .enqueue(PluginKey("p".into()), publish_frame(0), None)
                .ok()
                .unwrap();
            tokens.push(token);
        }
        assert!(matches!(
            bridge.enqueue(PluginKey("p".into()), publish_frame(0), None),
            Err(EntityPublishError::NeverQueued(_))
        ));
        assert!(bridge.try_retract(tokens[17]));
        assert!(!bridge.try_retract(tokens[17]));
        assert_eq!(bridge.pending_publish_count(), PUBLICATION_CAPACITY - 1);
        let mut observed = Vec::new();
        while let Some((request, ())) = bridge.take_if(|_| Some(())) {
            observed.push(request.token);
        }
        tokens.remove(17);
        assert_eq!(observed, tokens);
        assert_eq!(bridge.shared.retained_bytes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn byte_limit_and_token_exhaustion_refuse_before_enqueue() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        assert!(matches!(
            bridge.enqueue(
                PluginKey("p".into()),
                serde_json::json!("x".repeat(REQUEST_BYTE_LIMIT)),
                None
            ),
            Err(EntityPublishError::NeverQueued(_))
        ));
        assert_eq!(bridge.pending_publish_count(), 0);
        bridge.shared.queue.try_lock().unwrap().next_token = u64::MAX;
        assert!(matches!(
            bridge.enqueue(PluginKey("p".into()), publish_frame(0), None),
            Err(EntityPublishError::NeverQueued(_))
        ));
        assert_eq!(bridge.pending_publish_count(), 0);
    }

    #[test]
    fn queue_bytes_bound_admission_below_the_count_limit() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        for _ in 0..8 {
            bridge
                .enqueue(
                    PluginKey("p".into()),
                    publish_frame(REQUEST_BYTE_LIMIT - 256),
                    None,
                )
                .ok()
                .unwrap();
        }
        assert_eq!(bridge.pending_publish_count(), 8);
        assert!(matches!(
            bridge.enqueue(
                PluginKey("p".into()),
                publish_frame(REQUEST_BYTE_LIMIT - 256),
                None
            ),
            Err(EntityPublishError::NeverQueued(_))
        ));
    }

    #[test]
    fn queued_progress_survives_binding_and_a_full_doorbell() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::CausalProgressPublished)
            .unwrap();
        bridge.bind_owner_wake(sender.clone());
        assert!(bridge.take_progress_notification());
        assert!(bridge.ready());
        receiver.try_recv().unwrap();
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::EntityPublishProgress)
        ));
        assert!(bridge.take_progress_notification());
        assert!(!bridge.take_progress_notification());
    }

    #[test]
    fn bridge_unlock_wakes_a_retained_head_and_fault_keeps_it() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        bridge.take_progress_notification();
        {
            let notice = UnlockNotice {
                shared: &bridge.shared,
                acquired: Cell::new(false),
                changed: Cell::new(false),
            };
            let _guard = bridge.shared.try_guard(&notice).ok().unwrap();
            assert!(bridge.take_if(|_| Some(())).is_none());
            assert!(!bridge.ready());
            assert!(!bridge.take_progress_notification());
        }
        assert!(bridge.ready());
        assert!(bridge.take_progress_notification());
        assert_eq!(bridge.pending_publish_count(), 1);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let notice = UnlockNotice {
                shared: &bridge.shared,
                acquired: Cell::new(false),
                changed: Cell::new(false),
            };
            let _guard = bridge.shared.try_guard(&notice).ok().unwrap();
            panic!("inject bridge fault");
        }));
        assert!(bridge.take_progress_notification());
        assert!(!bridge.ready());
        assert!(bridge.take_if(|_| Some(())).is_none());
        assert!(!bridge.try_retract(1));
        assert_eq!(bridge.pending_publish_count(), 1);
    }

    #[test]
    fn fault_latch_preserves_the_head_against_timeout_retraction() {
        let bridge = HubEntityPublishBridge::for_test("p", "p.item");
        let (before, _) = bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        assert!(
            bridge.try_retract(before),
            "an unacquired request can retract before fault retention"
        );
        let (retained, _) = bridge
            .enqueue(PluginKey("p".into()), publish_frame(0), None)
            .ok()
            .unwrap();
        assert!(
            bridge
                .take_if(|_| {
                    bridge.retain_faulted();
                    None::<()>
                })
                .is_none()
        );
        assert!(!bridge.try_retract(retained));
        assert_eq!(bridge.pending_publish_count(), 1);
        assert!(matches!(
            bridge.enqueue(PluginKey("p".into()), publish_frame(0), None),
            Err(EntityPublishError::NeverQueued(_))
        ));
        assert_eq!(bridge.pending_publish_count(), 1);
    }
}
