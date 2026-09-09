//! Bounded publication requests shared by Lua workers and the runtime owner.

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError, mpsc};
use std::thread;
use std::time::Duration;

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::package_entity_fanout::{
    PackageEntityMutation, PackageEntityPublishResult, prepare_publish_mutation,
};
use botster_core::PluginKey;

const REQUEST_CAPACITY: usize = 256;
const REQUEST_BYTE_LIMIT: usize = 1024 * 1024;
const QUEUE_BYTE_CAPACITY: usize = 8 * 1024 * 1024;
type PublishResult = Result<PackageEntityPublishResult, String>;

#[derive(Clone)]
pub struct HubEntityPublishBridge {
    owner_thread: thread::ThreadId,
    shared: Arc<Shared>,
}

struct Shared {
    queue: Mutex<Queue>,
    count: AtomicUsize,
    blocked: AtomicBool,
    interest: AtomicBool,
    faulted: AtomicBool,
    progress: AtomicBool,
    owner: OnceLock<ControlSender>,
    reject_next: AtomicBool,
}

struct Queue {
    pending: VecDeque<PendingEntityPublishRequest>,
    bytes: usize,
    next_token: u64,
}

pub(crate) struct PendingEntityPublishRequest {
    pub(crate) token: u64,
    pub(crate) plugin_key: PluginKey,
    pub(crate) mutation: PackageEntityMutation,
    pub(crate) scope_id: Option<u64>,
    pub(crate) response: mpsc::Sender<PublishResult>,
    bytes: usize,
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
    pub(crate) fn new() -> Self {
        Self {
            owner_thread: thread::current().id(),
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue {
                    pending: VecDeque::new(),
                    bytes: 0,
                    next_token: 1,
                }),
                count: AtomicUsize::new(0),
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

    /// Call this only from the preparation callback while the bridge guard is held.
    pub(crate) fn retain_faulted(&self) {
        if !self.shared.faulted.swap(true, Ordering::SeqCst) {
            self.shared.notify();
        }
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
        let mutation = prepare_publish_mutation(frame).map_err(EntityPublishError::NeverQueued)?;
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
        let bytes = queue
            .bytes
            .checked_add(request_bytes)
            .ok_or_else(|| fail("entity publish byte count exhausted"))?;
        if queue.pending.len() >= REQUEST_CAPACITY || bytes > QUEUE_BYTE_CAPACITY {
            return Err(fail("entity publish queue capacity exhausted"));
        }
        let token = queue.next_token;
        queue.next_token = token
            .checked_add(1)
            .ok_or_else(|| fail("entity publish token exhausted"))?;
        let (response, receiver) = mpsc::channel();
        let identity = crate::package_event_router::LeaseIdentity::PendingEntityPublish {
            plugin_key: plugin_key.0.clone(),
            publication_token: token,
        };
        queue.pending.push_back(PendingEntityPublishRequest {
            token,
            plugin_key,
            mutation,
            scope_id,
            response,
            bytes: request_bytes,
            identity,
        });
        queue.bytes = bytes;
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
            queue.bytes -= removed.bytes;
            self.shared
                .count
                .store(queue.pending.len(), Ordering::SeqCst);
            notice.changed.set(true);
        }
        drop(removed);
        true
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
        queue.bytes -= request.bytes;
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
        serde_json::json!({"type": "entity_patch", "entity_type": "p:items", "snapshot_seq": 1,
            "id": "item", "patch": {"body": "x".repeat(bytes)}})
    }

    #[test]
    fn queue_limits_and_exact_retraction_preserve_other_requests() {
        let bridge = HubEntityPublishBridge::new();
        let mut tokens = Vec::new();
        for _ in 0..REQUEST_CAPACITY {
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
        assert_eq!(bridge.pending_publish_count(), REQUEST_CAPACITY - 1);
        let mut observed = Vec::new();
        while let Some((request, ())) = bridge.take_if(|_| Some(())) {
            observed.push(request.token);
        }
        tokens.remove(17);
        assert_eq!(observed, tokens);
        assert_eq!(bridge.shared.queue.try_lock().unwrap().bytes, 0);
    }

    #[test]
    fn byte_limit_and_token_exhaustion_refuse_before_enqueue() {
        let bridge = HubEntityPublishBridge::new();
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
        let bridge = HubEntityPublishBridge::new();
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
        let bridge = HubEntityPublishBridge::new();
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
        let bridge = HubEntityPublishBridge::new();
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
        let bridge = HubEntityPublishBridge::new();
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
