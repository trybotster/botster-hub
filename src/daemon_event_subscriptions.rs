//! Connection-scoped package-event subscriptions on the host control plane.
//!
//! This module owns subject admission, per-connection mailboxes, gap bits, and
//! the coalesced writer wake. It does not take over a Unix socket and does not
//! put frames on [`EntityFrameSender`].

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Instant;

use botster_hub_client::{
    DaemonDiagnostic, DaemonEvent, DaemonOperatorError, DaemonResponse, DaemonResponseKind,
};
use serde_json::Value;
use tokio::sync::Notify;

use crate::config::PackageEventPlanePolicy;
use crate::daemon_transport::daemon_response_base;
use crate::package_event_router::{ClientEventHolder, EventPlaneStatus, PackageEventRouter};

pub const MAX_SUBJECTS_PER_SUBSCRIPTION: usize = 16;
pub const MAX_SUBJECT_UTF8_BYTES: usize = 256;
pub const MAX_SUBJECT_AGGREGATE_BYTES: usize = 4_096;
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 64;

const GLOB_CHARS: [char; 6] = ['*', '?', '[', ']', '{', '}'];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientEventAdmitError {
    EmptySubject,
    WildcardSubject,
    DuplicateSubject,
    TooManySubjects,
    SubjectTooLong,
    AggregateTooLarge,
    TooManySubscriptions,
    DuplicateSubscription,
    UnknownSubscription,
    NotNegotiated,
    Router(EventPlaneStatus),
}

impl ClientEventAdmitError {
    #[must_use]
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::EmptySubject => "empty_subject",
            Self::WildcardSubject => "rejected_wildcard",
            Self::DuplicateSubject => "duplicate_subject",
            Self::TooManySubjects => "too_many_subjects",
            Self::SubjectTooLong => "subject_too_long",
            Self::AggregateTooLarge => "subject_aggregate_too_large",
            Self::TooManySubscriptions => "too_many_event_subscriptions",
            Self::DuplicateSubscription => "duplicate_event_subscription",
            Self::UnknownSubscription => "unknown_event_subscription",
            Self::NotNegotiated => "package_event_subscriptions_not_negotiated",
            Self::Router(status) => status.as_str(),
        }
    }

    #[must_use]
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::EmptySubject => "subject values must not be empty",
            Self::WildcardSubject => "subject values must be exact strings",
            Self::DuplicateSubject => "subject values must be unique",
            Self::TooManySubjects => "a subscription admits at most 16 subject values",
            Self::SubjectTooLong => "each subject value admits at most 256 UTF-8 bytes",
            Self::AggregateTooLarge => "subject values admit at most 4096 UTF-8 bytes together",
            Self::TooManySubscriptions => {
                "a host-control connection admits at most 64 package-event subscriptions"
            }
            Self::DuplicateSubscription => {
                "this connection already holds that event subscription id"
            }
            Self::UnknownSubscription => "no event subscription exists on this connection",
            Self::NotNegotiated => {
                "package_event_subscriptions was not negotiated on this host Hello"
            }
            Self::Router(status) => match status {
                EventPlaneStatus::RejectedUndeclared => "event is not an admitted contract",
                EventPlaneStatus::RejectedForeign => "event owner does not match the caller",
                EventPlaneStatus::RejectedWildcard => "event owner and name must be exact",
                EventPlaneStatus::RejectedAudience => "event contract does not admit clients",
                EventPlaneStatus::ShedBusy => "event router is busy",
                EventPlaneStatus::RejectedInvalid => "event owner or name is invalid",
                EventPlaneStatus::RejectedOverFanout => "event already has too many subscribers",
                _ => "event subscription was rejected",
            },
        }
    }
}

pub(crate) fn client_event_operator_error(
    error: ClientEventAdmitError,
    request_id: &str,
    operation: &str,
) -> DaemonResponse {
    let mut response = daemon_response_base(DaemonResponseKind::OperatorError);
    response.error = Some(DaemonOperatorError {
        code: error.code().to_string(),
        request_id: request_id.to_string(),
        operation: operation.to_string(),
        message: error.message().to_string(),
        diagnostics: vec![DaemonDiagnostic::action_failure(operation, error.message())],
    });
    response
}

#[derive(Debug, Clone)]
struct QueuedClientEvent {
    subscription_id: String,
    owner: String,
    name: String,
    payload: Value,
    enqueued_at: Instant,
    size: usize,
}

struct MailboxInner {
    events: VecDeque<QueuedClientEvent>,
    bytes: usize,
    gap_bits: HashMap<String, (String, String)>,
}

/// Bounded per-connection mailbox plus per-subscription gap bits.
pub(crate) struct ClientEventMailbox {
    inner: Mutex<MailboxInner>,
    wake: Notify,
    wake_bit: AtomicBool,
    event_max: usize,
    byte_max: usize,
    queue_age: std::time::Duration,
}

impl ClientEventMailbox {
    pub(crate) fn new(policy: PackageEventPlanePolicy) -> Self {
        Self {
            inner: Mutex::new(MailboxInner {
                events: VecDeque::new(),
                bytes: 0,
                gap_bits: HashMap::new(),
            }),
            wake: Notify::new(),
            wake_bit: AtomicBool::new(false),
            event_max: policy.consumer_queue_max_events,
            byte_max: policy.consumer_queue_max_bytes,
            queue_age: policy.queue_age,
        }
    }

    #[must_use]
    pub(crate) fn notify(&self) -> &Notify {
        &self.wake
    }

    fn signal_wake(&self) {
        self.wake_bit.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    #[must_use]
    pub(crate) fn take_wake(&self) -> bool {
        self.wake_bit.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn try_push(
        &self,
        subscription_id: &str,
        owner: &str,
        name: &str,
        payload: Value,
        size: usize,
    ) -> Result<(), EventPlaneStatus> {
        let mut inner = lock_mailbox(&self.inner)?;
        if inner.events.len() + 1 > self.event_max || inner.bytes + size > self.byte_max {
            inner.gap_bits.insert(
                subscription_id.to_string(),
                (owner.to_string(), name.to_string()),
            );
            drop(inner);
            self.signal_wake();
            return Err(EventPlaneStatus::ShedFull);
        }
        inner.bytes += size;
        inner.events.push_back(QueuedClientEvent {
            subscription_id: subscription_id.to_string(),
            owner: owner.to_string(),
            name: name.to_string(),
            payload,
            enqueued_at: Instant::now(),
            size,
        });
        drop(inner);
        self.signal_wake();
        Ok(())
    }

    pub(crate) fn set_gap(&self, subscription_id: &str, owner: &str, name: &str) {
        if let Ok(mut inner) = lock_mailbox(&self.inner) {
            inner.gap_bits.insert(
                subscription_id.to_string(),
                (owner.to_string(), name.to_string()),
            );
        }
        self.signal_wake();
    }

    #[must_use]
    pub(crate) fn has_ready_event(&self) -> bool {
        let Ok(inner) = lock_mailbox(&self.inner) else {
            return false;
        };
        !inner.gap_bits.is_empty() || !inner.events.is_empty()
    }

    pub(crate) fn take_ready_event(&self) -> Option<DaemonEvent> {
        let mut inner = lock_mailbox(&self.inner).ok()?;
        if let Some((subscription_id, (owner, name))) = inner
            .gap_bits
            .iter()
            .next()
            .map(|(subscription_id, identity)| (subscription_id.clone(), identity.clone()))
        {
            inner.gap_bits.remove(&subscription_id);
            return Some(DaemonEvent::EventGap {
                subscription_id,
                owner,
                name,
            });
        }
        loop {
            let queued = inner.events.pop_front()?;
            inner.bytes = inner.bytes.saturating_sub(queued.size);
            if queued.enqueued_at.elapsed() > self.queue_age {
                inner
                    .gap_bits
                    .insert(queued.subscription_id, (queued.owner, queued.name));
                if let Some((subscription_id, (owner, name))) = inner
                    .gap_bits
                    .iter()
                    .next()
                    .map(|(subscription_id, identity)| (subscription_id.clone(), identity.clone()))
                {
                    inner.gap_bits.remove(&subscription_id);
                    return Some(DaemonEvent::EventGap {
                        subscription_id,
                        owner,
                        name,
                    });
                }
                continue;
            }
            return Some(DaemonEvent::PackageEvent {
                subscription_id: queued.subscription_id,
                owner: queued.owner,
                name: queued.name,
                payload: queued.payload,
            });
        }
    }

    fn drop_subscription(&self, subscription_id: &str) {
        if let Ok(mut inner) = lock_mailbox(&self.inner) {
            inner.gap_bits.remove(subscription_id);
            let mut bytes = inner.bytes;
            inner.events.retain(|queued| {
                if queued.subscription_id == subscription_id {
                    bytes = bytes.saturating_sub(queued.size);
                    false
                } else {
                    true
                }
            });
            inner.bytes = bytes;
        }
    }
}

struct ConnectionEventState {
    mailbox: Arc<ClientEventMailbox>,
    subscriptions: HashMap<String, (String, String)>,
}

/// Per-connection host-control event subscription table.
#[derive(Default)]
pub(crate) struct ClientEventPlane {
    connections: Mutex<HashMap<String, ConnectionEventState>>,
    pending_cleanup: Mutex<HashSet<String>>,
}

impl std::fmt::Debug for ClientEventPlane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientEventPlane")
            .finish_non_exhaustive()
    }
}

impl ClientEventPlane {
    #[must_use]
    pub(crate) fn mailbox(&self, connection_id: &str) -> Option<Arc<ClientEventMailbox>> {
        let connections = lock_plane(&self.connections).ok()?;
        connections
            .get(connection_id)
            .map(|state| state.mailbox.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_subscribe(
        &self,
        connection_id: &str,
        subscription_id: &str,
        owner: &str,
        name: &str,
        subjects: Vec<String>,
        policy: PackageEventPlanePolicy,
        router: &PackageEventRouter,
    ) -> Result<(), ClientEventAdmitError> {
        let compiled = compile_subjects(&subjects)?;
        let mut connections =
            lock_plane(&self.connections).map_err(ClientEventAdmitError::Router)?;
        let state = connections
            .entry(connection_id.to_string())
            .or_insert_with(|| ConnectionEventState {
                mailbox: Arc::new(ClientEventMailbox::new(policy)),
                subscriptions: HashMap::new(),
            });
        if state.subscriptions.contains_key(subscription_id) {
            return Err(ClientEventAdmitError::DuplicateSubscription);
        }
        if state.subscriptions.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION {
            return Err(ClientEventAdmitError::TooManySubscriptions);
        }
        let mailbox = state.mailbox.clone();
        let status = router.try_subscribe_client(ClientEventHolder {
            connection_id: connection_id.to_string(),
            subscription_id: subscription_id.to_string(),
            owner: owner.to_string(),
            name: name.to_string(),
            subjects: compiled,
            mailbox,
        });
        if status != EventPlaneStatus::Accepted {
            return Err(ClientEventAdmitError::Router(status));
        }
        state.subscriptions.insert(
            subscription_id.to_string(),
            (owner.to_string(), name.to_string()),
        );
        Ok(())
    }

    pub(crate) fn try_unsubscribe(
        &self,
        connection_id: &str,
        subscription_id: &str,
        router: &PackageEventRouter,
    ) -> Result<(), ClientEventAdmitError> {
        let mut connections =
            lock_plane(&self.connections).map_err(ClientEventAdmitError::Router)?;
        let Some(state) = connections.get_mut(connection_id) else {
            return Err(ClientEventAdmitError::UnknownSubscription);
        };
        if !state.subscriptions.contains_key(subscription_id) {
            return Err(ClientEventAdmitError::UnknownSubscription);
        }
        let status = router.try_unsubscribe_client(connection_id, subscription_id);
        if status != EventPlaneStatus::Accepted {
            return Err(ClientEventAdmitError::Router(status));
        }
        state.subscriptions.remove(subscription_id);
        state.mailbox.drop_subscription(subscription_id);
        if state.subscriptions.is_empty() {
            connections.remove(connection_id);
        }
        Ok(())
    }

    pub(crate) fn cleanup_connection(&self, connection_id: &str, router: &PackageEventRouter) {
        if let Ok(mut pending) = self.pending_cleanup.lock() {
            pending.insert(connection_id.to_string());
        }
        let _ = self.apply_pending_cleanups(router);
    }

    #[must_use]
    pub(crate) fn has_pending_cleanup(&self) -> bool {
        self.pending_cleanup
            .lock()
            .map(|pending| !pending.is_empty())
            .unwrap_or(true)
    }

    /// Retry no-wait disconnect cleanup until router removal returns Accepted.
    #[must_use]
    pub(crate) fn apply_pending_cleanups(&self, router: &PackageEventRouter) -> bool {
        let ids = match self.pending_cleanup.lock() {
            Ok(pending) => pending.iter().cloned().collect::<Vec<_>>(),
            Err(_) => return true,
        };
        let mut remaining = Vec::new();
        for connection_id in ids {
            let Ok(mut connections) = lock_plane(&self.connections) else {
                remaining.push(connection_id);
                continue;
            };
            match router.try_cleanup_client_connection(&connection_id) {
                EventPlaneStatus::Accepted => {
                    connections.remove(&connection_id);
                }
                _ => remaining.push(connection_id),
            }
        }
        match self.pending_cleanup.lock() {
            Ok(mut pending) => {
                pending.retain(|connection_id| remaining.iter().any(|id| id == connection_id));
                for connection_id in remaining {
                    pending.insert(connection_id);
                }
                !pending.is_empty()
            }
            Err(_) => true,
        }
    }
}

pub(crate) fn compile_subjects(
    subjects: &[String],
) -> Result<BTreeSet<String>, ClientEventAdmitError> {
    if subjects.len() > MAX_SUBJECTS_PER_SUBSCRIPTION {
        return Err(ClientEventAdmitError::TooManySubjects);
    }
    let mut compiled = BTreeSet::new();
    let mut aggregate = 0usize;
    for subject in subjects {
        if subject.is_empty() {
            return Err(ClientEventAdmitError::EmptySubject);
        }
        if subject.chars().any(|ch| GLOB_CHARS.contains(&ch)) {
            return Err(ClientEventAdmitError::WildcardSubject);
        }
        let bytes = subject.len();
        if bytes > MAX_SUBJECT_UTF8_BYTES {
            return Err(ClientEventAdmitError::SubjectTooLong);
        }
        aggregate = aggregate.saturating_add(bytes);
        if aggregate > MAX_SUBJECT_AGGREGATE_BYTES {
            return Err(ClientEventAdmitError::AggregateTooLarge);
        }
        if !compiled.insert(subject.clone()) {
            return Err(ClientEventAdmitError::DuplicateSubject);
        }
    }
    Ok(compiled)
}

fn lock_plane(
    mutex: &Mutex<HashMap<String, ConnectionEventState>>,
) -> Result<std::sync::MutexGuard<'_, HashMap<String, ConnectionEventState>>, EventPlaneStatus> {
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
            Err(EventPlaneStatus::ShedBusy)
        }
    }
}

fn lock_mailbox(
    mutex: &Mutex<MailboxInner>,
) -> Result<std::sync::MutexGuard<'_, MailboxInner>, EventPlaneStatus> {
    match mutex.try_lock() {
        Ok(guard) => Ok(guard),
        Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
            Err(EventPlaneStatus::ShedBusy)
        }
    }
}

pub(crate) fn subscribe_events_response() -> DaemonResponse {
    daemon_response_base(DaemonResponseKind::EventSubscribed)
}

pub(crate) fn unsubscribe_events_response() -> DaemonResponse {
    daemon_response_base(DaemonResponseKind::EventUnsubscribed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PackageEventPlanePolicy;
    use crate::package_event_router::{EmittedContract, EventAudience};
    use crate::package_event_schema::CompiledEventSchema;
    use serde_json::json;
    use std::collections::BTreeSet;

    fn admitted_router(audience: EventAudience) -> PackageEventRouter {
        let router = PackageEventRouter::new(PackageEventPlanePolicy::default());
        router
            .try_register_contracts(vec![EmittedContract {
                owner: "owner".into(),
                name: "ready".into(),
                audience: BTreeSet::from([audience]),
                schema: CompiledEventSchema::compile(&json!({
                    "type": "object",
                    "additionalProperties": true
                }))
                .expect("schema"),
                package_generation: 1,
            }])
            .expect("register");
        router
    }

    #[test]
    fn subject_boundaries_use_utf8_bytes_and_exact_sets() {
        let sixteen: Vec<String> = (0..16).map(|index| format!("s{index}")).collect();
        assert!(compile_subjects(&sixteen).is_ok());
        let mut seventeen = sixteen.clone();
        seventeen.push("s16".into());
        assert_eq!(
            compile_subjects(&seventeen),
            Err(ClientEventAdmitError::TooManySubjects)
        );

        let exact_256 = "a".repeat(256);
        assert!(compile_subjects(std::slice::from_ref(&exact_256)).is_ok());
        assert_eq!(
            compile_subjects(&["a".repeat(257)]),
            Err(ClientEventAdmitError::SubjectTooLong)
        );

        let multi_byte = "é".repeat(128);
        assert_eq!(multi_byte.len(), 256);
        assert!(compile_subjects(&[multi_byte]).is_ok());
        assert_eq!(
            compile_subjects(&["é".repeat(129)]),
            Err(ClientEventAdmitError::SubjectTooLong)
        );

        let aggregate: Vec<String> = (0..16)
            .map(|index| format!("{index:02}{}", "a".repeat(254)))
            .collect();
        assert_eq!(aggregate.iter().map(String::len).sum::<usize>(), 4_096);
        assert!(compile_subjects(&aggregate).is_ok());
        let just_under: Vec<String> = {
            let mut values: Vec<String> = (0..15)
                .map(|index| format!("{index:02}{}", "a".repeat(254)))
                .collect();
            values.push(format!("zz{}", "a".repeat(253)));
            values
        };
        assert!(just_under.iter().map(String::len).sum::<usize>() < 4_096);
        assert!(compile_subjects(&just_under).is_ok());

        assert_eq!(
            compile_subjects(&[String::new()]),
            Err(ClientEventAdmitError::EmptySubject)
        );
        assert_eq!(
            compile_subjects(&["foo*".into()]),
            Err(ClientEventAdmitError::WildcardSubject)
        );
        assert_eq!(
            compile_subjects(&["a".into(), "a".into()]),
            Err(ClientEventAdmitError::DuplicateSubject)
        );
    }

    #[test]
    fn connection_scope_keeps_holders_independent() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe(
                "conn-a",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("a");
        plane
            .try_subscribe(
                "conn-b",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router,
            )
            .expect("b");
        assert_eq!(
            plane.try_subscribe(
                "conn-a",
                "same",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router
            ),
            Err(ClientEventAdmitError::DuplicateSubscription)
        );
        assert_eq!(
            plane.try_unsubscribe("conn-b", "missing", &router),
            Err(ClientEventAdmitError::UnknownSubscription)
        );
        plane
            .try_unsubscribe("conn-b", "same", &router)
            .expect("b unsub");
        assert!(plane.mailbox("conn-a").is_some());
        assert!(plane.mailbox("conn-b").is_none());
        plane.cleanup_connection("conn-a", &router);
        assert!(plane.mailbox("conn-a").is_none());
    }

    #[test]
    fn disconnect_cleanup_keeps_holder_until_router_accepts() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        plane
            .try_subscribe("conn", "sub", "owner", "ready", Vec::new(), policy, &router)
            .expect("subscribe");
        assert_eq!(router.test_client_holder_count("conn"), 1);
        router.test_with_inner_held(|| {
            plane.cleanup_connection("conn", &router);
        });
        assert!(
            plane.has_pending_cleanup(),
            "shed_busy must leave cleanup ownership on the ledger"
        );
        assert!(
            plane.mailbox("conn").is_some(),
            "plane state stays until router removal is accepted"
        );
        assert_eq!(router.test_client_holder_count("conn"), 1);
        assert!(
            !plane.apply_pending_cleanups(&router),
            "retry must finish after the router lock is free"
        );
        assert_eq!(router.test_client_holder_count("conn"), 0);
        assert!(plane.mailbox("conn").is_none());
        assert!(!plane.has_pending_cleanup());
    }

    #[test]
    fn missed_notify_is_recovered_by_wake_bit() {
        let mailbox = ClientEventMailbox::new(PackageEventPlanePolicy::default());
        assert!(!mailbox.take_wake());
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("push");
        assert!(mailbox.take_wake());
        assert!(!mailbox.take_wake());
        assert!(mailbox.has_ready_event());
    }

    #[test]
    fn sixty_fifth_subscription_is_rejected() {
        let router = admitted_router(EventAudience::Clients);
        let plane = ClientEventPlane::default();
        let policy = PackageEventPlanePolicy::default();
        for index in 0..MAX_SUBSCRIPTIONS_PER_CONNECTION {
            plane
                .try_subscribe(
                    "conn",
                    &format!("sub-{index}"),
                    "owner",
                    "ready",
                    Vec::new(),
                    policy,
                    &router,
                )
                .expect("subscribe under cap");
        }
        assert_eq!(
            plane.try_subscribe(
                "conn",
                "sub-64",
                "owner",
                "ready",
                Vec::new(),
                policy,
                &router
            ),
            Err(ClientEventAdmitError::TooManySubscriptions)
        );
    }

    #[test]
    fn mailbox_gap_is_outside_the_queue_and_writes_first() {
        let policy = PackageEventPlanePolicy {
            consumer_queue_max_events: 1,
            ..PackageEventPlanePolicy::default()
        };
        let mailbox = ClientEventMailbox::new(policy);
        mailbox
            .try_push("sub", "owner", "ready", json!({ "ok": true }), 8)
            .expect("first event");
        assert_eq!(
            mailbox.try_push("sub", "owner", "ready", json!({ "ok": true }), 8),
            Err(EventPlaneStatus::ShedFull)
        );
        match mailbox.take_ready_event() {
            Some(DaemonEvent::EventGap {
                subscription_id, ..
            }) => {
                assert_eq!(subscription_id, "sub");
            }
            other => panic!("gap must come first: {other:?}"),
        }
        match mailbox.take_ready_event() {
            Some(DaemonEvent::PackageEvent { .. }) => {}
            other => panic!("queued event remains after gap: {other:?}"),
        }
    }
}
