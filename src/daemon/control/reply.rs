//! Bounded ownership for plugin results that leave Core's completion queue.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::daemon::control::message::{ControlMessage, ControlSender};
use crate::daemon::error::DaemonTransportResult;
use botster_hub_client::DaemonResponse;

/// Global logical bytes retained after Hub drains plugin completions from Core.
/// This limit is separate from each connection's framed-response byte limit.
pub(crate) const RETAINED_PLUGIN_RESULT_BYTE_CAPACITY: usize = 8 * 1024 * 1024;

struct RetainedPluginResultBudgetInner {
    retained_bytes: AtomicUsize,
    release_pending: AtomicBool,
    completion_pending: AtomicBool,
    wake: Mutex<Option<ControlSender>>,
}

/// One global byte budget shared by pending plugin rows and transport replies.
#[derive(Clone)]
pub(crate) struct RetainedPluginResultBudget {
    inner: Arc<RetainedPluginResultBudgetInner>,
}

impl std::fmt::Debug for RetainedPluginResultBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedPluginResultBudget")
            .field("capacity", &RETAINED_PLUGIN_RESULT_BYTE_CAPACITY)
            .field("retained_bytes", &self.retained_bytes())
            .finish_non_exhaustive()
    }
}

impl Default for RetainedPluginResultBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl RetainedPluginResultBudget {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RetainedPluginResultBudgetInner {
                retained_bytes: AtomicUsize::new(0),
                release_pending: AtomicBool::new(false),
                completion_pending: AtomicBool::new(false),
                wake: Mutex::new(None),
            }),
        }
    }

    /// Bind the owner queue used to wake completion draining after a release.
    pub(crate) fn bind_owner_wake(&self, sender: ControlSender) {
        *self.inner.wake.lock().expect("plugin result wake mutex") = Some(sender);
    }

    /// Logical bytes that another bounded Core drain may transfer to Hub.
    pub(crate) fn available_bytes(&self) -> usize {
        RETAINED_PLUGIN_RESULT_BYTE_CAPACITY.saturating_sub(self.retained_bytes())
    }

    #[must_use]
    pub(crate) fn try_reserve(&self, encoded_len: usize) -> Option<RetainedPluginResultCharge> {
        if encoded_len > RETAINED_PLUGIN_RESULT_BYTE_CAPACITY {
            return None;
        }
        let mut retained = self.inner.retained_bytes.load(Ordering::Acquire);
        loop {
            let updated = retained.checked_add(encoded_len)?;
            if updated > RETAINED_PLUGIN_RESULT_BYTE_CAPACITY {
                return None;
            }
            match self.inner.retained_bytes.compare_exchange_weak(
                retained,
                updated,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(RetainedPluginResultCharge {
                        budget: self.clone(),
                        encoded_len,
                    });
                }
                Err(observed) => retained = observed,
            }
        }
    }

    /// Consume the coalesced release notification before another drain attempt.
    pub(crate) fn take_release_notification(&self) -> bool {
        self.inner.release_pending.swap(false, Ordering::AcqRel)
    }

    /// Return Core's callback for a newly published plugin completion.
    pub(crate) fn completion_notifier(&self) -> botster_core::PluginCompletionNotifier {
        let budget = self.clone();
        Arc::new(move || budget.publish_completion_notification())
    }

    fn publish_completion_notification(&self) {
        // Publish the bit before the best-effort send. A full queue already
        // contains a message that will wake the owner, which then checks it.
        self.inner.completion_pending.store(true, Ordering::Release);
        if let Some(sender) = self
            .inner
            .wake
            .lock()
            .expect("plugin result wake mutex")
            .as_ref()
        {
            let _ = sender.try_send(ControlMessage::PluginCompletionPublished);
        }
    }

    /// Consume the coalesced Core publication notification.
    pub(crate) fn take_completion_notification(&self) -> bool {
        self.inner.completion_pending.swap(false, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes.load(Ordering::Acquire)
    }

    #[cfg(not(test))]
    fn retained_bytes(&self) -> usize {
        self.inner.retained_bytes.load(Ordering::Acquire)
    }
}

/// RAII means that Rust releases the byte charge when this value is dropped.
/// The charge moves with the raw result and its transport reply.
pub(crate) struct RetainedPluginResultCharge {
    budget: RetainedPluginResultBudget,
    encoded_len: usize,
}

impl std::fmt::Debug for RetainedPluginResultCharge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedPluginResultCharge")
            .field("encoded_len", &self.encoded_len)
            .finish_non_exhaustive()
    }
}

impl Drop for RetainedPluginResultCharge {
    fn drop(&mut self) {
        self.budget
            .inner
            .retained_bytes
            .fetch_sub(self.encoded_len, Ordering::AcqRel);
        // Publish the bit before the best-effort send. A full queue already
        // contains a message that will wake the owner, which then checks it.
        self.budget
            .inner
            .release_pending
            .store(true, Ordering::Release);
        if let Some(sender) = self
            .budget
            .inner
            .wake
            .lock()
            .expect("plugin result wake mutex")
            .as_ref()
        {
            let _ = sender.try_send(ControlMessage::PluginResultCapacityReleased);
        }
    }
}

/// An owned plugin value and its logical-byte charge.
pub(crate) struct RetainedPluginResult<T> {
    value: T,
    charge: RetainedPluginResultCharge,
}

/// One control response whose charge remains until its owned values drop.
#[derive(Debug)]
pub(crate) enum ControlReply {
    Typed {
        response: DaemonTransportResult<DaemonResponse>,
        charge: Option<RetainedControlCharge>,
        delivery: Option<crate::runtime::SpawnDeliveryReceipt>,
    },
    EncodedPlugin {
        kind: botster_hub_client::DaemonResponseKind,
        encoded_frame: Vec<u8>,
        charge: crate::host_executor::HostPreparedCharge,
    },
}

#[derive(Debug)]
pub(crate) enum RetainedControlCharge {
    Plugin(RetainedPluginResultCharge),
    Host(crate::host_executor::HostPreparedCharge),
}

impl ControlReply {
    pub(crate) fn plain(response: DaemonTransportResult<DaemonResponse>) -> Self {
        Self::Typed {
            response,
            charge: None,
            delivery: None,
        }
    }

    pub(crate) fn retained(
        response: RetainedPluginResult<DaemonTransportResult<DaemonResponse>>,
    ) -> Self {
        let (response, charge) = response.into_parts();
        Self::Typed {
            response,
            charge: Some(RetainedControlCharge::Plugin(charge)),
            delivery: None,
        }
    }

    pub(crate) fn host(
        response: DaemonTransportResult<DaemonResponse>,
        charge: crate::host_executor::HostPreparedCharge,
    ) -> Self {
        Self::Typed {
            response,
            charge: Some(RetainedControlCharge::Host(charge)),
            delivery: None,
        }
    }

    pub(crate) fn spawn_delivery(
        response: DaemonTransportResult<DaemonResponse>,
        delivery: crate::runtime::SpawnDeliveryReceipt,
    ) -> Self {
        Self::Typed {
            response,
            charge: None,
            delivery: Some(delivery),
        }
    }

    /// Keep only the complete frame that a host worker validated and encoded.
    pub(crate) fn prepared(
        kind: botster_hub_client::DaemonResponseKind,
        encoded_frame: Vec<u8>,
        charge: crate::host_executor::HostPreparedCharge,
    ) -> Self {
        Self::EncodedPlugin {
            kind,
            encoded_frame,
            charge,
        }
    }

    pub(crate) fn kind(&self) -> Option<botster_hub_client::DaemonResponseKind> {
        match self {
            Self::Typed { response, .. } => response.as_ref().ok().map(|response| response.kind),
            Self::EncodedPlugin { kind, .. } => Some(*kind),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_parts(
        self,
    ) -> (
        DaemonTransportResult<DaemonResponse>,
        Option<RetainedControlCharge>,
        Option<Vec<u8>>,
    ) {
        match self {
            Self::Typed {
                response,
                charge,
                delivery,
            } => {
                drop(delivery);
                (response, charge, None)
            }
            Self::EncodedPlugin {
                encoded_frame,
                charge,
                ..
            } => {
                let frame: botster_hub_client::ServerFrame = serde_json::from_slice(&encoded_frame)
                    .expect("test reply contains a complete frame");
                let botster_hub_client::ServerFrame::Response { response, .. } = frame else {
                    panic!("test reply must contain a response");
                };
                (
                    Ok(response),
                    Some(RetainedControlCharge::Host(charge)),
                    Some(encoded_frame),
                )
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn expect(self, message: &str) -> DaemonResponse {
        self.into_parts().0.expect(message)
    }

    #[cfg(test)]
    pub(crate) fn ok(self) -> Option<DaemonResponse> {
        self.into_parts().0.ok()
    }
}

impl<T> RetainedPluginResult<T> {
    pub(crate) fn new(value: T, charge: RetainedPluginResultCharge) -> Self {
        Self { value, charge }
    }

    pub(crate) fn map<U>(self, transform: impl FnOnce(T) -> U) -> RetainedPluginResult<U> {
        RetainedPluginResult {
            value: transform(self.value),
            charge: self.charge,
        }
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn into_parts(self) -> (T, RetainedPluginResultCharge) {
        (self.value, self.charge)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_plugin_result_charge_is_global_and_releases_on_drop() {
        let budget = RetainedPluginResultBudget::new();
        let first = RetainedPluginResult::new(
            "raw",
            budget.try_reserve(5 * 1024 * 1024).expect("first charge"),
        )
        .map(|value| format!("{value}-mapped"));
        assert_eq!(budget.retained_bytes(), 5 * 1024 * 1024);
        assert!(budget.try_reserve(4 * 1024 * 1024).is_none());
        drop(first);
        assert_eq!(budget.retained_bytes(), 0);
        assert!(budget.take_release_notification());
        assert!(!budget.take_release_notification());
    }

    #[test]
    fn completion_and_capacity_wakes_survive_a_full_owner_queue_independently() {
        let budget = RetainedPluginResultBudget::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(ControlMessage::RejectedConnection)
            .expect("fill owner queue");
        budget.bind_owner_wake(sender);

        (budget.completion_notifier())();
        let charge = budget.try_reserve(1).expect("test retained byte");
        drop(charge);

        assert!(budget.take_completion_notification());
        assert!(budget.take_release_notification());
        assert!(!budget.take_completion_notification());
        assert!(!budget.take_release_notification());
        assert!(matches!(
            receiver.try_recv(),
            Ok(ControlMessage::RejectedConnection)
        ));
    }

    #[test]
    fn retained_reply_charge_releases_on_send_failure_and_receiver_task_abort() {
        let budget = RetainedPluginResultBudget::new();
        let (sender, receiver) = crate::daemon::control::message::control_reply_channel();
        drop(receiver);
        let failed = sender.send_retained(RetainedPluginResult::new(
            Ok(crate::client_api_dto::response::daemon_response_base(
                botster_hub_client::DaemonResponseKind::PluginMcpToolResult,
            )),
            budget.try_reserve(7).expect("failed-send charge"),
        ));
        assert!(failed.is_err());
        drop(failed);
        assert_eq!(budget.retained_bytes(), 0);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("reply abort runtime");
        runtime.block_on(async {
            let (sender, receiver) = crate::daemon::control::message::control_reply_channel();
            let mut requests = tokio::task::JoinSet::new();
            requests.spawn(crate::transport::unix::connection::receive_control_response(receiver));
            sender
                .send_retained(RetainedPluginResult::new(
                    Ok(crate::client_api_dto::response::daemon_response_base(
                        botster_hub_client::DaemonResponseKind::PluginMcpToolResult,
                    )),
                    budget.try_reserve(11).expect("aborted-task charge"),
                ))
                .expect("queue retained reply");
            requests.abort_all();
            while let Some(result) = requests.join_next().await {
                drop(result);
            }
        });
        assert_eq!(budget.retained_bytes(), 0);
    }
}
