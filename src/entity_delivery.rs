//! Encoded entity frames whose allocation keeps its byte charge until delivery.

use std::sync::Arc;

use botster_hub_client::{
    DaemonEntityFrame, ServerFrame, UNIX_FRAME_LENGTH_PREFIX_BYTES, encode_control_json,
};

use crate::admission::budgets::DAEMON_MAX_FRAME_BYTES;
use crate::bounded_json::{self, EncodeError};
use crate::shared_view::{SharedView, SharedViewBudget};

const CONTROL_HEADER_BYTES: usize = UNIX_FRAME_LENGTH_PREFIX_BYTES + 1;

#[derive(Debug)]
pub(crate) enum EntityDelivery {
    Typed(DaemonEntityFrame),
    Encoded(PreparedEntityDelivery),
}

/// A complete Unix container. WebRTC borrows the JSON after the container header.
#[derive(Debug)]
pub(crate) struct PreparedEntityDelivery {
    container: SharedView<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrepareEntityError {
    TooLarge,
    Serialize,
    Capacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntitySendError {
    Full,
    Disconnected,
}

impl PreparedEntityDelivery {
    /// Call on a host worker while its operation reservation covers JSON staging.
    pub(crate) fn prepare(
        frame: &ServerFrame,
        budget: &Arc<SharedViewBudget>,
        permit: &crate::host_executor::HostWorkPermit,
    ) -> Result<Self, PrepareEntityError> {
        assert!(
            permit.reserved_prepared_bytes() >= crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
            "entity encoding retains the operation preparation reservation"
        );
        let json =
            bounded_json::encode(frame, DAEMON_MAX_FRAME_BYTES).map_err(|error| match error {
                EncodeError::TooLarge => PrepareEntityError::TooLarge,
                EncodeError::Serialize => PrepareEntityError::Serialize,
            })?;
        let charge = budget
            .reserve(json.len() + CONTROL_HEADER_BYTES)
            .map_err(|_| PrepareEntityError::Capacity)?;
        let container = encode_control_json(&json).map_err(|_| PrepareEntityError::Serialize)?;
        Ok(Self {
            container: SharedView::from_reserved(container, charge),
        })
    }

    pub(crate) fn container(&self) -> &[u8] {
        &self.container
    }

    pub(crate) fn json(&self) -> &[u8] {
        &self.container[CONTROL_HEADER_BYTES..]
    }

    #[cfg(test)]
    pub(crate) fn into_typed(self) -> DaemonEntityFrame {
        let ServerFrame::Entity { entity } =
            serde_json::from_slice(self.json()).expect("prepared entity frame is valid JSON")
        else {
            panic!("prepared entity delivery must contain an entity frame");
        };
        entity
    }
}

impl From<DaemonEntityFrame> for EntityDelivery {
    fn from(frame: DaemonEntityFrame) -> Self {
        Self::Typed(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::entity::EntityFrameSender;

    fn frame() -> ServerFrame {
        ServerFrame::Entity {
            entity: DaemonEntityFrame::Snapshot {
                subscription_id: "subscription".into(),
                entity_type: "task".into(),
                snapshot_seq: 7,
                items: vec![serde_json::json!({"id": "a", "title": "work"})],
                resync_reason: None,
            },
        }
    }

    #[test]
    fn entity_container_charge_follows_the_queue_and_releases_on_removal() {
        let executor = crate::host_executor::HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let budget = SharedViewBudget::new();
        let source = frame();
        let prepared = PreparedEntityDelivery::prepare(&source, &budget, &permit).unwrap();
        let charged = prepared.container().len();
        assert_eq!(budget.used(), charged);
        assert_eq!(
            serde_json::from_slice::<ServerFrame>(prepared.json()).unwrap(),
            source
        );
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let sender = EntityFrameSender::Async(sender);
        sender.send_prepared_from_worker(prepared).unwrap();
        assert_eq!(budget.used(), charged);
        let delivery = receiver.try_recv().unwrap();
        assert_eq!(budget.used(), charged);
        drop(delivery);
        assert_eq!(budget.used(), 0);

        sender
            .send_prepared_from_worker(
                PreparedEntityDelivery::prepare(&source, &budget, &permit).unwrap(),
            )
            .unwrap();
        drop(receiver);
        assert_eq!(
            budget.used(),
            0,
            "subscriber removal drops its queued frame"
        );
    }

    #[test]
    fn entity_send_refusal_releases_only_the_rejected_allocation() {
        let executor = crate::host_executor::HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let budget = SharedViewBudget::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let sender = EntityFrameSender::Async(sender);
        sender
            .send_prepared_from_worker(
                PreparedEntityDelivery::prepare(&frame(), &budget, &permit).unwrap(),
            )
            .unwrap();
        let queued = budget.used();
        let next = PreparedEntityDelivery::prepare(&frame(), &budget, &permit).unwrap();
        assert_eq!(budget.used(), queued * 2);
        assert_eq!(
            sender.send_prepared_from_worker(next),
            Err(EntitySendError::Full)
        );
        assert_eq!(budget.used(), queued);
        drop(receiver);
        assert_eq!(budget.used(), 0);
        let next = PreparedEntityDelivery::prepare(&frame(), &budget, &permit).unwrap();
        assert_eq!(
            sender.send_prepared_from_worker(next),
            Err(EntitySendError::Disconnected)
        );
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn entity_encoding_counts_the_complete_envelope_before_reserving_storage() {
        let executor = crate::host_executor::HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let budget = SharedViewBudget::new();
        let mut source = frame();
        let ServerFrame::Entity {
            entity: DaemonEntityFrame::Snapshot { items, .. },
        } = &mut source
        else {
            unreachable!()
        };
        *items = vec![serde_json::json!({"id": "a", "title": "x".repeat(DAEMON_MAX_FRAME_BYTES)})];
        assert_eq!(
            PreparedEntityDelivery::prepare(&source, &budget, &permit).unwrap_err(),
            PrepareEntityError::TooLarge
        );
        assert_eq!(budget.used(), 0);

        let source = frame();
        let bytes = serde_json::to_vec(&source).unwrap().len() + CONTROL_HEADER_BYTES;
        let small = SharedViewBudget::with_capacity(bytes - 1);
        assert_eq!(
            PreparedEntityDelivery::prepare(&source, &small, &permit).unwrap_err(),
            PrepareEntityError::Capacity
        );
        assert_eq!(small.used(), 0);
        let exact = SharedViewBudget::with_capacity(bytes);
        let prepared = PreparedEntityDelivery::prepare(&source, &exact, &permit).unwrap();
        assert_eq!(exact.used(), bytes);
        drop(prepared);
        assert_eq!(exact.used(), 0);
    }
}
