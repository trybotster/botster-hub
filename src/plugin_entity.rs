//! Host worker phases for package entity preparation, delivery, and release.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use botster_core::{EntityKind, PluginInvocationResult};
use botster_hub_client::{DaemonEntityFrame, ServerFrame};
use serde_json::Value;

use crate::daemon::control::reply::RetainedPluginResult;
use crate::entity_delivery::{EntitySendError, PrepareEntityError, PreparedEntityDelivery};
use crate::host_executor::HostWorkPermit;
use crate::package_entity_fanout::PackageEntityMutation;
use crate::shared_view::SharedViewBudget;
use crate::subscription::entity::EntityFrameSender;
use crate::{HubRuntime, McpToolError};

pub(crate) enum Command {
    Prepare {
        entity_kind: EntityKind,
        result: RetainedPluginResult<Result<PluginInvocationResult, String>>,
    },
    Deliver {
        payload: Payload,
        target: Arc<Target>,
        publication_live: Arc<AtomicBool>,
        budget: Arc<SharedViewBudget>,
        resync_reason: Option<String>,
    },
    Reclaim(Payload),
}

/// The owner shares this identity without copying protocol strings.
#[derive(Debug)]
pub(crate) struct Target {
    pub(crate) subscription_id: String,
    pub(crate) entity_type: String,
    pub(crate) sender: EntityFrameSender,
}

#[derive(Debug)]
pub(crate) enum Completion {
    Prepared(Payload),
    Delivered {
        payload: Payload,
        status: DeliveryStatus,
    },
    Reclaimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryStatus {
    Sent,
    Full,
    Disconnected,
    Cancelled,
    Invalid,
    Capacity,
}

/// The Host permit covers this allocation through all delivery phases.
/// The owner reads only the scalar sequence and validity state.
#[derive(Debug)]
pub(crate) struct Payload {
    body: Body,
}

#[derive(Debug)]
enum Body {
    Snapshot { sequence: u64, items: Vec<Value> },
    Mutation(PackageEntityMutation),
    Error(McpToolError),
}

impl Payload {
    pub(crate) fn mutation(mutation: PackageEntityMutation) -> Self {
        Self {
            body: Body::Mutation(mutation),
        }
    }

    pub(crate) fn sequence(&self) -> Option<u64> {
        match &self.body {
            Body::Snapshot { sequence, .. } => Some(*sequence),
            Body::Mutation(mutation) => Some(mutation.snapshot_seq()),
            Body::Error(_) => None,
        }
    }

    fn into_frame(self, target: &Target, resync_reason: Option<String>) -> DaemonEntityFrame {
        let subscription_id = target.subscription_id.clone();
        let entity_type = target.entity_type.clone();
        match self.body {
            Body::Snapshot { sequence, items } => DaemonEntityFrame::Snapshot {
                subscription_id,
                entity_type,
                snapshot_seq: sequence,
                items,
                resync_reason,
            },
            Body::Mutation(PackageEntityMutation::Upsert {
                snapshot_seq,
                id,
                entity,
                ..
            }) => DaemonEntityFrame::Upsert {
                subscription_id,
                entity_type,
                snapshot_seq,
                id,
                entity,
            },
            Body::Mutation(PackageEntityMutation::Patch {
                snapshot_seq,
                id,
                patch,
                ..
            }) => DaemonEntityFrame::Patch {
                subscription_id,
                entity_type,
                snapshot_seq,
                id,
                patch,
            },
            Body::Mutation(PackageEntityMutation::Remove {
                snapshot_seq, id, ..
            }) => DaemonEntityFrame::Remove {
                subscription_id,
                entity_type,
                snapshot_seq,
                id,
            },
            Body::Error(error) => DaemonEntityFrame::Error {
                subscription_id,
                entity_type,
                code: error.code,
                message: error.message,
            },
        }
    }

    fn from_frame(frame: DaemonEntityFrame) -> Self {
        let body = match frame {
            DaemonEntityFrame::Snapshot {
                snapshot_seq,
                items,
                ..
            } => Body::Snapshot {
                sequence: snapshot_seq,
                items,
            },
            DaemonEntityFrame::Upsert {
                entity_type,
                snapshot_seq,
                id,
                entity,
                ..
            } => Body::Mutation(PackageEntityMutation::Upsert {
                entity_type,
                snapshot_seq,
                id,
                entity,
            }),
            DaemonEntityFrame::Patch {
                entity_type,
                snapshot_seq,
                id,
                patch,
                ..
            } => Body::Mutation(PackageEntityMutation::Patch {
                entity_type,
                snapshot_seq,
                id,
                patch,
            }),
            DaemonEntityFrame::Remove {
                entity_type,
                snapshot_seq,
                id,
                ..
            } => Body::Mutation(PackageEntityMutation::Remove {
                entity_type,
                snapshot_seq,
                id,
            }),
            DaemonEntityFrame::Error { code, message, .. } => {
                Body::Error(McpToolError::new(code, message))
            }
        };
        Self { body }
    }
}

pub(crate) fn execute(command: Command, permit: &HostWorkPermit) -> Completion {
    assert!(permit.reserved_prepared_bytes() >= crate::host_executor::HOST_PREPARED_BYTE_CAPACITY);
    match command {
        Command::Prepare {
            entity_kind,
            result,
        } => {
            let (result, charge) = result.into_parts();
            let converted = result
                .map_err(|message| McpToolError::new("plugin_completion_inconsistent", message))
                .and_then(|result| {
                    HubRuntime::convert_plugin_entity_snapshot(&entity_kind, result)
                });
            let body = match converted {
                Ok((sequence, items)) => {
                    if crate::bounded_json::encode(
                        &items,
                        crate::admission::budgets::DAEMON_MAX_FRAME_BYTES,
                    )
                    .is_ok()
                    {
                        Body::Snapshot { sequence, items }
                    } else {
                        Body::Error(McpToolError::new(
                            "entity_provider_frame_too_large",
                            "entity provider snapshot exceeds daemon frame limit",
                        ))
                    }
                }
                Err(error) => Body::Error(error),
            };
            drop(charge);
            Completion::Prepared(Payload { body })
        }
        Command::Deliver {
            payload,
            target,
            publication_live,
            budget,
            resync_reason,
        } => {
            if !publication_live.load(Ordering::Acquire) {
                return Completion::Delivered {
                    payload,
                    status: DeliveryStatus::Cancelled,
                };
            }
            let frame = ServerFrame::Entity {
                entity: payload.into_frame(&target, resync_reason),
            };
            let prepared = PreparedEntityDelivery::prepare(&frame, &budget, permit);
            let ServerFrame::Entity { entity } = frame else {
                unreachable!()
            };
            let payload = Payload::from_frame(entity);
            let status = match prepared {
                Ok(prepared) => {
                    // The exchange orders publication against owner cancellation.
                    if publication_live.swap(false, Ordering::AcqRel) {
                        match target.sender.send_prepared_from_worker(prepared) {
                            Ok(()) => DeliveryStatus::Sent,
                            Err(EntitySendError::Full) => DeliveryStatus::Full,
                            Err(EntitySendError::Disconnected) => DeliveryStatus::Disconnected,
                        }
                    } else {
                        drop(prepared);
                        DeliveryStatus::Cancelled
                    }
                }
                Err(PrepareEntityError::Capacity) => DeliveryStatus::Capacity,
                Err(PrepareEntityError::TooLarge | PrepareEntityError::Serialize) => {
                    DeliveryStatus::Invalid
                }
            };
            Completion::Delivered { payload, status }
        }
        Command::Reclaim(payload) => {
            drop(payload);
            Completion::Reclaimed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{HostCommand, HostCompletionPoll, HostExecutor, HostResult};
    use crate::owner_identity::{OwnerWorkIdentity, WaiterId};
    use std::time::{Duration, Instant};

    fn completion(executor: &HostExecutor) -> crate::host_executor::HostCompletion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match executor.poll_completion() {
                HostCompletionPoll::Ready(completion) => return completion,
                HostCompletionPoll::Empty => {
                    assert!(
                        Instant::now() < deadline,
                        "Host worker must complete the phase"
                    );
                    std::thread::yield_now();
                }
                HostCompletionPoll::Stopped => panic!("Host executor stopped"),
            }
        }
    }

    #[test]
    fn worker_delivery_retains_payload_and_permit_through_reclaim() {
        let executor = HostExecutor::new();
        let identity = OwnerWorkIdentity::first(WaiterId(901));
        let budget = SharedViewBudget::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(Target {
            subscription_id: "sub".into(),
            entity_type: "task".into(),
            sender: EntityFrameSender::Async(sender),
        });
        let payload = Payload {
            body: Body::Snapshot {
                sequence: 19,
                items: vec![serde_json::json!({"id": "a", "body": "x".repeat(128 * 1024)})],
            },
        };
        executor
            .submit(
                identity,
                HostCommand::PluginEntity(Command::Deliver {
                    payload,
                    target,
                    publication_live: Arc::new(AtomicBool::new(true)),
                    budget: Arc::clone(&budget),
                    resync_reason: None,
                }),
                executor.try_reserve().unwrap(),
            )
            .unwrap();
        let (received_identity, result, permit) = completion(&executor).into_parts();
        assert_eq!(received_identity, identity);
        let HostResult::PluginEntity(Completion::Delivered { payload, status }) = result else {
            panic!("expected entity delivery completion");
        };
        assert_eq!(status, DeliveryStatus::Sent);
        assert_eq!(payload.sequence(), Some(19));
        assert_eq!(
            permit.reserved_prepared_bytes(),
            crate::host_executor::HOST_PREPARED_BYTE_CAPACITY
        );
        assert!(budget.used() > 128 * 1024);
        let delivery = receiver.try_recv().unwrap();
        let crate::entity_delivery::EntityDelivery::Encoded(delivery) = delivery else {
            panic!("worker must publish an encoded frame");
        };
        let DaemonEntityFrame::Snapshot {
            snapshot_seq,
            items,
            ..
        } = delivery.into_typed()
        else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot_seq, 19);
        assert_eq!(items[0]["body"].as_str().unwrap().len(), 128 * 1024);
        assert_eq!(budget.used(), 0);
        let next = identity.next_phase().unwrap();
        executor
            .submit(
                next,
                HostCommand::PluginEntity(Command::Reclaim(payload)),
                permit,
            )
            .unwrap();
        let (reclaim_identity, result, permit) = completion(&executor).into_parts();
        assert_eq!(reclaim_identity, next);
        assert!(matches!(
            result,
            HostResult::PluginEntity(Completion::Reclaimed)
        ));
        drop(permit);
    }

    #[test]
    fn cancellation_and_full_queue_keep_the_payload_and_release_new_container_charge() {
        let executor = HostExecutor::new();
        let permit = executor.try_reserve().unwrap();
        let budget = SharedViewBudget::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(Target {
            subscription_id: "sub".into(),
            entity_type: "task".into(),
            sender: EntityFrameSender::Async(sender),
        });
        let mut payload = Payload {
            body: Body::Snapshot {
                sequence: 2,
                items: vec![],
            },
        };
        for (live, expected) in [
            (false, DeliveryStatus::Cancelled),
            (true, DeliveryStatus::Sent),
            (true, DeliveryStatus::Full),
        ] {
            let result = execute(
                Command::Deliver {
                    payload,
                    target: Arc::clone(&target),
                    publication_live: Arc::new(AtomicBool::new(live)),
                    budget: Arc::clone(&budget),
                    resync_reason: None,
                },
                &permit,
            );
            let Completion::Delivered {
                payload: returned,
                status,
            } = result
            else {
                panic!("expected delivery completion");
            };
            assert_eq!(status, expected);
            assert_eq!(returned.sequence(), Some(2));
            payload = returned;
            if !live {
                assert_eq!(budget.used(), 0);
                assert!(receiver.try_recv().is_err());
            }
        }
        drop(receiver.try_recv().unwrap());
        assert_eq!(
            budget.used(),
            0,
            "full queue must release the rejected container"
        );
        drop(execute(Command::Reclaim(payload), &permit));
    }
}
