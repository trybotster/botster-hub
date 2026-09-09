//! Host worker phases for package entity preparation, delivery, and release.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use botster_core::PluginInvocationResult;
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
        invocation: crate::runtime::PluginEntitySnapshotInvocation,
        result: RetainedPluginResult<PluginInvocationResult>,
        inconsistent: bool,
        target: Option<Arc<Target>>,
    },
    PrepareMutation(PackageEntityMutation),
    Deliver {
        payload: Payload,
        target: Arc<Target>,
        publication_live: Arc<AtomicBool>,
        budget: Arc<SharedViewBudget>,
        resync_reason: Option<String>,
    },
    Reclaim(Payload),
    Discard {
        payload: Option<Payload>,
        registration: Option<Registration>,
        reservation_identity: Option<crate::admission::reservations::PreparedSubscriptionIdentity>,
    },
    Finish {
        payload: Option<Payload>,
        registration: Option<Registration>,
        reservation_identity: Option<crate::admission::reservations::PreparedSubscriptionIdentity>,
        target: Arc<Target>,
        reservation: Option<botster_hub_client::DaemonSubscriptionReservation>,
        error: Option<(&'static str, &'static str)>,
        transport_request_id: String,
        reply_tx: crate::daemon::control::message::ControlReplySender,
        publication_live: Arc<AtomicBool>,
    },
}

#[derive(Debug)]
pub(crate) struct Registration {
    pub(crate) subscription_id: String,
    pub(crate) target_key: String,
    pub(crate) entity_type: String,
    pub(crate) reservation: crate::admission::reservations::PreparedSubscriptionIdentity,
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
    Prepared {
        payload: Payload,
        family: Arc<String>,
        registration: Option<Registration>,
    },
    Delivered {
        payload: Payload,
        status: DeliveryStatus,
    },
    Reclaimed,
    Finished {
        sent: bool,
    },
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
/// Keep admission after the body so payload destruction finishes before capacity returns.
#[derive(Debug)]
pub(crate) struct Payload {
    body: Body,
    admission: Option<crate::lua_runtime::EntityPublishPermit>,
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
            admission: mutation.admission().cloned(),
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
                admission: None,
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
                admission: None,
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
                admission: None,
                entity_type,
                snapshot_seq,
                id,
            }),
            DaemonEntityFrame::Error { code, message, .. } => {
                Body::Error(McpToolError::new(code, message))
            }
        };
        Self {
            body,
            admission: None,
        }
    }
}

pub(crate) fn execute(command: Command, permit: &mut HostWorkPermit) -> Completion {
    // Every phase retains the full reservation until the terminal reply consumes it.
    assert!(permit.reserved_prepared_bytes() >= crate::host_executor::HOST_PREPARED_BYTE_CAPACITY);
    match command {
        Command::Prepare {
            invocation,
            result,
            inconsistent,
            target,
        } => {
            let family = Arc::new(invocation.expected_entity_kind().as_str().to_string());
            let registration = target.map(|target| Registration {
                subscription_id: target.subscription_id.clone(),
                target_key: target.subscription_id.clone(),
                entity_type: target.entity_type.clone(),
                reservation: crate::admission::reservations::PreparedSubscriptionIdentity::new(
                    target.subscription_id.clone(),
                ),
            });
            let (result, charge) = result.into_parts();
            let converted = if inconsistent {
                drop(result);
                Err(McpToolError::new(
                    "plugin_completion_inconsistent",
                    "plugin completion identity did not match the admitted request",
                ))
            } else {
                HubRuntime::convert_plugin_entity_snapshot(
                    invocation.expected_entity_kind(),
                    result,
                )
            };
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
            Completion::Prepared {
                payload: Payload {
                    body,
                    admission: invocation.admission,
                },
                family,
                registration,
            }
        }
        Command::PrepareMutation(mutation) => Completion::Prepared {
            family: Arc::new(mutation.entity_type().to_string()),
            payload: Payload::mutation(mutation),
            registration: None,
        },
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
            let admission = payload.admission.clone();
            let frame = ServerFrame::Entity {
                entity: payload.into_frame(&target, resync_reason),
            };
            let prepared = PreparedEntityDelivery::prepare(&frame, &budget, permit);
            let ServerFrame::Entity { entity } = frame else {
                unreachable!()
            };
            let mut payload = Payload::from_frame(entity);
            if let Body::Mutation(mutation) = &mut payload.body {
                mutation.set_admission(admission.clone());
            }
            payload.admission = admission;
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
        Command::Discard {
            payload,
            registration,
            reservation_identity,
        } => {
            drop(payload);
            drop(registration);
            drop(reservation_identity);
            Completion::Reclaimed
        }
        Command::Finish {
            payload,
            registration,
            reservation_identity,
            target,
            reservation,
            error,
            transport_request_id,
            reply_tx,
            publication_live,
        } => {
            drop(registration);
            drop(reservation_identity);
            let provider_error = payload.and_then(|payload| match payload.body {
                Body::Error(error) => Some(error),
                _ => None,
            });
            let response = if let Some((code, message)) = error {
                crate::subscription::entity::entity_subscription_error(
                    code,
                    &target.subscription_id,
                    message,
                )
            } else if let Some(error) = provider_error {
                crate::subscription::entity::entity_subscription_error(
                    &error.code,
                    &target.subscription_id,
                    &error.message,
                )
            } else {
                let mut response = crate::client_api_dto::response::daemon_response_base(
                    botster_hub_client::DaemonResponseKind::EntitySubscribed,
                );
                response.subscription_reservation = reservation;
                response
            };
            let prepared =
                match crate::plugin_response::encode_response(response, &transport_request_id) {
                    Ok(prepared) => prepared,
                    Err(_) => {
                        // The fallback omits protocol strings supplied by the subscription.
                        let response = crate::subscription::entity::entity_subscription_error(
                            "entity_provider_frame_too_large",
                            "",
                            "entity subscription response exceeds daemon frame limit",
                        );
                        crate::plugin_response::encode_protocol_bounded_error(
                            response,
                            &transport_request_id,
                            "entity",
                        )
                    }
                };
            let charge = permit.take_prepared_charge(prepared.logical_bytes);
            let sent = if publication_live.swap(false, Ordering::AcqRel) {
                let reply = crate::daemon::control::reply::ControlReply::prepared(
                    prepared.kind,
                    prepared.encoded_frame,
                    charge,
                );
                match reply_tx.send_reply(reply) {
                    Ok(()) => true,
                    Err(reply) => {
                        drop(reply);
                        false
                    }
                }
            } else {
                drop(prepared);
                drop(charge);
                false
            };
            Completion::Finished { sent }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_executor::{HostCommand, HostCompletionPoll, HostExecutor, HostResult};
    use crate::owner_identity::{OwnerWorkIdentity, WaiterId};
    use std::time::{Duration, Instant};

    #[test]
    fn terminal_reply_moves_the_charge_and_preserves_request_correlation() {
        let executor = HostExecutor::new();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(Target {
            subscription_id: "sub".into(),
            entity_type: "task".into(),
            sender: EntityFrameSender::Async(sender),
        });
        let (reply_tx, mut reply_rx) = crate::daemon::control::message::control_reply_channel();
        executor
            .submit(
                OwnerWorkIdentity::first(WaiterId(903)),
                HostCommand::PluginEntity(Command::Finish {
                    registration: None,
                    reservation_identity: None,
                    payload: Some(Payload {
                        admission: None,
                        body: Body::Error(McpToolError::new("provider_error", "provider detail")),
                    }),
                    target,
                    reservation: None,
                    error: None,
                    transport_request_id: "42".into(),
                    reply_tx,
                    publication_live: Arc::new(AtomicBool::new(true)),
                }),
                executor.try_reserve().unwrap(),
            )
            .unwrap();
        let (_, result, permit) = completion(&executor).into_parts();
        assert!(matches!(
            result,
            HostResult::PluginEntity(Completion::Finished { sent: true })
        ));
        assert_eq!(permit.reserved_prepared_bytes(), 0);
        let reply = reply_rx.try_recv().unwrap();
        let (_, charge, encoded) = reply.into_parts();
        let frame: ServerFrame = serde_json::from_slice(encoded.as_ref().unwrap()).unwrap();
        let ServerFrame::Response {
            request_id,
            response,
        } = frame
        else {
            panic!("expected response");
        };
        assert_eq!(request_id, "42");
        let error = response.error.unwrap();
        assert_eq!(error.code, "provider_error");
        assert_eq!(error.message, "provider detail");
        drop(permit);
        drop(encoded);
        drop(charge);
    }

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
            admission: None,
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
    fn lifetime_budget_survives_worker_delivery_conversions_and_reclaim() {
        let bridge = crate::lua_runtime::HubEntityPublishBridge::new();
        let _reply = bridge.test_queue_publish(
            botster_core::PluginKey("p".into()),
            serde_json::json!({"type":"entity_patch", "entity_type":"p.item", "snapshot_seq":1,
                "id":"item", "patch":{"body":"x".repeat(16384)}}),
            None,
        );
        let (request, ()) = bridge.take_if(|_| Some(())).unwrap();
        let executor = HostExecutor::new();
        let mut permit = executor.try_reserve().unwrap();
        let mut identity = OwnerWorkIdentity::first(WaiterId(904));
        let mut run = |command, permit| {
            executor
                .submit(identity, HostCommand::PluginEntity(command), permit)
                .unwrap();
            let (returned_identity, result, permit) = completion(&executor).into_parts();
            assert_eq!(returned_identity, identity);
            identity = identity.next_phase().unwrap();
            let HostResult::PluginEntity(result) = result else {
                panic!("expected entity completion")
            };
            (result, permit)
        };
        let budget = SharedViewBudget::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(Target {
            subscription_id: "sub".into(),
            entity_type: "p.item".into(),
            sender: EntityFrameSender::Async(sender),
        });
        let mut payload = Payload::mutation(request.mutation);
        for (live, expected) in [
            (false, DeliveryStatus::Cancelled),
            (true, DeliveryStatus::Sent),
            (true, DeliveryStatus::Full),
        ] {
            let (result, returned_permit) = run(
                Command::Deliver {
                    payload,
                    target: Arc::clone(&target),
                    publication_live: Arc::new(AtomicBool::new(live)),
                    budget: Arc::clone(&budget),
                    resync_reason: None,
                },
                permit,
            );
            permit = returned_permit;
            let Completion::Delivered {
                payload: returned,
                status,
            } = result
            else {
                panic!("delivery returns its payload")
            };
            assert_eq!(status, expected);
            assert_eq!(bridge.retained_counts().0, 1);
            payload = returned;
        }
        drop(receiver);
        let (result, returned_permit) = run(
            Command::Deliver {
                payload,
                target,
                publication_live: Arc::new(AtomicBool::new(true)),
                budget,
                resync_reason: None,
            },
            permit,
        );
        permit = returned_permit;
        let Completion::Delivered { payload, status } = result else {
            panic!("delivery returns its payload")
        };
        assert_eq!(status, DeliveryStatus::Disconnected);
        assert_eq!(bridge.retained_counts().0, 1);
        assert!(matches!(
            run(Command::Reclaim(payload), permit).0,
            Completion::Reclaimed
        ));
        assert_eq!(bridge.retained_counts(), (0, 0));
    }

    #[test]
    fn cancellation_and_full_queue_keep_the_payload_and_release_new_container_charge() {
        let executor = HostExecutor::new();
        let mut permit = executor.try_reserve().unwrap();
        let budget = SharedViewBudget::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let target = Arc::new(Target {
            subscription_id: "sub".into(),
            entity_type: "task".into(),
            sender: EntityFrameSender::Async(sender),
        });
        let mut payload = Payload {
            admission: None,
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
                &mut permit,
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
        drop(execute(Command::Reclaim(payload), &mut permit));
    }
}
