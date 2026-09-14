//! Finite source seams for the isolated allocation test executable.
//!
//! This module uses the real ticket, channel, and registration types.
//! It does not start Core, Hub, or a Lua state.

use super::*;
use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
use crate::lua_runtime::{
    AcknowledgeOutcome, CoordinationReply, HubCoordinationResponse, reply_channel,
};
use botster_core::RoutedEnvelopeDrainOutcome;
use botster_core_daemon::RoutedEnvelopeDeliveryStateResult;

type Reply = CoordinationReply;
type Mark = fn(Phase, usize);

#[derive(Debug, Clone, Copy)]
pub enum Scenario {
    EmptySenderFirst,
    EmptyReceiverFirst,
    SuccessfulSend,
    FailedSend,
    UnreadSenderFirst,
    UnreadReceiverFirst,
    FullSend,
    PublisherLost,
    Refused,
    StoppedOriginal,
    StoppedLegacyOverlap,
    RegistrationSingle,
    RegistrationTwo,
    RegistrationRestore,
    RegistrationLayouts,
    BlockingWait,
    CallbackReplyUnread,
    CallbackReplyWaitTimeout,
    LeaseLastDrop,
}

impl Scenario {
    pub const ALL: [Self; 19] = [
        Self::EmptySenderFirst,
        Self::EmptyReceiverFirst,
        Self::SuccessfulSend,
        Self::FailedSend,
        Self::UnreadSenderFirst,
        Self::UnreadReceiverFirst,
        Self::FullSend,
        Self::PublisherLost,
        Self::Refused,
        Self::StoppedOriginal,
        Self::StoppedLegacyOverlap,
        Self::RegistrationSingle,
        Self::RegistrationTwo,
        Self::RegistrationRestore,
        Self::RegistrationLayouts,
        Self::BlockingWait,
        Self::CallbackReplyUnread,
        Self::CallbackReplyWaitTimeout,
        Self::LeaseLastDrop,
    ];
}

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum Phase {
    Startup,
    Start,
    WakeConstruct,
    Register,
    ChannelConstruct,
    PayloadConstruct,
    Send,
    Collect,
    Poll,
    ResultDrop,
    SenderDrop,
    ReceiverDrop,
    Retire,
    WakeDrop,
    QueueConstruct,
    QueueFill,
    ClosureConstruct,
    Admission,
    LegacyLost,
    QueueDrop,
    AtCapacity,
    Publish,
    AllReady,
    Restore,
    EmptyRoots,
    Wait,
    AccountConstruct,
    LeaseConstruct,
    LeaseClone,
    LeaseFirstDrop,
    LeaseLastDrop,
    AccountDrop,
    RegisteredLayout,
    ReadyLayout,
    NextPhaseLayout,
    End,
}

impl Phase {
    pub const ALL: [Self; 36] = [
        Self::Startup,
        Self::Start,
        Self::WakeConstruct,
        Self::Register,
        Self::ChannelConstruct,
        Self::PayloadConstruct,
        Self::Send,
        Self::Collect,
        Self::Poll,
        Self::ResultDrop,
        Self::SenderDrop,
        Self::ReceiverDrop,
        Self::Retire,
        Self::WakeDrop,
        Self::QueueConstruct,
        Self::QueueFill,
        Self::ClosureConstruct,
        Self::Admission,
        Self::LegacyLost,
        Self::QueueDrop,
        Self::AtCapacity,
        Self::Publish,
        Self::AllReady,
        Self::Restore,
        Self::EmptyRoots,
        Self::Wait,
        Self::AccountConstruct,
        Self::LeaseConstruct,
        Self::LeaseClone,
        Self::LeaseFirstDrop,
        Self::LeaseLastDrop,
        Self::AccountDrop,
        Self::RegisteredLayout,
        Self::ReadyLayout,
        Self::NextPhaseLayout,
        Self::End,
    ];
}

/// Concrete Hub layouts. Private std layouts come only from recorded allocation requests.
pub fn type_layouts() -> [(&'static str, usize, usize); 5] {
    [
        (
            "Reply",
            std::mem::size_of::<Reply>(),
            std::mem::align_of::<Reply>(),
        ),
        (
            "CoreTicketResult<Reply>",
            std::mem::size_of::<CoreTicketResult<Reply>>(),
            std::mem::align_of::<CoreTicketResult<Reply>>(),
        ),
        (
            "CoreRequest",
            std::mem::size_of::<CoreRequest>(),
            std::mem::align_of::<CoreRequest>(),
        ),
        (
            "CoreCompletionWake",
            std::mem::size_of::<CoreCompletionWake>(),
            std::mem::align_of::<CoreCompletionWake>(),
        ),
        (
            "crate::lua_memory::LuaCallbackCharge",
            std::mem::size_of::<crate::lua_memory::LuaCallbackCharge>(),
            std::mem::align_of::<crate::lua_memory::LuaCallbackCharge>(),
        ),
    ]
}

fn reply(mark: Mark, index: usize) -> Reply {
    mark(Phase::PayloadConstruct, index);
    // Core-ticket scenarios size CoordinationReply. Drain needs no callback charge.
    Ok(HubCoordinationResponse::Drain(
        RoutedEnvelopeDrainOutcome::default(),
    ))
}

fn assert_reply(value: Reply) {
    assert!(matches!(value, Ok(HubCoordinationResponse::Drain(_))));
}

fn channel(scenario: Scenario, mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let wake = Arc::new(CoreCompletionWake::new());
    let waiter = WaiterId(1);
    mark(Phase::Register, 0);
    let identity = wake.register_phases(waiter, 1).unwrap()[0];
    mark(Phase::ChannelConstruct, 0);
    let (mut ticket, publisher) = CoreTicket::<Reply>::channel(identity, Arc::clone(&wake), true);
    match scenario {
        Scenario::EmptySenderFirst => {
            mark(Phase::SenderDrop, 0);
            drop(publisher);
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
        }
        Scenario::EmptyReceiverFirst => {
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
            mark(Phase::SenderDrop, 0);
            drop(publisher);
        }
        Scenario::SuccessfulSend => {
            let value = reply(mark, 0);
            mark(Phase::Send, 0);
            publisher.publish(value);
            mark(Phase::Collect, 0);
            assert_eq!(wake.take_identities(1), [identity]);
            mark(Phase::Poll, 0);
            let CoreTicketPoll::Ready(value) = ticket.poll() else {
                panic!("the ticket did not return its result")
            };
            mark(Phase::ResultDrop, 0);
            assert_reply(value);
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
        }
        Scenario::FailedSend => {
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
            let value = reply(mark, 0);
            mark(Phase::Send, 0);
            publisher.publish(value);
        }
        Scenario::UnreadSenderFirst => {
            let value = reply(mark, 0);
            mark(Phase::Send, 0);
            publisher.publish(value);
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
        }
        Scenario::UnreadReceiverFirst => {
            let value = reply(mark, 0);
            mark(Phase::Send, 0);
            assert!(
                publisher
                    .sender
                    .as_ref()
                    .unwrap()
                    .try_send(CoreTicketResult { identity, value })
                    .is_ok()
            );
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
            mark(Phase::SenderDrop, 0);
            drop(publisher);
        }
        Scenario::FullSend => {
            let first = reply(mark, 0);
            mark(Phase::Send, 0);
            assert!(
                publisher
                    .sender
                    .as_ref()
                    .unwrap()
                    .try_send(CoreTicketResult {
                        identity,
                        value: first
                    })
                    .is_ok()
            );
            let second = reply(mark, 1);
            mark(Phase::Send, 1);
            assert!(matches!(
                publisher
                    .sender
                    .as_ref()
                    .unwrap()
                    .try_send(CoreTicketResult {
                        identity,
                        value: second
                    }),
                Err(TrySendError::Full(_))
            ));
            mark(Phase::SenderDrop, 0);
            drop(publisher);
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
        }
        Scenario::PublisherLost => {
            mark(Phase::SenderDrop, 0);
            drop(publisher);
            mark(Phase::Collect, 0);
            assert_eq!(wake.take_identities(1), [identity]);
            mark(Phase::Poll, 0);
            assert!(matches!(ticket.poll(), CoreTicketPoll::Lost));
            mark(Phase::ReceiverDrop, 0);
            drop(ticket);
        }
        _ => unreachable!("the scenario dispatcher selects channel cases"),
    }
    mark(Phase::Retire, 0);
    wake.retire_waiter(waiter);
    assert_empty(&wake);
    mark(Phase::EmptyRoots, 0);
    mark(Phase::WakeDrop, 0);
    drop(wake);
}

fn admission(scenario: Scenario, mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let wake = Arc::new(CoreCompletionWake::new());
    mark(Phase::QueueConstruct, 0);
    let (requests, receiver) = mpsc::sync_channel::<CoreRequest>(CORE_REQUEST_CAPACITY);
    if matches!(scenario, Scenario::Refused) {
        for index in 0..CORE_REQUEST_CAPACITY {
            mark(Phase::QueueFill, index);
            assert!(requests.try_send(CoreRequest::new(|_, _| {})).is_ok());
        }
    }
    mark(Phase::Register, 0);
    let waiter = WaiterId(1);
    let identity = wake.register_phases(waiter, 1).unwrap()[0];
    mark(Phase::ChannelConstruct, 0);
    let (mut ticket, publisher) = CoreTicket::<Reply>::channel(identity, Arc::clone(&wake), true);
    mark(Phase::ClosureConstruct, 0);
    let request = CoreRequest::new(move |_, _| {
        publisher.publish(Ok(HubCoordinationResponse::Drain(
            RoutedEnvelopeDrainOutcome::default(),
        )))
    });
    let accepting = AtomicBool::new(matches!(scenario, Scenario::Refused));
    mark(Phase::Admission, 0);
    let outcome = admit_request(&requests, &accepting, request);
    assert!(matches!(
        (scenario, outcome),
        (Scenario::Refused, CoreAdmission::Refused)
            | (
                Scenario::StoppedOriginal | Scenario::StoppedLegacyOverlap,
                CoreAdmission::Stopped
            )
    ));
    mark(Phase::Retire, 0);
    wake.retire(identity);
    if matches!(scenario, Scenario::StoppedLegacyOverlap) {
        // This reproduces the existing extra allocation. It is not the selected C1 behavior.
        mark(Phase::LegacyLost, 0);
        let mut extra = CoreTicket::<Reply>::lost(identity);
        mark(Phase::Poll, 1);
        assert!(matches!(extra.poll(), CoreTicketPoll::Lost));
        mark(Phase::ReceiverDrop, 1);
        drop(extra);
    }
    mark(Phase::Poll, 0);
    assert!(matches!(ticket.poll(), CoreTicketPoll::Lost));
    mark(Phase::ReceiverDrop, 0);
    drop(ticket);
    mark(Phase::Retire, 1);
    wake.retire_waiter(waiter);
    assert_empty(&wake);
    mark(Phase::QueueDrop, 0);
    drop((requests, receiver));
    mark(Phase::WakeDrop, 0);
    drop(wake);
}

fn assert_empty(wake: &CoreCompletionWake) {
    let state = wake.identities.lock().unwrap();
    assert!(state.registered.is_empty() && state.ready.is_empty() && state.next_phase.is_empty());
}

fn registrations(two: bool, mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let wake = CoreCompletionWake::new();
    let phases = if two { 2 } else { 1 };
    let waiters = CORE_OWNER_COMPLETION_CAPACITY / phases;
    for index in 0..waiters {
        mark(Phase::Register, index);
        assert_eq!(
            wake.register_phases(WaiterId(index as u64 + 1), phases)
                .unwrap()
                .len(),
            phases
        );
    }
    mark(Phase::AtCapacity, waiters);
    assert!(
        wake.register_phases(WaiterId(waiters as u64 + 1), 1)
            .is_none()
    );
    for index in 0..waiters {
        for phase in 1..=phases {
            mark(Phase::Publish, index * phases + phase - 1);
            wake.publish(OwnerWorkIdentity {
                waiter_id: WaiterId(index as u64 + 1),
                phase: phase as u64,
            });
        }
    }
    mark(Phase::AllReady, CORE_OWNER_COMPLETION_CAPACITY);
    for index in 0..CORE_OWNER_COMPLETION_CAPACITY {
        mark(Phase::Collect, index);
        assert_eq!(wake.take_identities(1).len(), 1);
    }
    for index in 0..waiters {
        mark(Phase::Retire, index);
        wake.retire_waiter(WaiterId(index as u64 + 1));
    }
    assert_empty(&wake);
    mark(Phase::EmptyRoots, 0);
    mark(Phase::WakeDrop, 0);
    drop(wake);
}

fn restore(mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let wake = CoreCompletionWake::new();
    let waiter = WaiterId(1);
    mark(Phase::Register, 0);
    let identity = wake.register_phases(waiter, 1).unwrap()[0];
    mark(Phase::Publish, 0);
    wake.publish(identity);
    mark(Phase::Collect, 0);
    let taken = wake.take_identities(1);
    mark(Phase::Restore, 0);
    wake.restore_identities(&taken);
    drop(taken);
    mark(Phase::Collect, 1);
    assert_eq!(wake.take_identities(1), [identity]);
    mark(Phase::Register, 1);
    let next = wake.register_phases(waiter, 1).unwrap()[0];
    assert_eq!(next.phase, 2);
    mark(Phase::Publish, 1);
    wake.publish(next);
    mark(Phase::Retire, 0);
    assert_eq!(wake.retire_waiter(waiter), 1);
    assert_empty(&wake);
    mark(Phase::EmptyRoots, 0);
    mark(Phase::WakeDrop, 0);
    drop(wake);
}

fn blocking_wait(mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let wake = Arc::new(CoreCompletionWake::new());
    mark(Phase::ChannelConstruct, 0);
    let (ticket, publisher) = CoreTicket::<Reply>::channel(
        OwnerWorkIdentity::first(WaiterId(1)),
        Arc::clone(&wake),
        false,
    );
    mark(Phase::Wait, 0);
    assert!(matches!(
        ticket.wait(Duration::from_secs(1)),
        Err(CoreTicketError::Timeout)
    ));
    mark(Phase::SenderDrop, 0);
    drop(publisher);
    mark(Phase::WakeDrop, 0);
    drop(wake);
    // The test executable keeps recording until this fresh thread exits and drops its TLS.
}

fn registration_layouts(mark: Mark) {
    mark(Phase::WakeConstruct, 0);
    let mut wake = CoreCompletionWake::new();
    let state = wake.identities.get_mut().unwrap();
    // Twelve keys cross the pinned std node capacity of eleven.
    // Separate phases identify each real container's allocation requests.
    for index in 0..12 {
        let waiter = WaiterId(index as u64 + 1);
        let identity = OwnerWorkIdentity::first(waiter);
        mark(Phase::RegisteredLayout, index);
        state.registered.insert(identity);
        mark(Phase::ReadyLayout, index);
        state.ready.insert(identity);
        mark(Phase::NextPhaseLayout, index);
        state.next_phase.insert(waiter, 1);
    }
    for index in 0..12 {
        mark(Phase::Retire, index);
        wake.retire_waiter(WaiterId(index as u64 + 1));
    }
    assert_empty(&wake);
    mark(Phase::EmptyRoots, 0);
    mark(Phase::WakeDrop, 0);
    drop(wake);
}

fn lease(mark: Mark) {
    mark(Phase::AccountConstruct, 0);
    let account = LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 1,
        total_vm_bytes: 1,
        per_callback_bytes: 1,
        total_callback_bytes: 1,
    })
    .unwrap();
    let charge = account.reserve_callback_bytes(1).unwrap();
    mark(Phase::LeaseConstruct, 0);
    let first = Arc::new(charge);
    mark(Phase::LeaseClone, 0);
    let second = Arc::clone(&first);
    mark(Phase::LeaseFirstDrop, 0);
    assert!(Arc::into_inner(first).is_none());
    mark(Phase::LeaseLastDrop, 0);
    drop(Arc::into_inner(second).unwrap());
    mark(Phase::AccountDrop, 0);
    drop(account);
}

fn callback_reply(wait: bool, mark: Mark) {
    // Production acknowledgements use reply_channel: sync_channel(1) plus a lease.
    mark(Phase::AccountConstruct, 0);
    let reply_bytes = crate::lua_memory::layout::single_reply_bytes::<Reply>(true)
        .expect("callback reply layout");
    let payload_bytes = 2;
    let total = reply_bytes
        .checked_add(payload_bytes)
        .expect("reply plus payload");
    let account = LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 1,
        total_vm_bytes: 1,
        per_callback_bytes: total,
        total_callback_bytes: total,
    })
    .unwrap();
    let mut admitted = account.reserve_callback_total(total).unwrap();
    let reply_charge = admitted.split(reply_bytes).expect("reply segment");
    let result_charge = admitted.split(1).expect("result segment");
    let conversion_charge = admitted;
    mark(Phase::ChannelConstruct, 0);
    let (sender, receiver) = reply_channel(reply_charge);
    if wait {
        mark(Phase::Wait, 0);
        assert!(matches!(
            receiver.recv_timeout(Duration::from_secs(1)),
            Err(RecvTimeoutError::Timeout)
        ));
        mark(Phase::ReceiverDrop, 0);
        drop(receiver);
        mark(Phase::SenderDrop, 0);
        drop(sender);
        drop((result_charge, conversion_charge));
    } else {
        mark(Phase::PayloadConstruct, 0);
        let outcome = AcknowledgeOutcome::new(
            RoutedEnvelopeDeliveryStateResult { state: None },
            result_charge,
            conversion_charge,
        );
        mark(Phase::Send, 0);
        assert!(sender.send(Ok(outcome)).is_ok());
        mark(Phase::SenderDrop, 0);
        mark(Phase::ReceiverDrop, 0);
        drop(receiver);
    }
}

/// Execute one finite scenario. The caller records until the fresh thread exits.
pub fn run(scenario: Scenario, mark: Mark) {
    mark(Phase::Start, 0);
    match scenario {
        Scenario::Refused | Scenario::StoppedOriginal | Scenario::StoppedLegacyOverlap => {
            admission(scenario, mark)
        }
        Scenario::RegistrationSingle => registrations(false, mark),
        Scenario::RegistrationTwo => registrations(true, mark),
        Scenario::RegistrationRestore => restore(mark),
        Scenario::RegistrationLayouts => registration_layouts(mark),
        Scenario::BlockingWait => blocking_wait(mark),
        Scenario::CallbackReplyUnread => callback_reply(false, mark),
        Scenario::CallbackReplyWaitTimeout => callback_reply(true, mark),
        Scenario::LeaseLastDrop => lease(mark),
        _ => channel(scenario, mark),
    }
    mark(Phase::End, 0);
}
