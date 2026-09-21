//! Local receipts share the Core collector and retain channel storage.

use super::*;
use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};

fn memory() -> Arc<LuaMemoryAccount> {
    LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 1024,
        total_vm_bytes: 1024,
        per_callback_bytes: 64 * 1024,
        total_callback_bytes: 64 * 1024,
    })
    .unwrap()
}

fn new_row(wake: &Arc<CoreCompletionWake>, id: u64) -> CoreWaiterRetirement {
    CoreWaiterRetirement {
        wake: Arc::clone(wake),
        waiter_id: WaiterId(id),
    }
}

fn charge<T>(memory: &Arc<LuaMemoryAccount>) -> crate::lua_memory::LuaCallbackCharge {
    memory
        .reserve_callback_total(retained_reply_bytes::<T>().unwrap())
        .unwrap()
}

#[test]
fn local_reply_refusal_returns_storage_before_channel_construction() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    let row = new_row(&wake, 1);
    let bytes = retained_reply_bytes::<u8>().unwrap();
    let too_small = memory.reserve_callback_total(bytes - 1).unwrap();
    let returned = row.local_reply::<u8>(too_small).unwrap_err();
    assert_eq!(returned.bytes(), bytes - 1);
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
    assert_eq!(memory.usage().1, bytes - 1);
    drop(returned);

    let previous = wake.register_phases(row.waiter_id, 2).unwrap();
    let returned = row.local_reply::<u8>(charge::<u8>(&memory)).unwrap_err();
    assert_eq!(returned.bytes(), bytes);
    assert_eq!(wake.live_identity_counts(), (2, 0, 1));
    assert_eq!(memory.usage().1, bytes);
    for identity in previous {
        assert!(wake.retire(identity));
    }
    let (ticket, publisher) = row.local_reply::<u8>(returned).unwrap();
    assert_eq!(publisher.0.identity.phase, 3);
    drop((publisher, ticket, row));
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
    assert_eq!(memory.usage().1, 0);
}

#[test]
fn local_reply_drop_wakes_loss_after_collection() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    let row = new_row(&wake, 2);
    let (mut ticket, publisher) = row.local_reply::<u8>(charge::<u8>(&memory)).unwrap();
    let identity = publisher.0.identity;
    drop(publisher);
    assert!(wake.take());
    assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
    assert_eq!(wake.take_identities(1), vec![identity]);
    assert!(matches!(ticket.poll(), CoreTicketPoll::Lost));
    assert!(memory.usage().1 > 0);
    drop((ticket, row));
    assert_eq!(memory.usage().1, 0);
}

#[test]
fn local_reply_success_preserves_phase_history() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    let row = new_row(&wake, 3);
    for phase in 1..=2 {
        let (mut ticket, publisher) = row.local_reply::<u64>(charge::<u64>(&memory)).unwrap();
        let identity = publisher.0.identity;
        assert_eq!(identity.phase, phase);
        publisher.publish(phase);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Pending));
        assert_eq!(wake.take_identities(1), vec![identity]);
        assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(value) if value == phase));
        drop(ticket);
        assert_eq!(memory.usage().1, 0);
        assert_eq!(wake.live_identity_counts(), (0, 0, 1));
    }
    drop(row);
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
}

#[derive(Debug)]
struct Payload(Arc<LuaMemoryAccount>);

impl Drop for Payload {
    fn drop(&mut self) {
        assert!(
            self.0.usage().1 > 0,
            "the payload must drop before its channel charge"
        );
    }
}

#[test]
fn local_reply_storage_survives_both_endpoint_orders() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    for receiver_first in [false, true] {
        let row = new_row(&wake, if receiver_first { 5 } else { 4 });
        let (ticket, publisher) = row
            .local_reply::<Payload>(charge::<Payload>(&memory))
            .unwrap();
        if receiver_first {
            drop(ticket);
            assert!(memory.usage().1 > 0);
            publisher.publish(Payload(Arc::clone(&memory)));
        } else {
            publisher.publish(Payload(Arc::clone(&memory)));
            assert!(memory.usage().1 > 0);
            drop(ticket);
        }
        assert_eq!(memory.usage().1, 0);
        drop(row);
    }
}

#[test]
fn local_reply_terminal_retirement_keeps_endpoint_storage() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    let row = new_row(&wake, 6);
    let (mut ticket, publisher) = row.local_reply::<u8>(charge::<u8>(&memory)).unwrap();
    let identity = publisher.0.identity;
    wake.bind_terminal_owner();
    publisher.publish(7);
    assert_eq!(wake.take_terminal_identity(row.waiter_id), Some(identity));
    assert!(matches!(ticket.poll(), CoreTicketPoll::Ready(7)));
    drop(row);
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
    assert!(memory.usage().1 > 0);
    drop(ticket);
    assert_eq!(memory.usage().1, 0);

    let row = new_row(&wake, 7);
    let (ticket, publisher) = row.local_reply::<u8>(charge::<u8>(&memory)).unwrap();
    drop(row);
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
    drop(publisher);
    assert!(wake.take_identities(1).is_empty());
    assert!(memory.usage().1 > 0);
    drop(ticket);
    assert_eq!(memory.usage().1, 0);
}

#[test]
fn local_reply_other_waiter_cannot_complete_this_receipt() {
    let memory = memory();
    let wake = Arc::new(CoreCompletionWake::new());
    let first = new_row(&wake, 8);
    let second = new_row(&wake, 9);
    let (mut first_ticket, first_publisher) =
        first.local_reply::<u8>(charge::<u8>(&memory)).unwrap();
    let (mut second_ticket, second_publisher) =
        second.local_reply::<u8>(charge::<u8>(&memory)).unwrap();
    let first_identity = first_publisher.0.identity;
    let second_identity = second_publisher.0.identity;
    first_publisher.publish(1);
    assert_eq!(wake.take_identities(1), vec![first_identity]);
    assert!(matches!(first_ticket.poll(), CoreTicketPoll::Ready(1)));
    assert!(matches!(second_ticket.poll(), CoreTicketPoll::Pending));
    drop(first);
    wake.publish(first_identity);
    assert!(wake.take_identities(1).is_empty());
    assert!(matches!(second_ticket.poll(), CoreTicketPoll::Pending));
    second_publisher.publish(2);
    assert_eq!(wake.take_identities(1), vec![second_identity]);
    assert!(matches!(second_ticket.poll(), CoreTicketPoll::Ready(2)));
    drop((first_ticket, second_ticket, second));
    assert_eq!(memory.usage().1, 0);
    assert_eq!(wake.live_identity_counts(), (0, 0, 0));
}
