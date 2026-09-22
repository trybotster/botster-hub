//! One funded reply slot for an ordinary spawn.
//! The payload must retain its own charge independently of this channel.

use std::sync::mpsc;
use std::time::Duration;

use crate::lua_memory::{LuaCallbackCharge, LuaCallbackStorageLease, layout};

#[allow(dead_code)] // The Host spawn delivery will own this endpoint.
pub(crate) struct SpawnReplySender<T> {
    sender: mpsc::SyncSender<T>,
    // Drop the channel endpoint before its storage lease.
    storage: LuaCallbackStorageLease,
}

#[allow(dead_code)] // The admitted Lua spawn caller will own this endpoint.
pub(crate) struct SpawnReplyReceiver<T> {
    receiver: mpsc::Receiver<T>,
    // Drop the channel endpoint before its storage lease.
    storage: LuaCallbackStorageLease,
}

#[allow(dead_code)] // A refused delivery must retain both the endpoint and payload.
pub(crate) struct SpawnReplyRefusal<T> {
    // Drop a refused payload before releasing the endpoint's channel lease.
    pub(crate) value: T,
    pub(crate) sender: SpawnReplySender<T>,
}

/// Return the original charge if it cannot fund channel construction.
#[allow(dead_code)] // Input admission will supply this disjoint channel charge.
pub(crate) fn spawn_reply_channel<T>(
    charge: LuaCallbackCharge,
) -> Result<(SpawnReplySender<T>, SpawnReplyReceiver<T>), LuaCallbackCharge> {
    let Some(bytes) = layout::single_reply_bytes::<T>(true) else {
        return Err(charge);
    };
    if charge.bytes() < bytes {
        return Err(charge);
    }
    let storage = LuaCallbackStorageLease::new(charge);
    let (sender, receiver) = mpsc::sync_channel(1);
    Ok((
        SpawnReplySender {
            sender,
            storage: storage.clone(),
        },
        SpawnReplyReceiver { receiver, storage },
    ))
}

#[allow(dead_code)] // Host will deliver each spawn result by value once.
impl<T> SpawnReplySender<T> {
    pub(crate) fn try_send(self, value: T) -> Result<(), SpawnReplyRefusal<T>> {
        match self.sender.try_send(value) {
            Ok(()) => Ok(()),
            // A single consuming sender cannot fill this slot before its send.
            // Preserve ownership even if the underlying channel reports Full.
            Err(mpsc::TrySendError::Full(value) | mpsc::TrySendError::Disconnected(value)) => {
                Err(SpawnReplyRefusal {
                    sender: self,
                    value,
                })
            }
        }
    }
}

#[allow(dead_code)] // The plugin worker will wait after admitted enqueue.
impl<T> SpawnReplyReceiver<T> {
    /// This wait shares the thread-local baseline noted at lua_runtime.rs:281-284.
    /// Pinned std context.rs:37-77 retains its Context Arc until thread exit.
    /// Pinned std array.rs:375-411 uses that context for a blocking receive.
    /// The channel lease does not account for this worker-lifetime storage.
    pub(crate) fn recv_timeout(&self, timeout: Duration) -> Result<T, mpsc::RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn memory() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 64 * 1024,
            total_vm_bytes: 64 * 1024,
            per_callback_bytes: 64 * 1024,
            total_callback_bytes: 64 * 1024,
        })
        .unwrap()
    }

    fn channel<T>(memory: &Arc<LuaMemoryAccount>) -> (SpawnReplySender<T>, SpawnReplyReceiver<T>) {
        let bytes = layout::single_reply_bytes::<T>(true).unwrap();
        spawn_reply_channel(memory.reserve_callback_total(bytes).unwrap()).unwrap()
    }

    #[test]
    fn insufficient_reply_charge_returns_original_reservation() {
        let memory = memory();
        let bytes = layout::single_reply_bytes::<u8>(true).unwrap();
        let charge = memory.reserve_callback_total(bytes - 1).unwrap();
        let returned = match spawn_reply_channel::<u8>(charge) {
            Err(charge) => charge,
            Ok(_) => panic!("undersized charge must refuse construction"),
        };
        assert_eq!(returned.bytes(), bytes - 1);
        assert_eq!(memory.usage().1, bytes - 1);
        drop(returned);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn reply_channel_delivers_once_and_retains_receiver_storage() {
        let memory = memory();
        let (sender, receiver) = channel::<u64>(&memory);
        let bytes = memory.usage().1;
        assert!(sender.try_send(42).is_ok());
        assert_eq!(memory.usage().1, bytes);
        assert_eq!(receiver.recv_timeout(Duration::ZERO).unwrap(), 42);
        assert!(matches!(
            receiver.recv_timeout(Duration::ZERO),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_eq!(memory.usage().1, bytes);
        drop(receiver);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn disconnected_reply_returns_payload_and_sender() {
        let memory = memory();
        let (sender, receiver) = channel::<u64>(&memory);
        let bytes = memory.usage().1;
        drop(receiver);
        let refusal = match sender.try_send(42) {
            Err(refusal) => refusal,
            Ok(()) => panic!("delivery must fail after the receiver drops"),
        };
        assert_eq!(refusal.value, 42);
        assert_eq!(memory.usage().1, bytes);
        let SpawnReplyRefusal { value, sender } = refusal;
        assert_eq!(value, 42);
        drop(sender);
        assert_eq!(memory.usage().1, 0);
    }

    struct DropProbe {
        memory: Arc<LuaMemoryAccount>,
        drops: Arc<AtomicUsize>,
        bytes: usize,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            assert_eq!(
                self.memory.usage().1,
                self.bytes,
                "payload must drop before channel storage"
            );
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reply_storage_outlives_endpoints_and_refused_payload() {
        let memory = memory();
        let drops = Arc::new(AtomicUsize::new(0));
        // Cover both endpoint orders, then queued and refused payload drops.
        for receiver_first in [false, true] {
            let (sender, receiver) = channel::<DropProbe>(&memory);
            let bytes = memory.usage().1;
            if receiver_first {
                drop(receiver);
                assert_eq!(memory.usage().1, bytes);
                drop(sender);
            } else {
                drop(sender);
                assert_eq!(memory.usage().1, bytes);
                drop(receiver);
            }
            assert_eq!(memory.usage().1, 0);
        }
        for receiver_first in [false, true] {
            let (sender, receiver) = channel::<DropProbe>(&memory);
            let payload = DropProbe {
                memory: Arc::clone(&memory),
                drops: Arc::clone(&drops),
                bytes: memory.usage().1,
            };
            if receiver_first {
                drop(receiver);
                let refusal = match sender.try_send(payload) {
                    Err(refusal) => refusal,
                    Ok(()) => panic!("receiver has already dropped"),
                };
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                drop(refusal);
            } else {
                assert!(sender.try_send(payload).is_ok());
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(receiver);
            }
            assert_eq!(memory.usage().1, 0);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}
