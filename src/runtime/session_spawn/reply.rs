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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // The delivery continuation will classify send refusal.
pub(crate) enum SpawnReplyRefusalKind {
    Full,
    Disconnected,
}

#[allow(dead_code)] // A refused delivery must retain both the endpoint and payload.
pub(crate) struct SpawnReplyRefusal<T> {
    // Drop a refused payload before releasing the endpoint's channel lease.
    pub(crate) value: T,
    pub(crate) sender: SpawnReplySender<T>,
    pub(crate) kind: SpawnReplyRefusalKind,
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
            Err(mpsc::TrySendError::Full(value)) => Err(SpawnReplyRefusal {
                sender: self,
                value,
                kind: SpawnReplyRefusalKind::Full,
            }),
            Err(mpsc::TrySendError::Disconnected(value)) => Err(SpawnReplyRefusal {
                sender: self,
                value,
                kind: SpawnReplyRefusalKind::Disconnected,
            }),
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
