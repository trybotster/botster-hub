use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// Narrow wake sink used by the shared adapter slot.
pub(crate) trait WakeSink: Clone + Send + Sync {
    fn wake(&self);
}

/// Wake that stores a permit so a write cannot be lost before the sender waits.
#[derive(Clone)]
pub(crate) struct AdapterWake {
    notify: Arc<Notify>,
    pending: Arc<AtomicBool>,
}

impl AdapterWake {
    pub(crate) fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn wake(&self) {
        self.pending.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait(&self) {
        loop {
            if self.pending.swap(false, Ordering::SeqCst) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if self.pending.swap(false, Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

impl WakeSink for AdapterWake {
    fn wake(&self) {
        AdapterWake::wake(self);
    }
}

/// Unix flavor: notify waiters with no stored permit.
#[derive(Clone)]
pub(crate) struct NotifyWaiters(Arc<Notify>);

impl NotifyWaiters {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    pub(crate) fn from_arc(notify: Arc<Notify>) -> Self {
        Self(notify)
    }
}

impl WakeSink for NotifyWaiters {
    fn wake(&self) {
        self.0.notify_waiters();
    }
}
