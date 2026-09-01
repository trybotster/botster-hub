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
        self.notify.notify_one();
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn wake_between_the_last_flag_check_and_wait_stores_a_permit() {
        let wake = AdapterWake::new();
        assert!(!wake.pending.swap(false, Ordering::SeqCst));
        wake.wake();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(50), wake.notify.notified())
                .await
                .expect("a wake before waiter registration must store a permit");
        });
    }
}
