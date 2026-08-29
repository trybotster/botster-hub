use std::sync::atomic::{AtomicBool, Ordering};

/// Internal adapter close cause. Host close cannot rewrite an already-closed Core close.
pub(crate) struct CloseCause {
    closed: AtomicBool,
    host_closed: AtomicBool,
}

impl CloseCause {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn host_closed(&self) -> bool {
        self.host_closed.load(Ordering::SeqCst)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn mark_host_if_open(&self) {
        if !self.is_closed() {
            self.host_closed.store(true, Ordering::SeqCst);
        }
    }
}
