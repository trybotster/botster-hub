use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use botster_core::contract::terminal_adapter::TerminalRouteCloseReason;

/// What a closed adapter reports to its close hook.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CloseReport {
    /// Hub closed the adapter before Core did.
    pub host_closed: bool,
    /// Core closed the adapter first because the session's worker was lost.
    pub worker_lost: bool,
}

/// Internal adapter close cause. Host close cannot rewrite an already-closed Core close.
pub(crate) struct CloseCause {
    closed: AtomicBool,
    host_closed: AtomicBool,
    /// Core's reason for the first close, when Core closed the route first.
    core_reason: OnceLock<TerminalRouteCloseReason>,
}

impl CloseCause {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            host_closed: AtomicBool::new(false),
            core_reason: OnceLock::new(),
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

    pub(crate) fn core_reason(&self) -> Option<TerminalRouteCloseReason> {
        self.core_reason.get().copied()
    }

    pub(crate) fn report(&self) -> CloseReport {
        CloseReport {
            host_closed: self.host_closed(),
            worker_lost: self.core_reason() == Some(TerminalRouteCloseReason::WorkerLinkFailed),
        }
    }

    /// Only the first close carries the route's reason; a later close keeps it.
    pub(crate) fn mark_core_if_open(&self, reason: TerminalRouteCloseReason) {
        if !self.is_closed() {
            let _ = self.core_reason.set(reason);
        }
    }

    pub(crate) fn mark_host_if_open(&self) {
        if !self.is_closed() {
            self.host_closed.store(true, Ordering::SeqCst);
        }
    }
}
