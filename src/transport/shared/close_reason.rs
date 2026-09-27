use std::sync::atomic::{AtomicU8, Ordering};

use botster_core::contract::terminal_adapter::TerminalRouteCloseReason;

/// What a closed adapter reports to its close hook.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CloseReport {
    /// Hub closed the adapter before Core did.
    pub host_closed: bool,
    /// Core closed the adapter first because the session's worker was lost.
    pub worker_lost: bool,
}

const OPEN: u8 = 0;
const HOST: u8 = 1;
const CORE_UNSPECIFIED: u8 = 2;
const CORE_REASON_BASE: u8 = 3;

/// Internal adapter close cause: one state, set once by the first close.
///
/// A Core close (with or without its reason) and a Host close race through
/// independent handles that share this slot. The first close commits with a
/// single compare-exchange from open, so the closed flag, the host flag and
/// Core's reason always describe the same close, and a later close changes
/// nothing.
pub(crate) struct CloseCause {
    state: AtomicU8,
    #[cfg(test)]
    commit_seam: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl CloseCause {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(OPEN),
            #[cfg(test)]
            commit_seam: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) != OPEN
    }

    pub(crate) fn host_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == HOST
    }

    pub(crate) fn core_reason(&self) -> Option<TerminalRouteCloseReason> {
        decode_core_reason(self.state.load(Ordering::Acquire))
    }

    pub(crate) fn report(&self) -> CloseReport {
        let state = self.state.load(Ordering::Acquire);
        CloseReport {
            host_closed: state == HOST,
            worker_lost: decode_core_reason(state)
                == Some(TerminalRouteCloseReason::WorkerLinkFailed),
        }
    }

    /// A close with no stated cause (drop, handle close): Core's close.
    pub(crate) fn close(&self) {
        self.commit_first(CORE_UNSPECIFIED);
    }

    pub(crate) fn close_from_host(&self) {
        self.commit_first(HOST);
    }

    pub(crate) fn close_from_core(&self, reason: TerminalRouteCloseReason) {
        self.commit_first(encode_core_reason(reason));
    }

    fn commit_first(&self, cause: u8) {
        if self.state.load(Ordering::Acquire) != OPEN {
            return;
        }
        #[cfg(test)]
        self.run_commit_seam();
        // A close that committed after the observation above wins here.
        let _ = self
            .state
            .compare_exchange(OPEN, cause, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Run `seam` once, between the next close's open observation and its
    /// commit, so a test can place a competing close in that window.
    #[cfg(test)]
    pub(crate) fn set_commit_seam(&self, seam: impl FnOnce() + Send + 'static) {
        *self.commit_seam.lock().unwrap() = Some(Box::new(seam));
    }

    #[cfg(test)]
    fn run_commit_seam(&self) {
        let seam = self.commit_seam.lock().unwrap().take();
        if let Some(seam) = seam {
            seam();
        }
    }
}

fn encode_core_reason(reason: TerminalRouteCloseReason) -> u8 {
    use TerminalRouteCloseReason as R;
    CORE_REASON_BASE
        + match reason {
            R::Replaced => 0,
            R::Detached => 1,
            R::SessionEnded => 2,
            R::WorkerLinkFailed => 3,
            R::AdapterClosed => 4,
            R::TerminalDelivered => 5,
            R::Stalled => 6,
            R::Overflowed => 7,
            R::InputFailed => 8,
            R::Failed => 9,
            R::Shutdown => 10,
            R::BindRejected => 11,
        }
}

fn decode_core_reason(state: u8) -> Option<TerminalRouteCloseReason> {
    use TerminalRouteCloseReason as R;
    Some(match state.checked_sub(CORE_REASON_BASE)? {
        0 => R::Replaced,
        1 => R::Detached,
        2 => R::SessionEnded,
        3 => R::WorkerLinkFailed,
        4 => R::AdapterClosed,
        5 => R::TerminalDelivered,
        6 => R::Stalled,
        7 => R::Overflowed,
        8 => R::InputFailed,
        9 => R::Failed,
        10 => R::Shutdown,
        11 => R::BindRejected,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_REASONS: [TerminalRouteCloseReason; 12] = [
        TerminalRouteCloseReason::Replaced,
        TerminalRouteCloseReason::Detached,
        TerminalRouteCloseReason::SessionEnded,
        TerminalRouteCloseReason::WorkerLinkFailed,
        TerminalRouteCloseReason::AdapterClosed,
        TerminalRouteCloseReason::TerminalDelivered,
        TerminalRouteCloseReason::Stalled,
        TerminalRouteCloseReason::Overflowed,
        TerminalRouteCloseReason::InputFailed,
        TerminalRouteCloseReason::Failed,
        TerminalRouteCloseReason::Shutdown,
        TerminalRouteCloseReason::BindRejected,
    ];

    #[test]
    fn every_core_reason_round_trips_through_the_state() {
        for reason in ALL_REASONS {
            let cause = CloseCause::new();
            cause.close_from_core(reason);
            assert_eq!(cause.core_reason(), Some(reason));
            assert!(cause.is_closed() && !cause.host_closed());
        }
    }

    #[test]
    fn a_host_close_inside_a_core_close_window_wins_whole() {
        let cause = std::sync::Arc::new(CloseCause::new());
        let host = std::sync::Arc::clone(&cause);
        cause.set_commit_seam(move || host.close_from_host());
        cause.close_from_core(TerminalRouteCloseReason::WorkerLinkFailed);
        assert!(cause.host_closed());
        assert_eq!(cause.core_reason(), None);
        assert_eq!(
            cause.report(),
            CloseReport {
                host_closed: true,
                worker_lost: false
            }
        );
    }

    #[test]
    fn a_core_close_inside_a_host_close_window_wins_whole() {
        let cause = std::sync::Arc::new(CloseCause::new());
        let core = std::sync::Arc::clone(&cause);
        cause.set_commit_seam(move || {
            core.close_from_core(TerminalRouteCloseReason::WorkerLinkFailed)
        });
        cause.close_from_host();
        assert!(!cause.host_closed());
        assert_eq!(
            cause.core_reason(),
            Some(TerminalRouteCloseReason::WorkerLinkFailed)
        );
        assert_eq!(
            cause.report(),
            CloseReport {
                host_closed: false,
                worker_lost: true
            }
        );
    }
}
