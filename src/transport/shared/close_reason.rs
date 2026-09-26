use std::sync::OnceLock;

use botster_core::contract::terminal_adapter::TerminalRouteCloseReason;

/// Who closed an adapter first, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CloseSource {
    /// Hub closed the adapter.
    Host,
    /// Core closed the route, with its reason.
    Core(TerminalRouteCloseReason),
    /// The adapter was dropped without a close.
    Dropped,
}

/// Internal adapter close cause. The first close records its source; a later
/// close cannot replace it, so a host sweep cannot rewrite a Core close and a
/// later Core close cannot replace the first Core reason.
pub(crate) struct CloseCause {
    source: OnceLock<CloseSource>,
}

impl CloseCause {
    pub(crate) fn new() -> Self {
        Self {
            source: OnceLock::new(),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.source.get().is_some()
    }

    pub(crate) fn host_closed(&self) -> bool {
        self.source() == Some(CloseSource::Host)
    }

    pub(crate) fn source(&self) -> Option<CloseSource> {
        self.source.get().copied()
    }

    /// Records `source` unless the adapter is already closed.
    pub(crate) fn close(&self, source: CloseSource) {
        let _ = self.source.set(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_close_source_wins() {
        let cause = CloseCause::new();
        assert_eq!(cause.source(), None);
        cause.close(CloseSource::Core(TerminalRouteCloseReason::Replaced));
        cause.close(CloseSource::Core(TerminalRouteCloseReason::Shutdown));
        cause.close(CloseSource::Host);
        cause.close(CloseSource::Dropped);
        assert_eq!(
            cause.source(),
            Some(CloseSource::Core(TerminalRouteCloseReason::Replaced))
        );
        assert!(!cause.host_closed());

        let hosted = CloseCause::new();
        hosted.close(CloseSource::Host);
        hosted.close(CloseSource::Core(TerminalRouteCloseReason::Detached));
        assert!(hosted.host_closed());
    }
}
