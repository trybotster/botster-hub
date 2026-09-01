//! Fair selection for Unix host-control frames.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostControlClass {
    Control,
    Event,
}

pub(crate) const MAX_HOST_FRAMES_PER_FLUSH_TURN: usize = 2;

#[must_use]
pub(crate) fn next_ready_host_control_class(
    last: Option<HostControlClass>,
    control_ready: bool,
    event_ready: bool,
) -> Option<HostControlClass> {
    match (last, control_ready, event_ready) {
        (_, false, false) => None,
        (_, true, false) => Some(HostControlClass::Control),
        (_, false, true) => Some(HostControlClass::Event),
        (Some(HostControlClass::Control), true, true) => Some(HostControlClass::Event),
        (Some(HostControlClass::Event) | None, true, true) => Some(HostControlClass::Control),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_control_and_event_alternate() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), true, true),
            Some(HostControlClass::Event)
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, true),
            Some(HostControlClass::Control)
        );
    }
}
