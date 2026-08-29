//! Fair selection among already-admitted host-control frames.
//!
//! The scheduler inspects ready control, entity, and event classes. It never
//! waits for a future slot. Terminal adapter frames are not part of this choice.

/// Already-admitted host-control write class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostControlClass {
    Control,
    Entity,
    Event,
}

/// Host writers emit at most this many already-admitted frames per flush turn.
///
/// Three is one visit to each ready class. The bound returns the connection
/// loop to poll new control input instead of draining a continuous event
/// ready-set.
pub(crate) const MAX_HOST_FRAMES_PER_FLUSH_TURN: usize = 3;

const CLASS_ORDER: [HostControlClass; 3] = [
    HostControlClass::Control,
    HostControlClass::Entity,
    HostControlClass::Event,
];

/// Pick the next ready class after `last`, or the first ready class.
///
/// Empty classes are skipped. The function returns `None` when no class is ready.
#[must_use]
pub(crate) fn next_ready_host_control_class(
    last: Option<HostControlClass>,
    control_ready: bool,
    entity_ready: bool,
    event_ready: bool,
) -> Option<HostControlClass> {
    let start = match last {
        Some(last) => class_index(last).wrapping_add(1) % CLASS_ORDER.len(),
        None => 0,
    };
    for offset in 0..CLASS_ORDER.len() {
        let class = CLASS_ORDER[(start + offset) % CLASS_ORDER.len()];
        if class_ready(class, control_ready, entity_ready, event_ready) {
            return Some(class);
        }
    }
    None
}

fn class_index(class: HostControlClass) -> usize {
    match class {
        HostControlClass::Control => 0,
        HostControlClass::Entity => 1,
        HostControlClass::Event => 2,
    }
}

fn class_ready(
    class: HostControlClass,
    control_ready: bool,
    entity_ready: bool,
    event_ready: bool,
) -> bool {
    match class {
        HostControlClass::Control => control_ready,
        HostControlClass::Entity => entity_ready,
        HostControlClass::Event => event_ready,
    }
}

#[cfg(test)]
mod tests {
    use super::{HostControlClass, next_ready_host_control_class};

    #[test]
    fn ready_control_and_event_alternate_and_never_wait() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), true, false, true),
            Some(HostControlClass::Event)
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, false, true),
            Some(HostControlClass::Control)
        );
    }

    #[test]
    fn only_control_writes_immediately() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, false, false),
            Some(HostControlClass::Control)
        );
        assert_eq!(
            next_ready_host_control_class(None, true, false, false),
            Some(HostControlClass::Control)
        );
    }

    #[test]
    fn only_event_writes_immediately() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), false, false, true),
            Some(HostControlClass::Event)
        );
    }

    #[test]
    fn empty_ready_set_returns_none() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Entity), false, false, false),
            None
        );
    }

    #[test]
    fn three_ready_classes_round_robin() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), true, true, true),
            Some(HostControlClass::Entity)
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Entity), true, true, true),
            Some(HostControlClass::Event)
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, true, true),
            Some(HostControlClass::Control)
        );
    }

    #[test]
    fn fair_write_class_coverage_per_transport() {
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), true, true, true),
            Some(HostControlClass::Entity),
            "WebRTC rotates Control, Entity, Event"
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Entity), true, true, true),
            Some(HostControlClass::Event),
            "WebRTC rotates Control, Entity, Event"
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, true, true),
            Some(HostControlClass::Control),
            "WebRTC rotates Control, Entity, Event"
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Control), true, false, true),
            Some(HostControlClass::Event),
            "Unix rotates Control and Event with Entity inactive"
        );
        assert_eq!(
            next_ready_host_control_class(Some(HostControlClass::Event), true, false, true),
            Some(HostControlClass::Control),
            "Unix rotates Control and Event with Entity inactive"
        );
        let unix = include_str!("transport/unix/mux_write.rs");
        assert!(
            unix.contains("            false,\n            event_ready,"),
            "Unix call site must pass entity_ready=false"
        );
        let webrtc = include_str!("transport/webrtc/control_channel.rs");
        assert!(
            webrtc.contains("            entity_ready,\n            host_event_ready(peer_state),"),
            "WebRTC call site must pass live entity_ready"
        );
    }
}
