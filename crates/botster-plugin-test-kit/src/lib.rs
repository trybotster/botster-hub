//! Test Botster Lua plugins against the real Hub plugin runtime.
//!
//! The kit loads a plugin package into a real Hub daemon that runs without
//! transports, drives it one settled step at a time, and reads its outputs.
//! See the README for the spec API and the example test.

pub use botster_hub::plugin_test_kit::{
    DEFAULT_STEP_DEADLINE, DaemonPluginLogs, DaemonSession, EnvelopeCursor, EnvelopeId,
    EnvelopeTarget, HandlerHold, KitError, KitHub, KitOptions, OBSERVER_PACKAGE,
    RegistrySessionState, RoutedEnvelope, RoutedEnvelopeDeliveryStateResult,
    RoutedEnvelopeDrainOutcome, SessionId, SessionLifecycleRecord, SessionLifecycleState,
    TimerFired, hold_handler, session_record,
};
pub use botster_hub_client::{DaemonRequest, DaemonResponse};

pub mod spec;
