//! Hub-owned runtime facade over the default `botster-core` engine.
//!
//! The hub owns explicit configuration and admission policy. Session process
//! mechanics, terminal byte routing, activity accounting, and shutdown stay in
//! `botster-core` through `DefaultBotsterEngine`.

use botster_core::{
    BotsterEngineObservation, BotsterEngineOutput, BotsterSpawnOutcome, ClientId,
    CoreSessionMetadata, DefaultBotsterEngine, DefaultBotsterEngineError, SessionActivityStatus,
    SessionId, SessionSpawnRequest, SubscriptionId,
};

use crate::config::HubConfig;

/// Hub runtime skeleton that embeds the default local core engine.
pub struct HubRuntime {
    config: HubConfig,
    engine: DefaultBotsterEngine,
}

impl HubRuntime {
    /// Build a hub runtime from explicit, already-validated hub config.
    #[must_use]
    pub fn new(config: HubConfig) -> Self {
        Self {
            config,
            engine: DefaultBotsterEngine::new(),
        }
    }

    /// Return the policy-resolved hub config that created this runtime.
    #[must_use]
    pub const fn config(&self) -> &HubConfig {
        &self.config
    }

    /// Return a recorded core session.
    #[must_use]
    pub fn session(&self, session_id: &SessionId) -> Option<&botster_core::CoreSession> {
        self.engine.session(session_id)
    }

    /// Spawn a local PTY-backed session through core from a host-owned request.
    pub fn spawn_session(
        &mut self,
        request: SessionSpawnRequest,
        metadata: CoreSessionMetadata,
    ) -> Result<BotsterSpawnOutcome, DefaultBotsterEngineError> {
        self.engine.spawn_session(request, metadata)
    }

    /// Attach a client subscription to a session through core.
    pub fn attach_client(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        subscription_id: SubscriptionId,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .attach_client(client_id, session_id, subscription_id, now_seconds)
    }

    /// Write terminal bytes into a session through core.
    pub fn write_bytes(
        &mut self,
        client_id: ClientId,
        session_id: SessionId,
        data: impl Into<Vec<u8>>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .write_bytes(client_id, session_id, data, now_seconds)
    }

    /// Drain available local runtime output through core's subscription path.
    pub fn drain_runtime_once(
        &mut self,
        session_id: &SessionId,
        last_output_at: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine.drain_runtime_once(session_id, last_output_at)
    }

    /// Classify one session's activity through core.
    pub fn classify_activity(
        &self,
        session_id: &SessionId,
        now_seconds: u64,
        active_threshold_seconds: u64,
    ) -> Result<SessionActivityStatus, DefaultBotsterEngineError> {
        self.engine
            .classify_activity(session_id, now_seconds, active_threshold_seconds)
    }

    /// Shut down one local PTY-backed session through core.
    pub fn shutdown_session(
        &mut self,
        session_id: SessionId,
        reason: impl Into<String>,
        now_seconds: u64,
    ) -> Result<BotsterEngineOutput, DefaultBotsterEngineError> {
        self.engine
            .shutdown_session(session_id, reason, now_seconds)
    }
}

/// Observation type emitted by the embedded core engine.
pub type HubRuntimeObservation = BotsterEngineObservation;

/// Output batch emitted by one hub runtime operation.
pub type HubRuntimeOutput = BotsterEngineOutput;

/// Spawn result emitted by the embedded core engine.
pub type HubRuntimeSpawnOutcome = BotsterSpawnOutcome;

/// Error emitted by the embedded default local core engine.
pub type HubRuntimeError = DefaultBotsterEngineError;
