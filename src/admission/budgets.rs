use std::time::Duration;

pub(crate) const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_INCOMPLETE_FRAME_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_MAX_CONNECTIONS: usize = 64;
pub(crate) const DAEMON_MAX_REJECTION_TASKS: usize = 8;
pub(crate) const DAEMON_CONTROL_QUEUE_CAPACITY: usize = 256;
pub(crate) const ENTITY_SUBSCRIPTION_QUEUE_CAPACITY: usize = 64;
/// Entity frames one muxed Unix connection may hold for all of its entity
/// subscriptions before the owner treats the next frame as an overflow.
pub(crate) const UNIX_CONNECTION_ENTITY_QUEUE_CAPACITY: usize = 256;
