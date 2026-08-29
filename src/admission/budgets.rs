use std::time::Duration;

pub(crate) const DAEMON_CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_INCOMPLETE_FRAME_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_MAX_FRAME_BYTES: usize = 1024 * 1024;
pub(crate) const DAEMON_MAX_CONNECTIONS: usize = 64;
pub(crate) const DAEMON_MAX_REJECTION_TASKS: usize = 8;
pub(crate) const DAEMON_CONTROL_QUEUE_CAPACITY: usize = 256;
pub(crate) const ENTITY_SUBSCRIPTION_QUEUE_CAPACITY: usize = 64;
