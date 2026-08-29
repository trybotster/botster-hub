//! Local WebRTC signaling and DataChannel adapter for installed browser packages.

use std::error::Error;
use std::fmt;

pub(crate) mod adapter;
pub(crate) mod control_channel;
pub(crate) mod delivery;
pub(crate) mod peer;
pub(crate) mod signaling;
pub(crate) mod subscription_channel;

#[cfg(test)]
pub(crate) mod test_support;

pub use peer::LocalWebrtcTransport;

pub(crate) use crate::admission::grants::LocalWebrtcSignalRequest;
pub(crate) use adapter::{WebRtcConnectionMux, WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle};
pub(crate) use peer::{
    LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_FILE, LOCAL_WEBRTC_SENDER_TERMINAL_RECORD_MAX_BYTES,
    LocalWebrtcSenderTerminalRecord,
};
pub(crate) use subscription_channel::LocalWebrtcAttachedSubscription;

pub(crate) type LocalWebrtcResult<T> = Result<T, LocalWebrtcError>;

#[derive(Debug)]
pub enum LocalWebrtcError {
    MissingGrant,
    ExpiredGrant,
    RedeemedGrant,
    SecretMismatch,
    OriginMismatch,
    InvalidOffer(String),
    Random(String),
    Webrtc(String),
}

impl fmt::Display for LocalWebrtcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingGrant => write!(formatter, "local WebRTC bootstrap grant was not found"),
            Self::ExpiredGrant => write!(formatter, "local WebRTC bootstrap grant expired"),
            Self::RedeemedGrant => write!(
                formatter,
                "local WebRTC bootstrap grant was already redeemed"
            ),
            Self::SecretMismatch => {
                write!(formatter, "local WebRTC bootstrap grant secret mismatch")
            }
            Self::OriginMismatch => write!(formatter, "local WebRTC bootstrap origin mismatch"),
            Self::InvalidOffer(error) => write!(formatter, "invalid local WebRTC offer: {error}"),
            Self::Random(error) => write!(formatter, "local WebRTC random token failed: {error}"),
            Self::Webrtc(error) => write!(formatter, "local WebRTC signaling failed: {error}"),
        }
    }
}

impl Error for LocalWebrtcError {}

impl From<crate::admission::grants::GrantAdmissionError> for LocalWebrtcError {
    fn from(error: crate::admission::grants::GrantAdmissionError) -> Self {
        use crate::admission::grants::GrantAdmissionError;
        match error {
            GrantAdmissionError::MissingGrant => Self::MissingGrant,
            GrantAdmissionError::ExpiredGrant => Self::ExpiredGrant,
            GrantAdmissionError::RedeemedGrant => Self::RedeemedGrant,
            GrantAdmissionError::SecretMismatch => Self::SecretMismatch,
            GrantAdmissionError::OriginMismatch => Self::OriginMismatch,
            GrantAdmissionError::Random(error) => Self::Random(error),
            GrantAdmissionError::InvalidSecret(error) => Self::Webrtc(error),
        }
    }
}
