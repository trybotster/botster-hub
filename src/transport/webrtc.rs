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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    fn webrtc_sources() -> Vec<(String, String)> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut files = vec![(
            "src/transport/webrtc.rs".to_string(),
            fs::read_to_string(manifest.join("src/transport/webrtc.rs")).expect("webrtc.rs"),
        )];
        let dir = manifest.join("src/transport/webrtc");
        for entry in fs::read_dir(&dir).expect("read webrtc dir") {
            let path = entry.expect("entry").path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().expect("name").to_string_lossy();
            if name == "test_support.rs" {
                continue;
            }
            let relative = format!("src/transport/webrtc/{name}");
            let source = fs::read_to_string(&path).expect("read webrtc source");
            files.push((relative, source));
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    fn production(source: &str) -> &str {
        source.split("mod tests").next().unwrap_or(source)
    }

    #[test]
    fn webrtc_state_machines_have_one_owner_file() {
        const OWNERS: &[(&str, &str)] = &[
            (
                "struct LocalWebrtcTransport",
                "src/transport/webrtc/peer.rs",
            ),
            (
                "struct LocalWebrtcPeerState",
                "src/transport/webrtc/peer.rs",
            ),
            (
                "struct LocalWebrtcFlowControl",
                "src/transport/webrtc/control_channel.rs",
            ),
            (
                "struct WebRtcConnectionMux",
                "src/transport/webrtc/adapter.rs",
            ),
            (
                "struct WebRtcTerminalAdapter",
                "src/transport/webrtc/adapter.rs",
            ),
            (
                "enum PendingLocalWebrtcRequest",
                "src/transport/webrtc/control_channel.rs",
            ),
            (
                "struct LocalWebrtcAttachedSubscription",
                "src/transport/webrtc/subscription_channel.rs",
            ),
        ];
        let files = webrtc_sources();
        for (decl, owner) in OWNERS {
            let present: Vec<&str> = files
                .iter()
                .filter(|(_, source)| production(source).contains(decl))
                .map(|(path, _)| path.as_str())
                .collect();
            assert_eq!(
                present.as_slice(),
                &[*owner],
                "{decl} must be declared only in {owner}, found {present:?}"
            );
            for (path, source) in &files {
                if path == owner {
                    continue;
                }
                assert!(
                    !production(source).contains(decl),
                    "{decl} must be absent from {path}"
                );
            }
        }
    }

    #[test]
    fn webrtc_transport_does_not_declare_admission_or_unix_policy_types() {
        const FORBIDDEN_DECLS: &[&str] = &[
            "struct GrantRegistry",
            "enum GrantRegistry",
            "struct UnixConnectionMux",
            "struct UnixTerminalAdmission",
            "struct ClosedEventRoute",
            "trait ClosedHandle",
        ];
        for (path, source) in webrtc_sources() {
            let production = production(&source);
            for forbidden in FORBIDDEN_DECLS {
                assert!(
                    !production.contains(forbidden),
                    "{path} production source must not declare {forbidden}"
                );
            }
        }
    }
}
