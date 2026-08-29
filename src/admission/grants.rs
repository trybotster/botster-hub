//! Grant issue, validation, origin policy, and session-key derivation.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use botster_core::AesGcmKey;
use botster_hub_client::DaemonLocalWebrtcBootstrap;
use serde_json::Value;

#[derive(Debug)]
pub(crate) struct LocalWebrtcSignalRequest {
    pub grant_id: String,
    pub grant_secret: String,
    pub origin: String,
    pub offer: Value,
}

pub(crate) const GRANT_TTL_SECONDS: u64 = 120;

#[derive(Debug)]
pub(crate) enum GrantAdmissionError {
    MissingGrant,
    ExpiredGrant,
    RedeemedGrant,
    SecretMismatch,
    OriginMismatch,
    Random(String),
    InvalidSecret(String),
}

impl fmt::Display for GrantAdmissionError {
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
            Self::Random(error) => write!(formatter, "local WebRTC random token failed: {error}"),
            Self::InvalidSecret(error) => {
                write!(formatter, "local WebRTC signaling failed: {error}")
            }
        }
    }
}

pub(crate) type GrantAdmissionResult<T> = Result<T, GrantAdmissionError>;

#[derive(Debug)]
pub(crate) struct LocalWebrtcGrant {
    pub grant_id: String,
    pub grant_secret: String,
    pub expected_origin: String,
    pub expires_at: u64,
    pub redeemed: bool,
}

impl LocalWebrtcGrant {
    pub(crate) fn validate(&self, request: &LocalWebrtcSignalRequest) -> GrantAdmissionResult<()> {
        if self.redeemed {
            return Err(GrantAdmissionError::RedeemedGrant);
        }
        if self.expires_at <= now_seconds() {
            return Err(GrantAdmissionError::ExpiredGrant);
        }
        if self.grant_secret != request.grant_secret {
            return Err(GrantAdmissionError::SecretMismatch);
        }
        if self.expected_origin != request.origin {
            return Err(GrantAdmissionError::OriginMismatch);
        }
        Ok(())
    }
}

pub(crate) struct AcceptedPeer {
    pub grant_id: String,
    pub stream_key: AesGcmKey,
}

#[derive(Default)]
pub(crate) struct GrantRegistry {
    grants: BTreeMap<String, LocalWebrtcGrant>,
}

impl GrantRegistry {
    pub(crate) fn issue_bootstrap(
        &mut self,
        package_name: &str,
        entrypoint_id: &str,
        expected_origin: &str,
    ) -> GrantAdmissionResult<DaemonLocalWebrtcBootstrap> {
        let now = now_seconds();
        self.prune_expired_grants(now);
        let grant_id = random_token("grant")?;
        let grant_secret = random_secret_token()?;
        let bootstrap = DaemonLocalWebrtcBootstrap {
            grant_id: grant_id.clone(),
            grant_secret: grant_secret.clone(),
            package_name: package_name.to_string(),
            entrypoint_id: entrypoint_id.to_string(),
            expected_origin: expected_origin.to_string(),
            expires_at: now + GRANT_TTL_SECONDS,
            signaling_transport: "daemon_request".to_string(),
            data_plane: "webrtc_data_channel".to_string(),
            ordered: true,
            max_retransmits: None,
            max_packet_lifetime_ms: None,
        };
        self.grants.insert(
            grant_id.clone(),
            LocalWebrtcGrant {
                grant_id,
                grant_secret,
                expected_origin: expected_origin.to_string(),
                expires_at: bootstrap.expires_at,
                redeemed: false,
            },
        );
        Ok(bootstrap)
    }

    pub(crate) fn redeem(
        &mut self,
        request: &LocalWebrtcSignalRequest,
    ) -> GrantAdmissionResult<AcceptedPeer> {
        let Some(grant) = self.grants.get_mut(&request.grant_id) else {
            return Err(GrantAdmissionError::MissingGrant);
        };
        grant.validate(request)?;
        grant.redeemed = true;
        let grant_id = grant.grant_id.clone();
        let stream_key = secret_stream_key(&request.grant_secret)?;
        Ok(AcceptedPeer {
            grant_id,
            stream_key,
        })
    }

    pub(crate) fn prune_expired_grants(&mut self, now: u64) {
        self.grants.retain(|_, grant| grant.expires_at > now);
    }

    pub(crate) fn clear(&mut self) {
        self.grants.clear();
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, grant_id: String, grant: LocalWebrtcGrant) {
        self.grants.insert(grant_id, grant);
    }

    #[cfg(test)]
    pub(crate) fn contains_key(&self, grant_id: &str) -> bool {
        self.grants.contains_key(grant_id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.grants.len()
    }
}

pub(crate) fn origin_from_local_url(local_url: &str) -> Option<String> {
    let scheme_end = local_url.find("://")?;
    let after_scheme = scheme_end + 3;
    let authority_end = local_url[after_scheme..]
        .find(['/', '?', '#'])
        .map(|index| after_scheme + index)
        .unwrap_or(local_url.len());
    if authority_end == after_scheme {
        return None;
    }
    Some(local_url[..authority_end].to_string())
}

fn random_token(prefix: &str) -> GrantAdmissionResult<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| GrantAdmissionError::Random(error.to_string()))?;
    Ok(format!("{prefix}-{}", hex(&bytes)))
}

fn random_secret_token() -> GrantAdmissionResult<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| GrantAdmissionError::Random(error.to_string()))?;
    Ok(format!("secret-{}", hex(&bytes)))
}

fn secret_stream_key(secret: &str) -> GrantAdmissionResult<AesGcmKey> {
    let encoded = secret.strip_prefix("secret-").ok_or_else(|| {
        GrantAdmissionError::InvalidSecret("invalid bootstrap secret".to_string())
    })?;
    let bytes = decode_hex(encoded).ok_or_else(|| {
        GrantAdmissionError::InvalidSecret("invalid bootstrap secret".to_string())
    })?;
    AesGcmKey::from_slice(&bytes)
        .map_err(|error| GrantAdmissionError::InvalidSecret(error.to_string()))
}

fn decode_hex(encoded: &str) -> Option<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut output = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issuing_bootstrap_prunes_expired_grants_and_keeps_live_replay_diagnostics() {
        let now = now_seconds();
        let mut grants = GrantRegistry::default();
        grants.insert(
            "grant-expired".to_string(),
            LocalWebrtcGrant {
                grant_id: "grant-expired".to_string(),
                grant_secret: "secret-expired".to_string(),
                expected_origin: "http://127.0.0.1:1".to_string(),
                expires_at: now.saturating_sub(1),
                redeemed: true,
            },
        );
        grants.insert(
            "grant-live-redeemed".to_string(),
            LocalWebrtcGrant {
                grant_id: "grant-live-redeemed".to_string(),
                grant_secret: "secret-live".to_string(),
                expected_origin: "http://127.0.0.1:2".to_string(),
                expires_at: now + GRANT_TTL_SECONDS,
                redeemed: true,
            },
        );

        let bootstrap = grants
            .issue_bootstrap("botster-web", "web-client", "http://127.0.0.1:41739")
            .expect("issue bootstrap");

        assert!(!grants.contains_key("grant-expired"));
        assert!(grants.contains_key("grant-live-redeemed"));
        assert!(grants.contains_key(&bootstrap.grant_id));
        assert_eq!(grants.len(), 2);
    }

    #[test]
    fn grant_validation_runs_redeemed_expiry_secret_then_origin() {
        let now = now_seconds();
        let request = LocalWebrtcSignalRequest {
            grant_id: "grant".to_string(),
            grant_secret: "secret-wrong".to_string(),
            origin: "http://wrong".to_string(),
            offer: serde_json::json!({}),
        };
        let redeemed = LocalWebrtcGrant {
            grant_id: "grant".to_string(),
            grant_secret: "secret-ok".to_string(),
            expected_origin: "http://ok".to_string(),
            expires_at: now + GRANT_TTL_SECONDS,
            redeemed: true,
        };
        assert!(matches!(
            redeemed.validate(&request),
            Err(GrantAdmissionError::RedeemedGrant)
        ));
        let expired = LocalWebrtcGrant {
            redeemed: false,
            expires_at: now.saturating_sub(1),
            ..redeemed
        };
        assert!(matches!(
            expired.validate(&request),
            Err(GrantAdmissionError::ExpiredGrant)
        ));
        let secret = LocalWebrtcGrant {
            redeemed: false,
            expires_at: now + GRANT_TTL_SECONDS,
            grant_secret: "secret-ok".to_string(),
            expected_origin: "http://ok".to_string(),
            grant_id: "grant".to_string(),
        };
        assert!(matches!(
            secret.validate(&request),
            Err(GrantAdmissionError::SecretMismatch)
        ));
        let origin = LocalWebrtcGrant {
            grant_secret: "secret-wrong".to_string(),
            expected_origin: "http://ok".to_string(),
            ..secret
        };
        assert!(matches!(
            origin.validate(&request),
            Err(GrantAdmissionError::OriginMismatch)
        ));
    }

    #[test]
    fn grant_admission_error_display_matches_local_webrtc_error() {
        assert_eq!(
            GrantAdmissionError::MissingGrant.to_string(),
            "local WebRTC bootstrap grant was not found"
        );
        assert_eq!(
            GrantAdmissionError::ExpiredGrant.to_string(),
            "local WebRTC bootstrap grant expired"
        );
        assert_eq!(
            GrantAdmissionError::RedeemedGrant.to_string(),
            "local WebRTC bootstrap grant was already redeemed"
        );
        assert_eq!(
            GrantAdmissionError::SecretMismatch.to_string(),
            "local WebRTC bootstrap grant secret mismatch"
        );
        assert_eq!(
            GrantAdmissionError::OriginMismatch.to_string(),
            "local WebRTC bootstrap origin mismatch"
        );
        assert_eq!(
            GrantAdmissionError::Random("boom".into()).to_string(),
            "local WebRTC random token failed: boom"
        );
        assert_eq!(
            GrantAdmissionError::InvalidSecret("invalid bootstrap secret".into()).to_string(),
            "local WebRTC signaling failed: invalid bootstrap secret"
        );
    }
}
