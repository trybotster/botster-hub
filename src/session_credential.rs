//! Per-session caller credential.
//!
//! Every spawn gets a fresh token in its environment. The token names its
//! session and carries a 256-bit secret: `<session_id>.<64 hex digits>`. Only
//! the SHA-256 of the secret is persisted, in the session's Core metadata, so
//! the credential survives adoption across a daemon restart and the raw token
//! exists only in the session's own environment.

use botster_core::SpawnEnvironmentVariable;
use botster_core_daemon::SpawnSessionRequest;
use sha2::{Digest, Sha256};

/// Environment name that carries a session's caller token into its process.
pub const SESSION_TOKEN_ENVIRONMENT: &str = "BOTSTER_SESSION_TOKEN";

/// Core session metadata key for the SHA-256 of the token's secret.
pub(crate) const TOKEN_DIGEST_METADATA_KEY: &str = "botster.caller_token_sha256";

const SECRET_BYTES: usize = 32;

/// Give one spawn request a fresh caller credential. The token is set after
/// any inherited or configured value of the same name, so the Hub's value is
/// the one the session sees, and its digest is written to the same request's
/// metadata, so the two can never disagree.
pub(crate) fn credentialed(mut spawn: SpawnSessionRequest) -> SpawnSessionRequest {
    let mut secret = [0u8; SECRET_BYTES];
    // getrandom reads the OS entropy source (getentropy on macOS, getrandom(2)
    // on Linux). Neither fails for a 32-byte request on a supported platform,
    // so a failure is a platform fault, not a spawn error.
    getrandom::fill(&mut secret).expect("the OS entropy source must be available");
    let token = format!("{}.{}", spawn.request.session_id.0, hex(&secret));
    let digest = hex(&Sha256::digest(secret));

    let variables = &mut spawn.request.environment.variables;
    variables.retain(|variable| variable.name != SESSION_TOKEN_ENVIRONMENT);
    variables.push(SpawnEnvironmentVariable {
        name: SESSION_TOKEN_ENVIRONMENT.to_string(),
        value: token,
    });
    spawn
        .metadata
        .entries
        .insert(TOKEN_DIGEST_METADATA_KEY.to_string(), digest);
    spawn
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::{
        CoreSessionMetadata, RequestId, SessionId, SessionSpawnRequest, SpawnEnvironment,
        SpawnWorkingDirectory,
    };

    fn spawn(session_id: &str, variables: Vec<SpawnEnvironmentVariable>) -> SpawnSessionRequest {
        SpawnSessionRequest {
            request: SessionSpawnRequest {
                request_id: RequestId("credential-test".to_string()),
                session_id: SessionId(session_id.to_string()),
                executable: "/bin/sh".to_string(),
                arguments: Vec::new(),
                working_directory: SpawnWorkingDirectory {
                    path: ".".to_string(),
                },
                environment: SpawnEnvironment {
                    variables,
                    unset: Vec::new(),
                },
                initial_pty_size: None,
            },
            metadata: CoreSessionMetadata::new(),
        }
    }

    fn token_of(spawn: &SpawnSessionRequest) -> Vec<&str> {
        spawn
            .request
            .environment
            .variables
            .iter()
            .filter(|variable| variable.name == SESSION_TOKEN_ENVIRONMENT)
            .map(|variable| variable.value.as_str())
            .collect()
    }

    #[test]
    fn a_spawn_gets_one_token_that_names_it_and_only_its_secret_digest_is_kept() {
        let issued = credentialed(spawn(
            "session-a",
            vec![SpawnEnvironmentVariable {
                name: SESSION_TOKEN_ENVIRONMENT.to_string(),
                value: "configured-by-a-session-type".to_string(),
            }],
        ));
        let tokens = token_of(&issued);
        assert_eq!(tokens.len(), 1, "exactly one token: {tokens:?}");
        let (session_id, secret) = tokens[0].rsplit_once('.').expect("token separator");
        assert_eq!(session_id, "session-a");
        assert_eq!(secret.len(), SECRET_BYTES * 2);
        assert!(secret.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let digest = &issued.metadata.entries[TOKEN_DIGEST_METADATA_KEY];
        let secret_bytes = (0..secret.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&secret[index..index + 2], 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(digest, &hex(&Sha256::digest(&secret_bytes)));
        assert!(
            !issued
                .metadata
                .entries
                .values()
                .any(|value| value.contains(secret)),
            "the raw secret must not reach persisted metadata"
        );
    }

    #[test]
    fn every_spawn_gets_a_different_secret() {
        let first = credentialed(spawn("session-a", Vec::new()));
        let second = credentialed(spawn("session-a", Vec::new()));
        assert_ne!(token_of(&first), token_of(&second));
        assert_ne!(
            first.metadata.entries[TOKEN_DIGEST_METADATA_KEY],
            second.metadata.entries[TOKEN_DIGEST_METADATA_KEY]
        );
    }
}
