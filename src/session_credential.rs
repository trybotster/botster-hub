//! Per-session caller credential.
//!
//! Every spawn gets a fresh token in its environment. The token names its
//! session and carries a 256-bit secret: `<session_id>.<64 hex digits>`. Only
//! the SHA-256 of the secret is persisted, in the session's Core metadata, so
//! the credential survives adoption across a daemon restart and the raw token
//! exists only in the session's own environment.
//!
//! Issuance happens where a spawn request is built, before any reservation or
//! context publication, so a refusal has nothing to clean up.

use botster_core::{CoreSessionMetadata, SessionSpawnRequest, SpawnEnvironmentVariable};
use sha2::{Digest, Sha256};

/// Environment name that carries a session's caller token into its process.
pub const SESSION_TOKEN_ENVIRONMENT: &str = "BOTSTER_SESSION_TOKEN";

/// Core session metadata key for the SHA-256 of the token's secret.
pub(crate) const TOKEN_DIGEST_METADATA_KEY: &str = "botster.caller_token_sha256";

const SECRET_BYTES: usize = 32;
const HEX_SECRET_BYTES: usize = SECRET_BYTES * 2;
const HEX_DIGEST_BYTES: usize = 64;

/// The source of a credential's secret. Production uses the OS entropy
/// source; tests inject a failing or fixed source.
pub(crate) type Entropy = fn(&mut [u8]) -> Result<(), getrandom::Error>;

/// The OS entropy source (getentropy on macOS, getrandom(2) on Linux).
pub(crate) fn os_entropy(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
    getrandom::fill(bytes)
}

/// The OS entropy source refused to produce a secret, so no session is spawned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CredentialUnavailable;

impl std::fmt::Display for CredentialUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the OS entropy source could not produce a session credential")
    }
}

/// Retained bytes the credential adds to a spawn request, for a charged
/// caller: the token and its name in the environment, and the digest and its
/// key in the metadata. Each string is built at exact capacity. Vector slots
/// and map nodes are the caller's to count, because only it knows their size.
pub(crate) fn credential_string_bytes(session_id: &str) -> Option<usize> {
    token_len(session_id)?
        .checked_add(SESSION_TOKEN_ENVIRONMENT.len())?
        .checked_add(HEX_DIGEST_BYTES)?
        .checked_add(TOKEN_DIGEST_METADATA_KEY.len())
}

fn token_len(session_id: &str) -> Option<usize> {
    session_id.len().checked_add(1 + HEX_SECRET_BYTES)
}

/// Give one spawn request a fresh caller credential. The token replaces any
/// configured value of the same name, so the Hub's value is the one the
/// session sees, and its digest goes into the same request's metadata, so the
/// two cannot disagree. Nothing is changed when entropy is unavailable.
pub(crate) fn issue(
    request: &mut SessionSpawnRequest,
    metadata: &mut CoreSessionMetadata,
    entropy: Entropy,
) -> Result<(), CredentialUnavailable> {
    let mut secret = [0u8; SECRET_BYTES];
    entropy(&mut secret).map_err(|_| CredentialUnavailable)?;
    let session_id = &request.session_id.0;
    let mut token = String::with_capacity(token_len(session_id).ok_or(CredentialUnavailable)?);
    token.push_str(session_id);
    token.push('.');
    push_hex(&mut token, &secret);
    let mut digest = String::with_capacity(HEX_DIGEST_BYTES);
    push_hex(&mut digest, &Sha256::digest(secret));

    let variables = &mut request.environment.variables;
    variables.retain(|variable| variable.name != SESSION_TOKEN_ENVIRONMENT);
    variables.push(SpawnEnvironmentVariable {
        name: SESSION_TOKEN_ENVIRONMENT.to_string(),
        value: token,
    });
    metadata
        .entries
        .insert(TOKEN_DIGEST_METADATA_KEY.to_string(), digest);
    Ok(())
}

fn push_hex(text: &mut String, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
}

#[cfg(test)]
pub(crate) mod test_entropy {
    /// Fails like an unavailable OS entropy source.
    pub(crate) fn failing(_bytes: &mut [u8]) -> Result<(), getrandom::Error> {
        Err(getrandom::Error::UNSUPPORTED)
    }

    /// A fixed secret, so two materializations of one request compare equal.
    pub(crate) fn fixed(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
        bytes.fill(0x5a);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Assertion messages never format a token or a secret: a failing test
    // must not print a credential into its log.
    use super::*;
    use botster_core::{RequestId, SessionId, SpawnEnvironment, SpawnWorkingDirectory};

    fn request(session_id: &str, variables: Vec<SpawnEnvironmentVariable>) -> SessionSpawnRequest {
        SessionSpawnRequest {
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
        }
    }

    fn tokens(request: &SessionSpawnRequest) -> Vec<String> {
        request
            .environment
            .variables
            .iter()
            .filter(|variable| variable.name == SESSION_TOKEN_ENVIRONMENT)
            .map(|variable| variable.value.clone())
            .collect()
    }

    #[test]
    fn a_spawn_gets_one_token_that_names_it_and_only_its_secret_digest_is_kept() {
        let mut spawn = request(
            "session-a",
            vec![SpawnEnvironmentVariable {
                name: SESSION_TOKEN_ENVIRONMENT.to_string(),
                value: "configured-by-a-session-type".to_string(),
            }],
        );
        let mut metadata = CoreSessionMetadata::new();
        issue(&mut spawn, &mut metadata, os_entropy).expect("entropy is available");
        let tokens = tokens(&spawn);
        assert!(tokens.len() == 1, "exactly one token variable");
        let (session_id, secret) = tokens[0].rsplit_once('.').expect("token separator");
        assert!(session_id == "session-a", "the token names its session");
        assert!(secret.len() == HEX_SECRET_BYTES, "a 256-bit hex secret");
        assert!(
            secret.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "hex secret"
        );
        assert!(
            tokens[0].capacity() == tokens[0].len(),
            "exact token capacity"
        );

        let secret_bytes = (0..secret.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&secret[index..index + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let mut expected = String::new();
        push_hex(&mut expected, &Sha256::digest(&secret_bytes));
        let digest = &metadata.entries[TOKEN_DIGEST_METADATA_KEY];
        assert!(
            digest == &expected,
            "the metadata digest is sha256 of the secret"
        );
        assert!(digest.capacity() == digest.len(), "exact digest capacity");
        assert!(
            !metadata
                .entries
                .values()
                .any(|value| value.contains(secret)),
            "the raw secret must not reach persisted metadata"
        );
        assert_eq!(
            credential_string_bytes("session-a"),
            Some(
                tokens[0].len()
                    + SESSION_TOKEN_ENVIRONMENT.len()
                    + digest.len()
                    + TOKEN_DIGEST_METADATA_KEY.len()
            )
        );
    }

    #[test]
    fn every_spawn_gets_a_different_secret() {
        let mut first = request("session-a", Vec::new());
        let mut second = request("session-a", Vec::new());
        let (mut first_metadata, mut second_metadata) =
            (CoreSessionMetadata::new(), CoreSessionMetadata::new());
        issue(&mut first, &mut first_metadata, os_entropy).expect("entropy");
        issue(&mut second, &mut second_metadata, os_entropy).expect("entropy");
        assert!(
            tokens(&first) != tokens(&second),
            "two spawns got the same secret"
        );
        assert!(
            first_metadata.entries[TOKEN_DIGEST_METADATA_KEY]
                != second_metadata.entries[TOKEN_DIGEST_METADATA_KEY],
            "two spawns got the same digest"
        );
    }

    #[test]
    fn unavailable_entropy_refuses_and_changes_nothing() {
        let mut spawn = request("session-a", Vec::new());
        let mut metadata = CoreSessionMetadata::new();
        let before = (spawn.clone(), metadata.clone());
        assert_eq!(
            issue(&mut spawn, &mut metadata, test_entropy::failing),
            Err(CredentialUnavailable)
        );
        assert!(
            (spawn, metadata) == before,
            "a refused issue leaves the request as it was"
        );
    }
}
