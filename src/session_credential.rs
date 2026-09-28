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

/// Environment name that carries a session's bearer token into its process.
/// The agent presents it as `Authorization: Bearer ${BOTSTER_MCP_TOKEN}`.
pub const MCP_TOKEN_ENVIRONMENT: &str = "BOTSTER_MCP_TOKEN";

/// Environment name that carries the daemon's MCP endpoint into a session.
/// It is issued with the token, in the same place, so a restarted session
/// gets both fresh.
pub const MCP_URL_ENVIRONMENT: &str = "BOTSTER_MCP_URL";

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
/// caller: the token and its name in the environment, the URL and its name
/// when there is one, and the digest and its key in the metadata. Each string is built at exact capacity. Vector slots
/// and map nodes are the caller's to count, because only it knows their size.
pub(crate) fn credential_string_bytes(session_id: &str, mcp_url: Option<&str>) -> Option<usize> {
    token_len(session_id)?
        .checked_add(MCP_TOKEN_ENVIRONMENT.len())?
        .checked_add(HEX_DIGEST_BYTES)?
        .checked_add(TOKEN_DIGEST_METADATA_KEY.len())?
        .checked_add(mcp_url.map_or(Some(0), |url| {
            url.len().checked_add(MCP_URL_ENVIRONMENT.len())
        })?)
}

/// Environment variables the credential adds: the token, and the URL when
/// the daemon serves MCP.
pub(crate) fn credential_variable_slots(mcp_url: Option<&str>) -> usize {
    1 + usize::from(mcp_url.is_some())
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
    mcp_url: Option<&str>,
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
    variables.retain(|variable| {
        variable.name != MCP_TOKEN_ENVIRONMENT && variable.name != MCP_URL_ENVIRONMENT
    });
    variables.push(SpawnEnvironmentVariable {
        name: MCP_TOKEN_ENVIRONMENT.to_string(),
        value: token,
    });
    if let Some(url) = mcp_url {
        variables.push(SpawnEnvironmentVariable {
            name: MCP_URL_ENVIRONMENT.to_string(),
            value: url.to_string(),
        });
    }
    metadata
        .entries
        .insert(TOKEN_DIGEST_METADATA_KEY.to_string(), digest);
    Ok(())
}

/// A presented caller token, reduced to what verification needs: the session
/// it names and the SHA-256 of its secret. The secret itself is never kept,
/// and `Debug` prints nothing of the token.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CallerToken {
    session_id: String,
    secret_digest: [u8; 32],
}

impl std::fmt::Debug for CallerToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CallerToken(..)")
    }
}

impl CallerToken {
    /// Parse `<session_id>.<64 lowercase hex digits>`, splitting at the LAST
    /// '.'. Anything else is `None`: no separator, an empty or unprintable
    /// session ID, or a secret of the wrong length or alphabet. No Core state
    /// is consulted, so junk never reaches Core.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (session_id, secret) = text.rsplit_once('.')?;
        if session_id.is_empty() || !session_id.bytes().all(|byte| byte.is_ascii_graphic()) {
            return None;
        }
        if secret.len() != HEX_SECRET_BYTES {
            return None;
        }
        let mut bytes = [0u8; SECRET_BYTES];
        for (slot, pair) in bytes.iter_mut().zip(secret.as_bytes().chunks_exact(2)) {
            *slot = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
        }
        Some(Self {
            session_id: session_id.to_string(),
            secret_digest: Sha256::digest(bytes).into(),
        })
    }

    /// The session the token names. This is a claim until `matches` agrees.
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    /// True when the stored digest (`botster.caller_token_sha256`, lowercase
    /// hex) is the digest of this token's secret. The comparison looks at
    /// every byte whatever the stored value is.
    pub(crate) fn matches(&self, stored_digest_hex: &str) -> bool {
        let stored = stored_digest_hex.as_bytes();
        let mut expected = [0u8; HEX_DIGEST_BYTES];
        for (index, byte) in self.secret_digest.iter().enumerate() {
            expected[index * 2] = hex_digit(byte >> 4);
            expected[index * 2 + 1] = hex_digit(byte & 0x0f);
        }
        let mut difference = u8::from(stored.len() != expected.len());
        for (index, expected_byte) in expected.iter().enumerate() {
            difference |= stored.get(index).copied().unwrap_or(0) ^ expected_byte;
        }
        difference == 0
    }
}

fn hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

fn hex_digit(nibble: u8) -> u8 {
    b"0123456789abcdef"[usize::from(nibble)]
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
            .filter(|variable| variable.name == MCP_TOKEN_ENVIRONMENT)
            .map(|variable| variable.value.clone())
            .collect()
    }

    #[test]
    fn a_spawn_gets_one_token_that_names_it_and_only_its_secret_digest_is_kept() {
        let mut spawn = request(
            "session-a",
            vec![SpawnEnvironmentVariable {
                name: MCP_TOKEN_ENVIRONMENT.to_string(),
                value: "configured-by-a-session-type".to_string(),
            }],
        );
        let mut metadata = CoreSessionMetadata::new();
        issue(&mut spawn, &mut metadata, None, os_entropy).expect("entropy is available");
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
                    + MCP_TOKEN_ENVIRONMENT.len()
                    + digest.len()
                    + TOKEN_DIGEST_METADATA_KEY.len()
            )
        );
    }

    /// Issue a credential with the fixed test secret and return the token.
    fn issued_token(session_id: &str) -> (String, String) {
        let mut spawn = request(session_id, Vec::new());
        let mut metadata = CoreSessionMetadata::default();
        issue(&mut spawn, &mut metadata, None, test_entropy::fixed).unwrap();
        (
            spawn.environment.variables.remove(0).value,
            metadata.entries.remove(TOKEN_DIGEST_METADATA_KEY).unwrap(),
        )
    }

    #[test]
    fn a_caller_token_matches_only_its_own_digest() {
        let (token, digest) = issued_token("sess-1");
        let parsed = CallerToken::parse(&token).expect("an issued token parses");
        assert!(parsed.session_id() == "sess-1");
        assert!(parsed.matches(&digest));
        // One digit wrong, a truncated digest, and an empty digest all refuse.
        let mut wrong = digest.clone();
        wrong.replace_range(63.., if digest.ends_with('0') { "1" } else { "0" });
        assert!(!parsed.matches(&wrong));
        assert!(!parsed.matches(&digest[..63]));
        assert!(!parsed.matches(""));
    }

    #[test]
    fn a_session_id_with_dots_splits_at_the_last_dot() {
        let (token, digest) = issued_token("sess.with.dots");
        let parsed = CallerToken::parse(&token).unwrap();
        assert!(parsed.session_id() == "sess.with.dots");
        assert!(parsed.matches(&digest));
    }

    #[test]
    fn malformed_tokens_do_not_parse() {
        let secret = "5a".repeat(32);
        let short = "5a".repeat(31);
        let upper = "5A".repeat(32);
        let not_hex = format!("{}zz", "5a".repeat(31));
        let cases = [
            String::new(),
            "no-separator".to_string(),
            format!(".{secret}"),
            format!("sess {secret}"),
            format!("sess.{short}"),
            format!("sess.{secret}00"),
            format!("sess.{upper}"),
            format!("sess.{not_hex}"),
            format!("sess\u{7f}.{secret}"),
            format!("sess-\u{e9}.{secret}"),
        ];
        for (index, case) in cases.iter().enumerate() {
            assert!(CallerToken::parse(case).is_none(), "case {index} parsed");
        }
    }

    #[test]
    fn a_caller_token_never_prints() {
        let (token, _) = issued_token("sess-1");
        let parsed = CallerToken::parse(&token).unwrap();
        let shown = format!("{parsed:?}");
        assert!(!shown.contains("5a5a"));
        assert!(!shown.contains("sess-1"));
    }

    #[test]
    fn the_endpoint_url_is_issued_with_the_token_and_replaces_a_configured_one() {
        let stale = vec![SpawnEnvironmentVariable {
            name: MCP_URL_ENVIRONMENT.to_string(),
            value: "http://stale.invalid/mcp".to_string(),
        }];
        let mut spawn = request("sess-1", stale);
        let mut metadata = CoreSessionMetadata::default();
        let url = "http://127.0.0.1:47001/mcp";
        issue(&mut spawn, &mut metadata, Some(url), test_entropy::fixed).unwrap();
        let urls: Vec<_> = spawn
            .environment
            .variables
            .iter()
            .filter(|variable| variable.name == MCP_URL_ENVIRONMENT)
            .collect();
        assert!(urls.len() == 1 && urls[0].value == url);
        assert!(
            spawn
                .environment
                .variables
                .iter()
                .filter(|variable| variable.name == MCP_TOKEN_ENVIRONMENT)
                .count()
                == 1
        );
        assert_eq!(credential_variable_slots(Some(url)), 2);
        assert_eq!(credential_variable_slots(None), 1);
    }

    #[test]
    fn every_spawn_gets_a_different_secret() {
        let mut first = request("session-a", Vec::new());
        let mut second = request("session-a", Vec::new());
        let (mut first_metadata, mut second_metadata) =
            (CoreSessionMetadata::new(), CoreSessionMetadata::new());
        issue(&mut first, &mut first_metadata, None, os_entropy).expect("entropy");
        issue(&mut second, &mut second_metadata, None, os_entropy).expect("entropy");
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
            issue(&mut spawn, &mut metadata, None, test_entropy::failing),
            Err(CredentialUnavailable)
        );
        assert!(
            (spawn, metadata) == before,
            "a refused issue leaves the request as it was"
        );
    }
}
