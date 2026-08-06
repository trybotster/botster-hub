//! Network coordinate policy and fetching.
//!
//! The manifest introduces artifact URLs, which are network coordinates in
//! their own right, so the HTTPS rule applies to **every** coordinate the
//! installer fetches and not only the outer document.
//!
//! **Redirects are not followed.** A followed redirect could downgrade to
//! plaintext or cross origins after validation has already passed, so a
//! redirect response is an error rather than a hop.
//!
//! Same-origin with the metadata document is deliberately **not** required.
//! Forbidding a separate artifact host would rule out an ordinary CDN layout
//! for no security gain: each URL is validated on its own, and every artifact
//! is checksum-verified against the signed manifest regardless of where it came
//! from.

use std::time::Duration;

use botster_hub_installation::validate_release_source;

use crate::error::{InstallerError, InstallerResult};

const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Sanity cap on a declared artifact size, so a hostile manifest cannot ask the
/// installer to buffer an unbounded download.
pub const MAX_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024;

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_global(Some(FETCH_TIMEOUT))
        .timeout_connect(Some(FETCH_TIMEOUT))
        .timeout_recv_response(Some(FETCH_TIMEOUT))
        .timeout_recv_body(Some(FETCH_TIMEOUT))
        .build()
        .into()
}

/// Fetch a URL after validating it under the release-source policy.
pub fn fetch(url: &str, limit: u64, subject: &str) -> InstallerResult<Vec<u8>> {
    validate_release_source(url)?;
    let request = ureq::http::Request::builder()
        .method(ureq::http::Method::GET)
        .uri(url)
        .body(Vec::new())
        .map_err(|_| {
            InstallerError::new(
                "invalid_release_source",
                format!("{subject} URL is invalid"),
            )
        })?;
    let mut response = agent().run(request).map_err(|error| {
        InstallerError::new(
            "release_source_unreachable",
            format!("{subject} could not be fetched: {error}"),
        )
    })?;
    let status = response.status();
    if status.is_redirection() {
        return Err(InstallerError::new(
            "release_redirect_refused",
            format!(
                "{subject} answered {status}; redirects are not followed because a redirect can downgrade to plaintext or cross origins after validation"
            ),
        ));
    }
    if !status.is_success() {
        return Err(InstallerError::new(
            "release_source_unavailable",
            format!("{subject} answered {status}"),
        ));
    }
    response
        .body_mut()
        .with_config()
        .limit(limit)
        .read_to_vec()
        .map_err(|error| {
            InstallerError::new(
                "release_body_rejected",
                format!("{subject} body could not be read within its bound: {error}"),
            )
        })
}
