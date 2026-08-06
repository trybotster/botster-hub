//! Network-coordinate policy for managed release sources.
//!
//! The ticket's HTTPS rule applies to *every* coordinate the installer fetches,
//! not only the outer document, so this policy is shared by the Hub — which
//! validates the coordinate recorded in a receipt — and by the installer, which
//! validates the metadata URL and each artifact URL independently. One rule,
//! one implementation, so the two cannot drift apart.

use crate::safety::InstallationProblem;

/// Accept HTTPS anywhere, and plain HTTP only against loopback.
///
/// Loopback HTTP exists for tests and local fixtures. Anything else — a
/// non-loopback `http://`, a `file://`, an unparseable string — is refused.
pub fn validate_release_source(source_url: &str) -> Result<(), InstallationProblem> {
    let uri = source_url
        .parse::<http::Uri>()
        .map_err(|_| invalid("installation release source is not a valid absolute URL"))?;
    let Some(scheme) = uri.scheme_str() else {
        return Err(invalid("installation release source has no scheme"));
    };
    let Some(host) = uri.host() else {
        return Err(invalid("installation release source has no host"));
    };
    match scheme {
        "https" => Ok(()),
        "http" if is_loopback_host(host) => Ok(()),
        "http" => Err(InstallationProblem::new(
            "insecure_release_source",
            "installation release source must use HTTPS or loopback HTTP",
        )),
        _ => Err(invalid("installation release source scheme is unsupported")),
    }
}

/// Whether a host names the loopback interface.
#[must_use]
pub fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn invalid(message: &'static str) -> InstallationProblem {
    InstallationProblem::new("invalid_release_source", message)
}
