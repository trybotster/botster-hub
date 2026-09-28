//! HTTP/1.1 request head parsing and the gates every MCP request passes.
//!
//! This is a deliberately small subset: `POST /mcp` with a `Content-Length`
//! body. It is strict on purpose. Any `Transfer-Encoding`, a repeated
//! `Content-Length`, an `Expect`, an absolute-form target, and any `Origin`
//! header are refused, so request smuggling and browser access have no path
//! in. The gates run in a fixed order, and every refusal happens before the
//! body is read or Core is consulted:
//!
//! 1. `Host` names this listener (DNS rebinding);
//! 2. no `Origin` header (a browser page always sends one);
//! 3. a well-formed bearer token (the shape check needs no Core state);
//! 4. the path, the method, then the body framing.
//!
//! A missing, malformed, or unknown token gets the same 401 body.

use crate::session_credential::CallerToken;

/// Largest request head: the request line and every header, in bytes.
/// User decision 2026-09-28: 16 KiB and 64 headers. One line changes them.
pub(crate) const MAX_HEADER_BLOCK_BYTES: usize = 16 * 1024;
/// Most headers one request may carry.
pub(crate) const MAX_HEADER_COUNT: usize = 64;
/// The one endpoint path.
pub(crate) const MCP_PATH: &str = "/mcp";

/// What a request head asks for once it has passed the gates.
pub(crate) struct AdmittedHead {
    /// The bearer token, reduced to a session claim and a secret digest.
    pub(crate) token: CallerToken,
    /// Exact body length from `Content-Length`.
    pub(crate) body_len: usize,
    /// False when the client sent `Connection: close`.
    pub(crate) keep_alive: bool,
    /// True for a POST; other methods are refused after the token gate.
    pub(crate) method: Method,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    Post,
    Other,
}

/// A refused request. Each variant has one fixed status, code and message,
/// so a response never echoes any part of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    BadRequest,
    Forbidden,
    Unauthenticated,
    NotFound,
    MethodNotAllowed,
    LengthRequired,
    PayloadTooLarge,
    UnsupportedMediaType,
    ExpectationFailed,
    HeaderTooLarge,
    VersionNotSupported,
    Busy,
}

impl Refusal {
    pub(crate) fn status(self) -> (u16, &'static str) {
        match self {
            Self::BadRequest => (400, "Bad Request"),
            Self::Forbidden => (403, "Forbidden"),
            Self::Unauthenticated => (401, "Unauthorized"),
            Self::NotFound => (404, "Not Found"),
            Self::MethodNotAllowed => (405, "Method Not Allowed"),
            Self::LengthRequired => (411, "Length Required"),
            Self::PayloadTooLarge => (413, "Payload Too Large"),
            Self::UnsupportedMediaType => (415, "Unsupported Media Type"),
            Self::ExpectationFailed => (417, "Expectation Failed"),
            Self::HeaderTooLarge => (431, "Request Header Fields Too Large"),
            Self::VersionNotSupported => (505, "HTTP Version Not Supported"),
            Self::Busy => (503, "Service Unavailable"),
        }
    }

    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::Forbidden => "forbidden",
            Self::Unauthenticated => "caller_unauthenticated",
            Self::NotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::LengthRequired => "length_required",
            Self::PayloadTooLarge => "payload_too_large",
            Self::UnsupportedMediaType => "unsupported_media_type",
            Self::ExpectationFailed => "expectation_failed",
            Self::HeaderTooLarge => "header_too_large",
            Self::VersionNotSupported => "version_not_supported",
            Self::Busy => "busy",
        }
    }
}

/// Position just after the blank line that ends a request head.
pub(crate) fn head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

/// Parse and gate one complete request head (`head` ends with the blank line).
/// `port` is this listener's port, for the `Host` check.
pub(crate) fn admit_head(head: &[u8], port: u16) -> Result<AdmittedHead, Refusal> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADER_COUNT];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(head) {
        Ok(httparse::Status::Complete(used)) if used == head.len() => {}
        Ok(_) => return Err(Refusal::BadRequest),
        Err(httparse::Error::TooManyHeaders) => return Err(Refusal::HeaderTooLarge),
        Err(_) => return Err(Refusal::BadRequest),
    }
    if request.version != Some(1) {
        return Err(Refusal::VersionNotSupported);
    }

    let mut host = None;
    let mut origin_present = false;
    let mut authorization = None;
    let mut authorization_count = 0usize;
    let mut content_length = None;
    let mut content_type = None;
    let mut framing_refusal = None;
    let mut keep_alive = true;
    let mut content_length_count = 0usize;
    let mut host_count = 0usize;
    for header in request.headers.iter() {
        let name = header.name;
        if name.eq_ignore_ascii_case("host") {
            host_count += 1;
            host = Some(header.value);
        } else if name.eq_ignore_ascii_case("origin") {
            origin_present = true;
        } else if name.eq_ignore_ascii_case("authorization") {
            authorization_count += 1;
            authorization = Some(header.value);
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length_count += 1;
            content_length = Some(header.value);
        } else if name.eq_ignore_ascii_case("content-type") {
            content_type = Some(header.value);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            framing_refusal = Some(Refusal::LengthRequired);
        } else if name.eq_ignore_ascii_case("expect") {
            framing_refusal.get_or_insert(Refusal::ExpectationFailed);
        } else if name.eq_ignore_ascii_case("upgrade") {
            framing_refusal.get_or_insert(Refusal::BadRequest);
        } else if name.eq_ignore_ascii_case("connection") {
            let value = std::str::from_utf8(header.value).unwrap_or("");
            if value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("close"))
            {
                keep_alive = false;
            }
        }
    }

    // Gate 1: Host names this listener, exactly once.
    if host_count != 1 || !host_names_listener(host.unwrap_or_default(), port) {
        return Err(Refusal::Forbidden);
    }
    // Gate 2: a browser always sends Origin; an agent CLI never does.
    if origin_present {
        return Err(Refusal::Forbidden);
    }
    // Gate 3: one well-formed bearer token.
    if authorization_count != 1 {
        return Err(Refusal::Unauthenticated);
    }
    let token = bearer_token(authorization.unwrap_or_default()).ok_or(Refusal::Unauthenticated)?;

    // Gate 4: target, method, framing.
    if request.path != Some(MCP_PATH) {
        return Err(Refusal::NotFound);
    }
    let method = match request.method {
        Some("POST") => Method::Post,
        Some(_) => Method::Other,
        None => return Err(Refusal::BadRequest),
    };
    if method == Method::Other {
        return Ok(AdmittedHead {
            token,
            body_len: 0,
            keep_alive,
            method,
        });
    }
    if let Some(refusal) = framing_refusal {
        return Err(refusal);
    }
    if !content_type.is_some_and(is_json) {
        return Err(Refusal::UnsupportedMediaType);
    }
    if content_length_count != 1 {
        return Err(if content_length_count == 0 {
            Refusal::LengthRequired
        } else {
            Refusal::BadRequest
        });
    }
    let body_len = parse_content_length(content_length.unwrap_or_default())?;
    Ok(AdmittedHead {
        token,
        body_len,
        keep_alive,
        method,
    })
}

/// The one `Host` value forms a client may send: this listener's own address
/// or the loopback name, with the listener's port.
fn host_names_listener(value: &[u8], port: u16) -> bool {
    let Ok(value) = std::str::from_utf8(value) else {
        return false;
    };
    let value = value.trim();
    let Some((name, given)) = value.rsplit_once(':') else {
        return false;
    };
    given.parse::<u16>() == Ok(port)
        && (name.eq_ignore_ascii_case("127.0.0.1") || name.eq_ignore_ascii_case("localhost"))
}

fn bearer_token(value: &[u8]) -> Option<CallerToken> {
    let value = std::str::from_utf8(value).ok()?.trim();
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    CallerToken::parse(token.trim())
}

fn is_json(value: &[u8]) -> bool {
    let Ok(value) = std::str::from_utf8(value) else {
        return false;
    };
    let media_type = value.split(';').next().unwrap_or("").trim();
    media_type.eq_ignore_ascii_case("application/json")
}

fn parse_content_length(value: &[u8]) -> Result<usize, Refusal> {
    let text = std::str::from_utf8(value).map_err(|_| Refusal::BadRequest)?;
    let text = text.trim();
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Refusal::BadRequest);
    }
    match text.parse::<usize>() {
        Ok(length) if length <= botster_hub_client::MAX_CONTROL_REQUEST_BYTES => Ok(length),
        // Too large to parse counts as too large to accept.
        _ => Err(Refusal::PayloadTooLarge),
    }
}

/// Serialize one response. The body is fixed by the caller; nothing of the
/// request is echoed.
pub(crate) fn response_bytes(
    status: u16,
    reason: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
    keep_alive: bool,
) -> Vec<u8> {
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: {}\r\n",
        body.len(),
        if keep_alive { "keep-alive" } else { "close" },
    );
    for (name, value) in extra_headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

/// The response for a refusal: a fixed JSON body carrying only its code.
/// Every refusal ends the connection: the body of a refused request is never
/// read, so the next bytes cannot be trusted to start a new request.
pub(crate) fn refusal_response(refusal: Refusal) -> Vec<u8> {
    let (status, reason) = refusal.status();
    let body = format!(
        "{{\"error\":{{\"code\":\"{}\",\"message\":\"{}\"}}}}",
        refusal.code(),
        reason
    );
    let mut extra: Vec<(&str, &str)> = Vec::new();
    match refusal {
        Refusal::Unauthenticated => extra.push(("WWW-Authenticate", "Bearer")),
        Refusal::MethodNotAllowed => extra.push(("Allow", "POST")),
        _ => {}
    }
    response_bytes(status, reason, &extra, body.as_bytes(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PORT: u16 = 47_001;

    fn token() -> String {
        format!("sess-1.{}", "5a".repeat(32))
    }

    /// A request head with the given header lines and the standard ones.
    fn head(method: &str, path: &str, lines: &[String]) -> Vec<u8> {
        let mut text = format!("{method} {path} HTTP/1.1\r\n");
        for line in lines {
            text.push_str(line);
            text.push_str("\r\n");
        }
        text.push_str("\r\n");
        text.into_bytes()
    }

    fn standard() -> Vec<String> {
        vec![
            format!("Host: 127.0.0.1:{PORT}"),
            format!("Authorization: Bearer {}", token()),
            "Content-Type: application/json".to_string(),
            "Content-Length: 12".to_string(),
        ]
    }

    fn admit(lines: &[String]) -> Result<AdmittedHead, Refusal> {
        admit_head(&head("POST", MCP_PATH, lines), PORT)
    }

    fn without(prefix: &str) -> Vec<String> {
        standard()
            .into_iter()
            .filter(|line| !line.to_ascii_lowercase().starts_with(prefix))
            .collect()
    }

    fn with(extra: &str) -> Vec<String> {
        let mut lines = standard();
        lines.push(extra.to_string());
        lines
    }

    #[test]
    fn a_well_formed_request_is_admitted() {
        let admitted = admit(&standard()).ok().expect("admitted");
        assert_eq!(admitted.body_len, 12);
        assert!(admitted.keep_alive);
        assert_eq!(admitted.method, Method::Post);
        assert!(admitted.token.session_id() == "sess-1");
    }

    #[test]
    fn host_must_name_this_listener_exactly_once() {
        for host in [
            "Host: evil.example:47001".to_string(),
            "Host: 127.0.0.1:47002".to_string(),
            "Host: 127.0.0.1".to_string(),
            "Host: 127.0.0.1.evil.example:47001".to_string(),
        ] {
            let mut lines = without("host");
            lines.push(host);
            assert_eq!(admit(&lines).err(), Some(Refusal::Forbidden));
        }
        assert_eq!(admit(&without("host")).err(), Some(Refusal::Forbidden));
        assert_eq!(
            admit(&with(&format!("Host: 127.0.0.1:{PORT}"))).err(),
            Some(Refusal::Forbidden)
        );
        let mut lines = without("host");
        lines.push(format!("host: LOCALHOST:{PORT}"));
        assert!(admit(&lines).is_ok());
    }

    #[test]
    fn any_origin_header_is_refused_before_the_token_is_read() {
        // No token at all: the Origin refusal still wins, so a page learns
        // nothing about token handling.
        let mut lines = without("authorization");
        lines.push("Origin: http://evil.example".to_string());
        assert_eq!(admit(&lines).err(), Some(Refusal::Forbidden));
        assert_eq!(
            admit(&with("origin: null")).err(),
            Some(Refusal::Forbidden)
        );
        assert_eq!(
            admit(&with(&format!("Origin: http://127.0.0.1:{PORT}"))).err(),
            Some(Refusal::Forbidden)
        );
    }

    #[test]
    fn a_missing_or_malformed_token_is_unauthenticated() {
        assert_eq!(
            admit(&without("authorization")).err(),
            Some(Refusal::Unauthenticated)
        );
        let secret = "5a".repeat(32);
        for value in [
            "Authorization: Basic abc".to_string(),
            "Authorization: Bearer".to_string(),
            "Authorization: Bearer sess-1".to_string(),
            format!("Authorization: Bearer sess-1.{}", "5a".repeat(31)),
            format!("Authorization: Bearer sess 1.{secret}"),
            format!("Authorization: {}", token()),
        ] {
            let mut lines = without("authorization");
            lines.push(value);
            assert_eq!(admit(&lines).err(), Some(Refusal::Unauthenticated));
        }
        // Two Authorization headers are ambiguous, so unauthenticated.
        assert_eq!(
            admit(&with(&format!("Authorization: Bearer {}", token()))).err(),
            Some(Refusal::Unauthenticated)
        );
        let mut lines = without("authorization");
        lines.push(format!("authorization: bearer {}", token()));
        assert!(admit(&lines).is_ok());
    }

    #[test]
    fn the_token_gate_runs_before_the_path_and_method_gates() {
        let lines = without("authorization");
        assert_eq!(
            admit_head(&head("POST", "/elsewhere", &lines), PORT).err(),
            Some(Refusal::Unauthenticated)
        );
        assert_eq!(
            admit_head(&head("GET", MCP_PATH, &lines), PORT).err(),
            Some(Refusal::Unauthenticated)
        );
    }

    #[test]
    fn path_and_method_are_checked_after_the_token() {
        assert_eq!(
            admit_head(&head("POST", "/elsewhere", &standard()), PORT).err(),
            Some(Refusal::NotFound)
        );
        assert_eq!(
            admit_head(&head("POST", "/mcp?x=1", &standard()), PORT).err(),
            Some(Refusal::NotFound)
        );
        assert_eq!(
            admit_head(&head("POST", "http://127.0.0.1/mcp", &standard()), PORT).err(),
            Some(Refusal::NotFound)
        );
        for method in ["GET", "DELETE", "PUT"] {
            let admitted = admit_head(&head(method, MCP_PATH, &standard()), PORT)
                .ok()
                .expect("admitted for a 405");
            assert_eq!(admitted.method, Method::Other);
        }
    }

    #[test]
    fn framing_that_could_smuggle_a_request_is_refused() {
        assert_eq!(
            admit(&with("Transfer-Encoding: chunked")).err(),
            Some(Refusal::LengthRequired)
        );
        assert_eq!(
            admit(&with("transfer-encoding: identity")).err(),
            Some(Refusal::LengthRequired)
        );
        assert_eq!(
            admit(&with("Content-Length: 12")).err(),
            Some(Refusal::BadRequest)
        );
        assert_eq!(
            admit(&without("content-length")).err(),
            Some(Refusal::LengthRequired)
        );
        assert_eq!(
            admit(&with("Expect: 100-continue")).err(),
            Some(Refusal::ExpectationFailed)
        );
        assert_eq!(
            admit(&with("Upgrade: websocket")).err(),
            Some(Refusal::BadRequest)
        );
        for value in ["Content-Length: -1", "Content-Length: 1x", "Content-Length: "] {
            let mut lines = without("content-length");
            lines.push(value.to_string());
            assert_eq!(admit(&lines).err(), Some(Refusal::BadRequest));
        }
    }

    #[test]
    fn the_body_bound_is_inclusive() {
        let limit = botster_hub_client::MAX_CONTROL_REQUEST_BYTES;
        let mut lines = without("content-length");
        lines.push(format!("Content-Length: {limit}"));
        assert_eq!(admit(&lines).ok().map(|head| head.body_len), Some(limit));
        let mut lines = without("content-length");
        lines.push(format!("Content-Length: {}", limit + 1));
        assert_eq!(admit(&lines).err(), Some(Refusal::PayloadTooLarge));
        let mut lines = without("content-length");
        lines.push("Content-Length: 99999999999999999999999999".to_string());
        assert_eq!(admit(&lines).err(), Some(Refusal::PayloadTooLarge));
    }

    #[test]
    fn the_body_must_be_json() {
        let mut lines = without("content-type");
        lines.push("Content-Type: text/plain".to_string());
        assert_eq!(admit(&lines).err(), Some(Refusal::UnsupportedMediaType));
        assert_eq!(
            admit(&without("content-type")).err(),
            Some(Refusal::UnsupportedMediaType)
        );
        let mut lines = without("content-type");
        lines.push("content-type: Application/JSON; charset=utf-8".to_string());
        assert!(admit(&lines).is_ok());
    }

    #[test]
    fn header_count_and_version_bounds_hold() {
        let mut lines = standard();
        for index in 0..MAX_HEADER_COUNT {
            lines.push(format!("X-Filler-{index}: 1"));
        }
        assert_eq!(admit(&lines).err(), Some(Refusal::HeaderTooLarge));
        let http10 = b"POST /mcp HTTP/1.0\r\nHost: 127.0.0.1:47001\r\n\r\n";
        assert_eq!(
            admit_head(http10, PORT).err(),
            Some(Refusal::VersionNotSupported)
        );
        assert_eq!(
            admit_head(b"NOT A REQUEST\r\n\r\n", PORT).err(),
            Some(Refusal::BadRequest)
        );
    }

    #[test]
    fn connection_close_ends_keep_alive() {
        let admitted = admit(&with("Connection: close")).ok().expect("admitted");
        assert!(!admitted.keep_alive);
    }

    #[test]
    fn head_end_finds_the_blank_line() {
        assert_eq!(head_end(b"GET / HTTP/1.1\r\nA: b\r\n\r\nbody"), Some(25));
        assert_eq!(head_end(b"GET / HTTP/1.1\r\nA: b\r\n"), None);
    }

    #[test]
    fn a_refusal_response_never_echoes_the_request() {
        let bytes = refusal_response(Refusal::Unauthenticated);
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
        assert!(text.contains("WWW-Authenticate: Bearer\r\n"));
        assert!(text.contains("\"code\":\"caller_unauthenticated\""));
        // The refused body is never read, so the connection closes.
        assert!(text.contains("Connection: close\r\n"));
        let not_allowed = String::from_utf8(refusal_response(Refusal::MethodNotAllowed)).unwrap();
        assert!(not_allowed.contains("Allow: POST\r\n"));
    }
}
