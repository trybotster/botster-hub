//! The first frame of an accepted connection (HC HS-1, TB-L2; design section 1).
//!
//! The listener reads exactly one control-framed frame and classifies the connection by it:
//! - `{ "frame": "route_connect", route_id, route_token }` is a route connection;
//! - `{ "type": "hello", … }` is a control connection;
//! - anything else is refused with the typed close that this module returns.
//!
//! The contract crate's `RouteConnect` has no `frame` tag, and its tag layout is pending steward ruling H2
//! (`botster-hub-contract` `pairing.rs`). So this module reads the tag itself and parses the rest as `RouteConnect`.

use botster_hub_contract::prelude::{
    ClientFrame, CloseReason, Hello, ProtocolErrorData, RouteConnect,
};
use botster_route_codec::prelude::ProtocolErrorCode;
use serde_json::{Map, Value};

/// The tag value of a route connection's first frame (TB-L2).
pub const ROUTE_CONNECT: &str = "route_connect";

/// A classified connection.
#[derive(Debug, Clone, PartialEq)]
pub enum First {
    Control(Hello),
    Route(RouteConnect),
}

fn protocol_error(code: ProtocolErrorCode) -> CloseReason {
    CloseReason::ProtocolError(ProtocolErrorData { code })
}

/// Classifies the first frame's body (the JSON, without the length prefix).
///
/// - Not a JSON object, a `route_connect` without its two strings, or a `hello` that does not parse: `protocol_error
///   { malformed_frame }`.
/// - A `frame` tag other than `route_connect`: `protocol_error { unknown_frame }`.
/// - Any other first envelope frame: `handshake_order` (HS-1).
pub fn classify(body: &[u8]) -> Result<First, CloseReason> {
    let mut object: Map<String, Value> = match serde_json::from_slice(body) {
        Ok(Value::Object(object)) => object,
        _ => return Err(protocol_error(ProtocolErrorCode::MalformedFrame)),
    };
    if let Some(tag) = object.remove("frame") {
        if tag != ROUTE_CONNECT {
            return Err(protocol_error(ProtocolErrorCode::UnknownFrame));
        }
        return serde_json::from_value(Value::Object(object))
            .map(First::Route)
            .map_err(|_| protocol_error(ProtocolErrorCode::MalformedFrame));
    }
    let is_hello = object.get("type").and_then(Value::as_str) == Some("hello");
    match serde_json::from_value::<ClientFrame>(Value::Object(object)) {
        Ok(ClientFrame::Hello(hello)) => Ok(First::Control(hello)),
        Err(_) if is_hello => Err(protocol_error(ProtocolErrorCode::MalformedFrame)),
        _ => Err(CloseReason::HandshakeOrder),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn body(value: Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn hello() -> Value {
        json!({"type": "hello", "protocol": "botster-hub", "versions": {"min": 1, "max": 1},
            "client": {"name": "t", "version": "0"}, "features": [], "terminal_formats": ["ghostsnp/2+raw"]})
    }

    #[test]
    fn hello_is_a_control_connection() {
        match classify(&body(hello())) {
            Ok(First::Control(hello)) => assert_eq!(hello.protocol, "botster-hub"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_tagged_route_connect_is_a_route_connection() {
        let first = body(json!({"frame": "route_connect", "route_id": "r1", "route_token": "t"}));
        assert_eq!(
            classify(&first),
            Ok(First::Route(RouteConnect {
                route_id: "r1".into(),
                route_token: "t".into()
            }))
        );
    }

    #[test]
    fn another_envelope_frame_first_is_handshake_order() {
        let req = json!({"type": "req", "id": "1", "op": "list", "kind": "session"});
        assert_eq!(classify(&body(req)), Err(CloseReason::HandshakeOrder));
        assert_eq!(
            classify(&body(json!({"type": "nope"}))),
            Err(CloseReason::HandshakeOrder)
        );
        assert_eq!(
            classify(&body(json!({"route_id": "r1", "route_token": "t"}))),
            Err(CloseReason::HandshakeOrder),
            "an untagged route_connect is not a route connection"
        );
    }

    #[test]
    fn a_frame_that_does_not_parse_is_malformed() {
        let malformed = Err(protocol_error(ProtocolErrorCode::MalformedFrame));
        assert_eq!(classify(b"{"), malformed);
        assert_eq!(classify(b"[]"), malformed);
        assert_eq!(classify(&body(json!({"type": "hello"}))), malformed);
        assert_eq!(
            classify(&body(json!({"frame": "route_connect", "route_id": "r1"}))),
            malformed
        );
    }

    #[test]
    fn another_frame_tag_is_unknown() {
        assert_eq!(
            classify(&body(json!({"frame": "auth_response"}))),
            Err(protocol_error(ProtocolErrorCode::UnknownFrame))
        );
    }
}
