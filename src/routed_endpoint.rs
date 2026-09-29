//! The Hub's names for routed-envelope endpoints.
//!
//! Core treats an endpoint as an opaque string. The Hub gives that string a
//! shape that never assumes a bare session ID is meaningful on its own:
//!
//! - `hub:<hub_id>/session:<session_id>` for a session;
//! - `hub:<hub_id>/operator` for the operator of a hub;
//! - `plugin:<plugin_key>` for a plugin.
//!
//! Every string is built here and parsed here. No plugin or client parses one:
//! they see the structured `EndpointRef`. Only the local hub is accepted as
//! the sender today; the shape leaves room for others.

/// A parsed routed-envelope endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointRef {
    Session { hub_id: String, session_id: String },
    Operator { hub_id: String },
    Plugin { plugin_key: String },
    /// Anything else, kept verbatim so nothing is lost.
    Other(String),
}

const HUB_PREFIX: &str = "hub:";
const SESSION_MARK: &str = "/session:";
const OPERATOR_MARK: &str = "/operator";
const PLUGIN_PREFIX: &str = "plugin:";

/// A hub ID names one hub and cannot hold the separator.
fn usable_hub_id(hub_id: &str) -> bool {
    !hub_id.is_empty() && !hub_id.contains('/')
}

/// The endpoint string for a session of a hub.
pub(crate) fn session_endpoint(hub_id: &str, session_id: &str) -> String {
    debug_assert!(usable_hub_id(hub_id));
    format!("{HUB_PREFIX}{hub_id}{SESSION_MARK}{session_id}")
}

/// The endpoint string for the operator of a hub.
pub(crate) fn operator_endpoint(hub_id: &str) -> String {
    debug_assert!(usable_hub_id(hub_id));
    format!("{HUB_PREFIX}{hub_id}{OPERATOR_MARK}")
}

/// Parse one endpoint string. This is the only parser.
pub(crate) fn parse_endpoint(endpoint: &str) -> EndpointRef {
    if let Some(rest) = endpoint.strip_prefix(HUB_PREFIX)
        && let Some(split) = rest.find('/')
    {
        let (hub_id, tail) = rest.split_at(split);
        if usable_hub_id(hub_id) {
            if let Some(session_id) = tail.strip_prefix(SESSION_MARK) {
                return EndpointRef::Session {
                    hub_id: hub_id.to_string(),
                    session_id: session_id.to_string(),
                };
            }
            if tail == OPERATOR_MARK {
                return EndpointRef::Operator {
                    hub_id: hub_id.to_string(),
                };
            }
        }
    }
    if let Some(plugin_key) = endpoint.strip_prefix(PLUGIN_PREFIX) {
        return EndpointRef::Plugin {
            plugin_key: plugin_key.to_string(),
        };
    }
    EndpointRef::Other(endpoint.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_endpoint_round_trips_and_names_its_hub() {
        let text = session_endpoint("device-1", "sess-a.b/c");
        assert_eq!(text, "hub:device-1/session:sess-a.b/c");
        assert_eq!(
            parse_endpoint(&text),
            EndpointRef::Session {
                hub_id: "device-1".into(),
                session_id: "sess-a.b/c".into(),
            }
        );
    }

    #[test]
    fn the_operator_and_plugin_forms_parse() {
        assert_eq!(
            parse_endpoint(&operator_endpoint("device-1")),
            EndpointRef::Operator {
                hub_id: "device-1".into()
            }
        );
        assert_eq!(
            parse_endpoint("plugin:botster-messaging"),
            EndpointRef::Plugin {
                plugin_key: "botster-messaging".into()
            }
        );
    }

    #[test]
    fn a_bare_session_endpoint_is_not_a_session() {
        // The old `session:<id>` form names no hub, so it is never parsed as
        // a session of any hub.
        for text in ["session:sess-1", "hub:/session:x", "hub:device", "hub:d/other"] {
            assert_eq!(parse_endpoint(text), EndpointRef::Other(text.to_string()));
        }
    }

    /// The builders are the only source of endpoint text: no Hub code builds
    /// a `session:<id>`-only endpoint.
    #[test]
    fn no_producer_builds_a_bare_session_endpoint() {
        for hub in ["a", "device-2b343b8b6bc4855c"] {
            assert!(session_endpoint(hub, "s").starts_with("hub:"));
            assert!(operator_endpoint(hub).starts_with("hub:"));
        }
        let messaging = include_str!("daemon/control/messaging.rs");
        assert!(
            !messaging.contains("format!(\"session:"),
            "messaging builds endpoints through routed_endpoint"
        );
    }
}
