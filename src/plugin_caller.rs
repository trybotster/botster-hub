//! Who a plugin handler is running for.
//!
//! The Hub sets the caller in the invocation context, never the plugin and
//! never the client's arguments. An MCP tool call runs for the operator or for
//! a verified session; every other invocation (event, timer, route) runs for
//! the plugin itself. Lua handlers read it as `request.caller`, the second
//! argument of every handler.

use botster_core::{BoundaryJson, PluginInvocationContext};
use serde_json::{Value, json};

/// The metadata key that carries the caller inside the invocation context.
const CALLER_KEY: &str = "caller";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PluginCaller {
    /// A local operator (the Hub socket), trusted with everything.
    Operator,
    /// A session whose credential the Hub verified.
    Session { hub_id: String, session_id: String },
    /// No tool caller: an event, timer or route the plugin serves itself.
    Plugin,
}

impl PluginCaller {
    /// The value Lua sees as `request.caller`.
    pub(crate) fn to_json(&self) -> Value {
        match self {
            Self::Operator => json!({ "kind": "operator" }),
            Self::Session { hub_id, session_id } => {
                json!({ "kind": "session", "hub_id": hub_id, "session_id": session_id })
            }
            Self::Plugin => json!({ "kind": "plugin" }),
        }
    }

    /// The caller a tool call's context carries. An absent or malformed caller
    /// is the plugin: it never widens to operator.
    pub(crate) fn from_context(context: &PluginInvocationContext) -> Self {
        let Some(caller) = context
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.0.get(CALLER_KEY))
        else {
            return Self::Plugin;
        };
        match caller.get("kind").and_then(Value::as_str) {
            Some("operator") => Self::Operator,
            Some("session") => match (
                caller.get("hub_id").and_then(Value::as_str),
                caller.get("session_id").and_then(Value::as_str),
            ) {
                (Some(hub_id), Some(session_id)) => Self::Session {
                    hub_id: hub_id.to_string(),
                    session_id: session_id.to_string(),
                },
                _ => Self::Plugin,
            },
            _ => Self::Plugin,
        }
    }

    /// The context metadata that carries this caller, for a tool invocation.
    pub(crate) fn to_metadata(&self) -> Option<BoundaryJson> {
        match self {
            Self::Plugin => None,
            _ => Some(BoundaryJson(json!({ CALLER_KEY: self.to_json() }))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(metadata: Option<Value>) -> PluginInvocationContext {
        PluginInvocationContext {
            client_id: None,
            session_id: None,
            subscription_id: None,
            surface_id: None,
            origin: None,
            metadata: metadata.map(BoundaryJson),
        }
    }

    #[test]
    fn a_caller_round_trips_through_the_context_metadata() {
        for caller in [
            PluginCaller::Operator,
            PluginCaller::Session {
                hub_id: "hub-1".to_string(),
                session_id: "s-1".to_string(),
            },
        ] {
            let metadata = caller.to_metadata().map(|metadata| metadata.0);
            assert_eq!(PluginCaller::from_context(&context(metadata)), caller);
        }
        assert_eq!(PluginCaller::Plugin.to_metadata(), None);
    }

    #[test]
    fn a_missing_or_malformed_caller_is_the_plugin_never_the_operator() {
        assert_eq!(
            PluginCaller::from_context(&context(None)),
            PluginCaller::Plugin
        );
        for malformed in [
            json!({ "caller": {} }),
            json!({ "caller": { "kind": "root" } }),
            json!({ "caller": { "kind": "session", "session_id": "s" } }),
            json!({ "caller": "operator" }),
        ] {
            assert_eq!(
                PluginCaller::from_context(&context(Some(malformed))),
                PluginCaller::Plugin
            );
        }
    }
}
