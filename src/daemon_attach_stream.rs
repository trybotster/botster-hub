//! Owner registry for subscription-owned attach drains.
//!
//! Core owns incremental frames, FINISH, `attached`, and queued input/resize.
//! Hub authorizes the route and pulls `drain_subscription` once per Drain.

use std::collections::{BTreeMap, BTreeSet};

use botster_core::{ClientId, SessionId, SubscriptionId};
use botster_core_daemon::CoreDaemonError;
use botster_hub_client::DaemonEvent;

use super::{DaemonTransportError, DaemonTransportResult, daemon_event_from_client};
use crate::HubRuntime;
use crate::client_api::events_from_drain;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachStreamOwner {
    pub client_id: String,
    pub grant_id: Option<String>,
}

pub(crate) struct AttachStream {
    owner: AttachStreamOwner,
}

impl AttachStream {
    fn new(owner: AttachStreamOwner) -> Self {
        Self { owner }
    }

    pub(crate) fn owner_client_id(&self) -> String {
        self.owner.client_id.clone()
    }

    fn owner_matches(&self, client_id: Option<&str>, grant_id: Option<&str>) -> bool {
        if let Some(client_id) = client_id
            && self.owner.client_id == client_id
        {
            return true;
        }
        match (grant_id, self.owner.grant_id.as_deref()) {
            (Some(left), Some(right)) => left == right,
            _ => false,
        }
    }
}

pub(crate) struct AttachStreamRegistry {
    streams: BTreeMap<(String, String), AttachStream>,
    pub(crate) active_subscriptions: BTreeMap<String, BTreeSet<String>>,
    pub(crate) attach_owner_grant_ids: BTreeMap<(String, String), String>,
}

impl Default for AttachStreamRegistry {
    fn default() -> Self {
        Self {
            streams: BTreeMap::new(),
            active_subscriptions: BTreeMap::new(),
            attach_owner_grant_ids: BTreeMap::new(),
        }
    }
}

impl AttachStreamRegistry {
    pub(crate) fn start_attach(
        &mut self,
        owner: AttachStreamOwner,
        session_id: String,
        subscription_id: String,
    ) {
        self.cancel_stream(&session_id, &subscription_id);
        if let Some(grant_id) = owner.grant_id.clone() {
            self.attach_owner_grant_ids
                .insert((session_id.clone(), subscription_id.clone()), grant_id);
        }
        self.active_subscriptions
            .entry(session_id.clone())
            .or_default()
            .insert(subscription_id.clone());
        self.streams
            .insert((session_id, subscription_id), AttachStream::new(owner));
    }

    pub(crate) fn begin_core_attach(
        &self,
        runtime: &mut HubRuntime,
        session_id: &str,
        subscription_id: &str,
        now_seconds: u64,
    ) -> Result<Vec<DaemonEvent>, CoreDaemonError> {
        let Some(client_id) = self.stream_owner_client_id(session_id, subscription_id) else {
            return Ok(Vec::new());
        };
        let attached = runtime.attach_client(
            ClientId(client_id),
            SessionId(session_id.to_string()),
            SubscriptionId(subscription_id.to_string()),
            now_seconds,
        )?;
        Ok(events_from_drain(botster_core_daemon::DrainResult {
            client_egress: attached.client_egress,
            ..botster_core_daemon::DrainResult::default()
        })
        .into_iter()
        .map(daemon_event_from_client)
        .collect())
    }

    pub(crate) fn stream_owner_client_id(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<String> {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(AttachStream::owner_client_id)
    }

    pub(crate) fn authorize_drain(
        &self,
        session_id: &str,
        subscription_id: &str,
        client_id: Option<&str>,
        grant_id: Option<&str>,
    ) -> DaemonTransportResult<()> {
        let Some(stream) = self
            .streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
        else {
            return Ok(());
        };
        if stream.owner_matches(client_id, grant_id) {
            Ok(())
        } else {
            Err(DaemonTransportError::SnapshotStreamForbidden {
                session_id: session_id.to_string(),
                subscription_id: subscription_id.to_string(),
            })
        }
    }

    pub(crate) fn cancel_stream(&mut self, session_id: &str, subscription_id: &str) {
        self.streams
            .remove(&(session_id.to_string(), subscription_id.to_string()));
        if let Some(subscriptions) = self.active_subscriptions.get_mut(session_id) {
            subscriptions.remove(subscription_id);
            if subscriptions.is_empty() {
                self.active_subscriptions.remove(session_id);
            }
        }
        self.attach_owner_grant_ids
            .remove(&(session_id.to_string(), subscription_id.to_string()));
    }

    pub(crate) fn retain_active_sessions(&mut self, active_session_ids: &BTreeSet<String>) {
        let stale: Vec<(String, String)> = self
            .streams
            .keys()
            .filter(|(session_id, _)| !active_session_ids.contains(session_id))
            .cloned()
            .collect();
        for (session_id, subscription_id) in stale {
            self.cancel_stream(&session_id, &subscription_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> AttachStreamOwner {
        AttachStreamOwner {
            client_id: "client-a".to_string(),
            grant_id: None,
        }
    }

    #[test]
    fn foreign_owner_cannot_drain() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        assert!(
            registry
                .authorize_drain("s", "sub", Some("other"), None)
                .is_err()
        );
        assert!(
            registry
                .authorize_drain("s", "sub", Some("client-a"), None)
                .is_ok()
        );
    }

    #[test]
    fn start_attach_records_route_owner_only() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        assert_eq!(
            registry.stream_owner_client_id("s", "sub").as_deref(),
            Some("client-a")
        );
        registry.cancel_stream("s", "sub");
        assert_eq!(registry.stream_owner_client_id("s", "sub"), None);
        assert!(!registry.active_subscriptions.contains_key("s"));
    }
}
