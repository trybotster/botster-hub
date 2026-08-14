//! Owner registry for subscription-owned attach drains.
//!
//! Core owns incremental frames, FINISH, `attached`, and queued input/resize.
//! Hub authorizes the route and records generation plus adapter-bound flags.

use std::collections::{BTreeMap, BTreeSet};

use botster_core::{
    ClientId, SessionId, SubscriptionId, TerminalCapabilitySet, TerminalSubscriptionGeneration,
    TerminalSubscriptionRecord,
};
use botster_core_daemon::CoreDaemonError;
use botster_hub_client::{
    ATTACH_STATE_ATTACHING, DaemonEvent, FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY,
    FEATURE_UNIX_TERMINAL_ADAPTER,
};
use botster_terminal_protocol::TerminalCompatibility;

use super::{DaemonTransportError, DaemonTransportResult, daemon_event_from_client};
use crate::HubRuntime;
use crate::client_api::events_from_drain;
use crate::unix_terminal_adapter::{
    UnixConnectionMux, UnixTerminalAdapter, UnixTerminalAdapterHandle,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ConnectionBoundRoute {
    pub session_id: String,
    pub subscription_id: String,
    pub generation: TerminalSubscriptionGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachStreamOwner {
    pub client_id: String,
    pub grant_id: Option<String>,
}

pub(crate) struct AttachStream {
    owner: AttachStreamOwner,
    generation: Option<TerminalSubscriptionGeneration>,
    adapter_bound: bool,
    adapter: Option<UnixTerminalAdapterHandle>,
}

impl AttachStream {
    fn new(owner: AttachStreamOwner) -> Self {
        Self {
            owner,
            generation: None,
            adapter_bound: false,
            adapter: None,
        }
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

    fn close_adapter(&mut self) {
        if let Some(adapter) = self.adapter.take() {
            adapter.close();
        }
        self.adapter_bound = false;
    }
}

#[derive(Default)]
pub(crate) struct AttachStreamRegistry {
    streams: BTreeMap<(String, String), AttachStream>,
    pub(crate) active_subscriptions: BTreeMap<String, BTreeSet<String>>,
    pub(crate) attach_owner_grant_ids: BTreeMap<(String, String), String>,
    connection_bound_routes: BTreeMap<String, BTreeSet<ConnectionBoundRoute>>,
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
        self.forget_connection_bound_route(session_id, subscription_id);
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

    fn forget_connection_bound_route(&mut self, session_id: &str, subscription_id: &str) {
        let mut empty_clients = Vec::new();
        for (client_id, routes) in &mut self.connection_bound_routes {
            routes.retain(|route| {
                route.session_id != session_id || route.subscription_id != subscription_id
            });
            if routes.is_empty() {
                empty_clients.push(client_id.clone());
            }
        }
        for client_id in empty_clients {
            self.connection_bound_routes.remove(&client_id);
        }
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

    pub(crate) fn is_adapter_bound(&self, session_id: &str, subscription_id: &str) -> bool {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .is_some_and(|stream| stream.adapter_bound)
    }

    pub(crate) fn close_adapter(&mut self, session_id: &str, subscription_id: &str) {
        if let Some(stream) = self
            .streams
            .get_mut(&(session_id.to_string(), subscription_id.to_string()))
        {
            stream.close_adapter();
        }
    }

    pub(crate) fn bound_routes(&self) -> Vec<(String, String, UnixTerminalAdapterHandle)> {
        self.streams
            .iter()
            .filter_map(|((session_id, subscription_id), stream)| {
                stream
                    .adapter
                    .clone()
                    .map(|handle| (session_id.clone(), subscription_id.clone(), handle))
            })
            .collect()
    }

    pub(crate) fn bound_route_keys_for_client(
        &self,
        client_id: &str,
    ) -> BTreeSet<(String, String)> {
        self.streams
            .iter()
            .filter(|(_, stream)| stream.owner.client_id == client_id && stream.adapter_bound)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(crate) fn close_adapters_for_client(&mut self, client_id: &str) {
        let keys = self.bound_route_keys_for_client(client_id);
        for (session_id, subscription_id) in keys {
            self.close_adapter(&session_id, &subscription_id);
        }
    }

    pub(crate) fn reconcile_inventory(&mut self, inventory: &[TerminalSubscriptionRecord]) {
        let stale: Vec<(String, String)> = self
            .streams
            .iter()
            .filter(|((session_id, subscription_id), stream)| {
                stream.adapter_bound
                    && !inventory.iter().any(|row| {
                        row.session_id.0 == *session_id
                            && row.subscription_id.0 == *subscription_id
                            && stream
                                .generation
                                .is_none_or(|generation| row.generation == generation)
                    })
            })
            .map(|(key, _)| key.clone())
            .collect();
        for (session_id, subscription_id) in stale {
            self.close_adapter(&session_id, &subscription_id);
            self.cancel_stream(&session_id, &subscription_id);
        }
    }

    pub(crate) fn record_generation(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        generation: TerminalSubscriptionGeneration,
    ) {
        if let Some(stream) = self
            .streams
            .get_mut(&(session_id.to_string(), subscription_id.to_string()))
        {
            stream.generation = Some(generation);
        }
    }

    pub(crate) fn mark_adapter_bound(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        generation: TerminalSubscriptionGeneration,
        adapter: UnixTerminalAdapterHandle,
    ) {
        let key = (session_id.to_string(), subscription_id.to_string());
        let client_id = self
            .streams
            .get(&key)
            .map(|stream| stream.owner.client_id.clone());
        if let Some(stream) = self.streams.get_mut(&key) {
            stream.generation = Some(generation);
            stream.adapter_bound = true;
            stream.adapter = Some(adapter);
        }
        if let Some(client_id) = client_id {
            self.connection_bound_routes
                .entry(client_id)
                .or_default()
                .insert(ConnectionBoundRoute {
                    session_id: key.0,
                    subscription_id: key.1,
                    generation,
                });
        }
    }

    pub(crate) fn take_connection_bound_routes(
        &mut self,
        client_id: &str,
    ) -> BTreeSet<ConnectionBoundRoute> {
        self.connection_bound_routes
            .remove(client_id)
            .unwrap_or_default()
    }

    pub(crate) fn connection_bound_route_still_owned(
        &self,
        client_id: &str,
        session_id: &str,
        subscription_id: &str,
        generation: TerminalSubscriptionGeneration,
    ) -> bool {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .is_some_and(|stream| {
                stream.owner.client_id == client_id && stream.generation == Some(generation)
            })
    }
}

pub(crate) fn is_terminal_body_event(event: &DaemonEvent) -> bool {
    matches!(
        event,
        DaemonEvent::Snapshot { .. }
            | DaemonEvent::Scrollback { .. }
            | DaemonEvent::TerminalOutput { .. }
            | DaemonEvent::ProcessExit { .. }
            | DaemonEvent::AttachState { .. }
    )
}

pub(crate) fn terminal_event_is_pre_bind_forbidden(event: &DaemonEvent) -> bool {
    match event {
        DaemonEvent::Snapshot { .. }
        | DaemonEvent::Scrollback { .. }
        | DaemonEvent::TerminalOutput { .. }
        | DaemonEvent::ProcessExit { .. } => true,
        DaemonEvent::AttachState { state, .. } => state != ATTACH_STATE_ATTACHING,
        _ => false,
    }
}

pub(crate) fn initial_attaching_only(events: &[DaemonEvent]) -> bool {
    let terminal: Vec<_> = events
        .iter()
        .filter(|event| is_terminal_body_event(event))
        .collect();
    matches!(
        terminal.as_slice(),
        [event] if !terminal_event_is_pre_bind_forbidden(event)
    )
}

pub(crate) fn negotiated_unix_capability_set(
    required_features: &[String],
) -> Result<TerminalCapabilitySet, botster_core::TerminalCapabilitySetError> {
    let include_snapshot = required_features
        .iter()
        .any(|feature| feature == FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY);
    let tokens: Vec<String> = TerminalCompatibility::current()
        .features
        .into_iter()
        .filter(|token| {
            if token == FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY {
                include_snapshot
            } else {
                true
            }
        })
        .collect();
    TerminalCapabilitySet::from_tokens(tokens)
}

pub(crate) fn hello_requires_unix_adapter(required_features: &[String]) -> bool {
    required_features
        .iter()
        .any(|feature| feature == FEATURE_UNIX_TERMINAL_ADAPTER)
}

pub(crate) fn live_generation_for_route(
    inventory: &[TerminalSubscriptionRecord],
    client_id: &str,
    session_id: &str,
    subscription_id: &str,
) -> Option<TerminalSubscriptionGeneration> {
    inventory.iter().find_map(|row| {
        if row.client_id.0 == client_id
            && row.session_id.0 == session_id
            && row.subscription_id.0 == subscription_id
        {
            Some(row.generation)
        } else {
            None
        }
    })
}

pub(crate) fn attach_failed_events(session_id: &str, subscription_id: &str) -> Vec<DaemonEvent> {
    vec![DaemonEvent::AttachState {
        session_id: session_id.to_string(),
        subscription_id: subscription_id.to_string(),
        state: botster_hub_client::ATTACH_STATE_ATTACH_FAILED.to_string(),
    }]
}

pub(crate) fn fail_closed_pre_bind_attach(
    registry: &mut AttachStreamRegistry,
    runtime: &mut HubRuntime,
    client_id: &str,
    session_id: &str,
    subscription_id: &str,
    now_seconds: u64,
    adapter: Option<UnixTerminalAdapterHandle>,
) -> Vec<DaemonEvent> {
    if let Some(adapter) = adapter {
        adapter.close();
    }
    registry.close_adapter(session_id, subscription_id);
    let generation = live_generation_for_route(
        &runtime.list_terminal_subscriptions(),
        client_id,
        session_id,
        subscription_id,
    );
    if let Some(generation) = generation {
        let _ = runtime.detach_terminal_subscription(
            ClientId(client_id.to_string()),
            SessionId(session_id.to_string()),
            SubscriptionId(subscription_id.to_string()),
            generation,
            now_seconds,
        );
    }
    registry.cancel_stream(session_id, subscription_id);
    attach_failed_events(session_id, subscription_id)
}

pub(crate) struct UnixBindRequest<'a> {
    pub client_id: &'a str,
    pub session_id: &'a str,
    pub subscription_id: &'a str,
    pub required_features: &'a [String],
    pub now_seconds: u64,
    pub mux: Option<&'a UnixConnectionMux>,
}

pub(crate) fn bind_unix_adapter_after_attaching(
    registry: &mut AttachStreamRegistry,
    runtime: &mut HubRuntime,
    request: UnixBindRequest<'_>,
) -> Result<Option<UnixTerminalAdapterHandle>, Vec<DaemonEvent>> {
    let inventory = runtime.list_terminal_subscriptions();
    let Some(generation) = live_generation_for_route(
        &inventory,
        request.client_id,
        request.session_id,
        request.subscription_id,
    ) else {
        return Err(fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            None,
        ));
    };
    registry.record_generation(request.session_id, request.subscription_id, generation);
    let capabilities = match negotiated_unix_capability_set(request.required_features) {
        Ok(capabilities) => capabilities,
        Err(_) => {
            return Err(fail_closed_pre_bind_attach(
                registry,
                runtime,
                request.client_id,
                request.session_id,
                request.subscription_id,
                request.now_seconds,
                None,
            ));
        }
    };
    let (adapter, handle) = match request.mux {
        Some(mux) => mux.create_adapter(),
        None => UnixTerminalAdapter::pair(),
    };
    if runtime
        .bind_terminal_adapter(
            ClientId(request.client_id.to_string()),
            SessionId(request.session_id.to_string()),
            SubscriptionId(request.subscription_id.to_string()),
            generation,
            capabilities,
            Box::new(adapter),
        )
        .is_err()
    {
        return Err(fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            Some(handle),
        ));
    }
    registry.mark_adapter_bound(
        request.session_id,
        request.subscription_id,
        generation,
        handle.clone(),
    );
    if let Some(mux) = request.mux {
        mux.register(
            request.session_id.to_string(),
            request.subscription_id.to_string(),
            handle.clone(),
        );
    }
    Ok(Some(handle))
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

    #[test]
    fn only_the_initial_attaching_frame_is_accepted_before_bind() {
        let attaching = DaemonEvent::AttachState {
            session_id: "s".to_string(),
            subscription_id: "sub".to_string(),
            state: ATTACH_STATE_ATTACHING.to_string(),
        };
        assert!(initial_attaching_only(std::slice::from_ref(&attaching)));
        assert!(initial_attaching_only(&[
            DaemonEvent::SessionLifecycle {
                session_id: "s".to_string(),
                state: "running".to_string(),
            },
            attaching.clone(),
        ]));
        assert!(!initial_attaching_only(&[]));
        assert!(!initial_attaching_only(&[DaemonEvent::AttachState {
            session_id: "s".to_string(),
            subscription_id: "sub".to_string(),
            state: "attached".to_string(),
        }]));
        assert!(!initial_attaching_only(&[
            attaching.clone(),
            DaemonEvent::Snapshot {
                session_id: "s".to_string(),
                subscription_id: "sub".to_string(),
                history: botster_hub_client::DaemonOpaqueHistoryPayload::from_bytes(b"x"),
            },
        ]));
        assert!(terminal_event_is_pre_bind_forbidden(
            &DaemonEvent::TerminalOutput {
                session_id: "s".to_string(),
                subscription_id: "sub".to_string(),
                payload: botster_hub_client::DaemonLiveOutputPayload::from_bytes(b"out"),
            }
        ));
        assert!(terminal_event_is_pre_bind_forbidden(
            &DaemonEvent::ProcessExit {
                session_id: "s".to_string(),
                subscription_id: "sub".to_string(),
                code: Some(0),
            }
        ));
    }

    #[test]
    fn bound_route_keys_are_captured_before_close_clears_the_flag() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound("s", "sub", TerminalSubscriptionGeneration(1), handle);
        let keys = registry.bound_route_keys_for_client("client-a");
        registry.close_adapters_for_client("client-a");
        assert!(keys.contains(&("s".to_string(), "sub".to_string())));
        assert!(
            !registry.is_adapter_bound("s", "sub"),
            "close must not be used as the bound-route classifier"
        );
        let recorded = registry.take_connection_bound_routes("client-a");
        assert!(
            recorded.iter().any(|route| {
                route.session_id == "s"
                    && route.subscription_id == "sub"
                    && route.generation == TerminalSubscriptionGeneration(1)
            }),
            "connection-scoped bound routes must survive adapter close"
        );
        assert!(
            registry.connection_bound_route_still_owned(
                "client-a",
                "s",
                "sub",
                TerminalSubscriptionGeneration(1)
            ),
            "close must keep owner and generation for cleanup matching"
        );
    }

    #[test]
    fn cancel_stream_forgets_connection_bound_ledger_and_rejects_stale_owner() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        let (_, handle_a) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound("s", "sub", TerminalSubscriptionGeneration(1), handle_a);
        registry.cancel_stream("s", "sub");
        assert!(
            registry.take_connection_bound_routes("client-a").is_empty(),
            "every cancel path must drop the closing client's ledger entry"
        );

        let replacement = AttachStreamOwner {
            client_id: "client-b".to_string(),
            grant_id: None,
        };
        registry.start_attach(replacement, "s".into(), "sub".into());
        let (_, handle_b) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound("s", "sub", TerminalSubscriptionGeneration(2), handle_b);
        assert!(
            !registry.connection_bound_route_still_owned(
                "client-a",
                "s",
                "sub",
                TerminalSubscriptionGeneration(1)
            ),
            "stale owner+generation must not match the replacement route"
        );
        assert!(registry.connection_bound_route_still_owned(
            "client-b",
            "s",
            "sub",
            TerminalSubscriptionGeneration(2)
        ));
        let recorded_b = registry.take_connection_bound_routes("client-b");
        assert!(recorded_b.iter().any(|route| {
            route.session_id == "s"
                && route.subscription_id == "sub"
                && route.generation == TerminalSubscriptionGeneration(2)
        }));
    }

    #[test]
    fn reconcile_releases_routes_missing_from_core_inventory() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        registry.record_generation("s", "sub", TerminalSubscriptionGeneration(1));
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound("s", "sub", TerminalSubscriptionGeneration(1), handle);
        registry.reconcile_inventory(&[]);
        assert_eq!(registry.stream_owner_client_id("s", "sub"), None);

        registry.start_attach(owner(), "s".into(), "unbound".into());
        registry.reconcile_inventory(&[]);
        assert_eq!(
            registry.stream_owner_client_id("s", "unbound").as_deref(),
            Some("client-a"),
            "unbound routes stay until session retain or explicit detach"
        );
    }

    #[test]
    fn capability_intersection_includes_snapshot_only_when_hello_requires_it() {
        let without = negotiated_unix_capability_set(&[FEATURE_UNIX_TERMINAL_ADAPTER.to_string()])
            .expect("advertised tokens");
        assert!(!without.contains(FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY));
        let with = negotiated_unix_capability_set(&[
            FEATURE_UNIX_TERMINAL_ADAPTER.to_string(),
            FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY.to_string(),
        ])
        .expect("advertised tokens");
        assert!(with.contains(FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY));
    }

    #[test]
    fn attach_stream_source_does_not_branch_on_snapshot_phases() {
        let source = include_str!("daemon_attach_stream.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "attach stream must stay content-blind: found {forbidden}"
            );
        }
    }
}
