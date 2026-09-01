//! Owner registry for subscription-owned attach drains.
//!
//! Core owns incremental frames, FINISH, `attached`, and queued input/resize.
//! Hub authorizes the route and records generation plus adapter-bound flags.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ops::Bound;

use botster_core::{
    ClientId, SessionId, SubscriptionId, TerminalCapabilitySet, TerminalSubscriptionGeneration,
    TerminalSubscriptionRecord,
};
use botster_core_daemon::CoreDaemonError;
use botster_hub_client::{
    DaemonAttachOccupancy, DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonStatus,
    FEATURE_TERMINAL_SUBSCRIPTION_CLOSED, FEATURE_UNIX_TERMINAL_ADAPTER,
    FEATURE_WEBRTC_TERMINAL_ADAPTER,
};
use botster_terminal_protocol::{
    FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY, TerminalCompatibility,
};

use crate::HubDaemon;
use crate::HubRuntime;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon::owner_loop::PendingRuntimeState;
use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapter, UnixTerminalAdapterHandle};
use crate::transport::webrtc::{
    WebRtcConnectionMux, WebRtcTerminalAdapter, WebRtcTerminalAdapterHandle,
};

#[derive(Clone)]
pub(crate) enum BoundAdapterHandle {
    Unix(UnixTerminalAdapterHandle),
    WebRtc(WebRtcTerminalAdapterHandle),
}

pub(crate) fn forward_attach_bootstrap(
    handle: &BoundAdapterHandle,
    egress: &[botster_core::TransportEgress],
) {
    for frame in egress {
        let Ok(bytes) = serde_json::to_vec(frame) else {
            continue;
        };
        let Ok(opaque) = botster_terminal_protocol::TerminalFrame::from_bytes(&bytes) else {
            continue;
        };
        handle.write_opaque_frame(&opaque);
    }
}

impl BoundAdapterHandle {
    pub(crate) fn write_opaque_frame(&self, frame: &botster_terminal_protocol::TerminalFrame) {
        match self {
            Self::Unix(handle) => handle.write_opaque_frame(frame),
            Self::WebRtc(handle) => handle.write_opaque_frame(frame),
        }
    }

    pub(crate) fn close(&self) {
        match self {
            Self::Unix(handle) => handle.close(),
            Self::WebRtc(handle) => handle.close(),
        }
    }

    pub(crate) fn close_from_host(&self) {
        match self {
            Self::Unix(handle) => handle.close_from_host(),
            Self::WebRtc(handle) => handle.close_from_host(),
        }
    }
}

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
    adapter: Option<BoundAdapterHandle>,
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
        crate::admission::peer_generation::grant_ids_match(self.owner.grant_id.as_deref(), grant_id)
    }

    fn close_adapter(&mut self) {
        if let Some(adapter) = self.adapter.take() {
            adapter.close_from_host();
        }
        self.adapter_bound = false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InventoryReconcileProgress {
    pub validated: usize,
    pub more: bool,
    pub after: Option<(String, String)>,
}

#[derive(Default)]
pub(crate) struct AttachStreamRegistry {
    streams: BTreeMap<(String, String), AttachStream>,
    pub(crate) active_subscriptions: BTreeMap<String, BTreeSet<String>>,
    pub(crate) attach_owner_grant_ids: BTreeMap<(String, String), String>,
    pub(crate) live_attach_routes: BTreeSet<(String, String)>,
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
    ) -> Result<Vec<botster_core::TransportEgress>, CoreDaemonError> {
        let Some(client_id) = self.stream_owner_client_id(session_id, subscription_id) else {
            return Ok(Vec::new());
        };
        let attached = runtime.attach_client(
            ClientId(client_id),
            SessionId(session_id.to_string()),
            SubscriptionId(subscription_id.to_string()),
            now_seconds,
        )?;
        Ok(attached
            .client_egress
            .into_iter()
            .map(|(_, egress)| egress)
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
        self.close_adapter(session_id, subscription_id);
        self.remove_stream_metadata(session_id, subscription_id);
    }

    pub(crate) fn forget_stream_without_adapter_close(
        &mut self,
        session_id: &str,
        subscription_id: &str,
    ) {
        self.forget_connection_bound_route(session_id, subscription_id);
        self.remove_stream_metadata(session_id, subscription_id);
    }

    fn remove_stream_metadata(&mut self, session_id: &str, subscription_id: &str) {
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
        let key = (session_id.to_string(), subscription_id.to_string());
        let Some(stream) = self.streams.get(&key) else {
            return;
        };
        let client_id = stream.owner.client_id.clone();
        let generation = stream.generation;
        let Some(routes) = self.connection_bound_routes.get_mut(&client_id) else {
            return;
        };
        match generation {
            Some(generation) => {
                routes.remove(&ConnectionBoundRoute {
                    session_id: session_id.to_string(),
                    subscription_id: subscription_id.to_string(),
                    generation,
                });
            }
            None => {
                routes.retain(|route| {
                    route.session_id != session_id || route.subscription_id != subscription_id
                });
            }
        }
        if routes.is_empty() {
            self.connection_bound_routes.remove(&client_id);
        }
    }

    pub(crate) fn retain_sessions_present_in(&mut self, present: impl Fn(&str) -> bool) {
        let stale: Vec<(String, String)> = self
            .streams
            .keys()
            .filter(|(session_id, _)| !present(session_id))
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

    #[allow(dead_code)]
    pub(crate) fn bound_routes(&self) -> Vec<(String, String, BoundAdapterHandle)> {
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub(crate) fn close_adapters_for_client(&mut self, client_id: &str) {
        let keys = self.bound_route_keys_for_client(client_id);
        for (session_id, subscription_id) in keys {
            self.close_adapter(&session_id, &subscription_id);
        }
    }

    pub(crate) fn bound_route_keys_for_grant(&self, grant_id: &str) -> BTreeSet<(String, String)> {
        self.streams
            .iter()
            .filter(|(_, stream)| {
                stream.owner.grant_id.as_deref() == Some(grant_id) && stream.adapter_bound
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(crate) fn close_adapters_for_grant(&mut self, grant_id: &str) {
        let keys = self.bound_route_keys_for_grant(grant_id);
        for (session_id, subscription_id) in keys {
            self.close_adapter(&session_id, &subscription_id);
        }
    }

    pub(crate) fn bound_route_keys_for_session(
        &self,
        session_id: &str,
    ) -> BTreeSet<(String, String)> {
        self.streams
            .iter()
            .filter(|((bound_session, _), stream)| {
                bound_session == session_id && stream.adapter_bound
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(crate) fn close_adapters_for_session(&mut self, session_id: &str) {
        let keys = self.bound_route_keys_for_session(session_id);
        for (bound_session, subscription_id) in keys {
            self.close_adapter(&bound_session, &subscription_id);
        }
    }

    #[cfg(test)]
    pub(crate) fn reconcile_inventory(&mut self, inventory: &[TerminalSubscriptionRecord]) {
        let lookup = |session_id: &str, subscription_id: &str| {
            inventory.iter().find_map(|row| {
                (row.session_id.0 == session_id && row.subscription_id.0 == subscription_id)
                    .then_some(row.generation)
            })
        };
        let _ = self.reconcile_inventory_slice(lookup, None, usize::MAX);
    }

    /// Close a bound route when Core membership is absent or the generation
    /// mismatches the recorded stream generation.
    #[must_use]
    pub(crate) fn route_is_stale_against_live_generation(
        stream_generation: Option<TerminalSubscriptionGeneration>,
        live: Option<TerminalSubscriptionGeneration>,
    ) -> bool {
        match live {
            None => true,
            Some(live_generation) => {
                stream_generation.is_some_and(|generation| generation != live_generation)
            }
        }
    }

    /// Visit at most `max_entries` stream-map rows after `after`, exclusive.
    /// Unbound rows count toward the visit budget and advance the cursor.
    pub(crate) fn reconcile_inventory_slice(
        &mut self,
        mut lookup: impl FnMut(&str, &str) -> Option<TerminalSubscriptionGeneration>,
        after: Option<(String, String)>,
        max_entries: usize,
    ) -> InventoryReconcileProgress {
        let start = match after.as_ref() {
            Some(after_key) => Bound::Excluded(after_key.clone()),
            None => Bound::Unbounded,
        };
        let mut visit = Vec::new();
        let mut more = false;
        for (key, stream) in self.streams.range((start, Bound::Unbounded)) {
            if visit.len() >= max_entries {
                more = true;
                break;
            }
            visit.push((key.clone(), stream.adapter_bound));
        }
        let mut last = after;
        let mut validated = 0;
        for ((session_id, subscription_id), adapter_bound) in visit {
            last = Some((session_id.clone(), subscription_id.clone()));
            if !adapter_bound {
                continue;
            }
            validated += 1;
            let stream_generation = self
                .streams
                .get(&(session_id.clone(), subscription_id.clone()))
                .and_then(|stream| stream.generation);
            let live = lookup(&session_id, &subscription_id);
            if Self::route_is_stale_against_live_generation(stream_generation, live) {
                self.close_adapter(&session_id, &subscription_id);
                self.cancel_stream(&session_id, &subscription_id);
            }
        }
        InventoryReconcileProgress {
            validated,
            more,
            after: last,
        }
    }

    pub(crate) fn recorded_generation(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<TerminalSubscriptionGeneration> {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .and_then(|stream| stream.generation)
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
        adapter: BoundAdapterHandle,
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

    #[allow(dead_code)]
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

pub(crate) fn negotiated_unix_capability_set(
    _required_features: &[String],
    terminal_requirement: Option<&botster_terminal_protocol::TerminalCompatibilityRequirement>,
) -> Result<TerminalCapabilitySet, botster_core::TerminalCapabilitySetError> {
    let include_snapshot = terminal_requirement.is_some_and(|requirement| {
        requirement
            .required_features
            .iter()
            .any(|feature| feature == FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY)
    });
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

#[allow(dead_code)]
pub(crate) fn hello_requires_unix_adapter(required_features: &[String]) -> bool {
    required_features
        .iter()
        .any(|feature| feature == FEATURE_UNIX_TERMINAL_ADAPTER)
}

#[allow(dead_code)]
pub(crate) fn hello_requires_webrtc_adapter(required_features: &[String]) -> bool {
    required_features
        .iter()
        .any(|feature| feature == FEATURE_WEBRTC_TERMINAL_ADAPTER)
}

pub(crate) fn hello_requires_terminal_subscription_closed(required_features: &[String]) -> bool {
    required_features
        .iter()
        .any(|feature| feature == FEATURE_TERMINAL_SUBSCRIPTION_CLOSED)
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

pub(crate) fn fail_closed_pre_bind_attach(
    registry: &mut AttachStreamRegistry,
    runtime: &mut HubRuntime,
    client_id: &str,
    session_id: &str,
    subscription_id: &str,
    now_seconds: u64,
    adapter: Option<BoundAdapterHandle>,
) {
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
}

pub(crate) struct UnixBindRequest<'a> {
    pub client_id: &'a str,
    pub session_id: &'a str,
    pub subscription_id: &'a str,
    pub capabilities: TerminalCapabilitySet,
    pub now_seconds: u64,
    pub mux: Option<&'a UnixConnectionMux>,
}

pub(crate) fn bind_unix_adapter_after_attaching(
    registry: &mut AttachStreamRegistry,
    runtime: &mut HubRuntime,
    request: UnixBindRequest<'_>,
) -> Result<Option<UnixTerminalAdapterHandle>, ()> {
    let inventory = runtime.list_terminal_subscriptions();
    let Some(generation) = live_generation_for_route(
        &inventory,
        request.client_id,
        request.session_id,
        request.subscription_id,
    ) else {
        fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            None,
        );
        return Err(());
    };
    registry.record_generation(request.session_id, request.subscription_id, generation);
    let capabilities = request.capabilities.clone();
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
        fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            Some(BoundAdapterHandle::Unix(handle)),
        );
        return Err(());
    }
    registry.mark_adapter_bound(
        request.session_id,
        request.subscription_id,
        generation,
        BoundAdapterHandle::Unix(handle.clone()),
    );
    if let Some(mux) = request.mux {
        mux.register(
            request.session_id.to_string(),
            request.subscription_id.to_string(),
            generation.0,
            handle.clone(),
        );
    }
    Ok(Some(handle))
}

pub(crate) struct WebrtcBindRequest<'a> {
    pub client_id: &'a str,
    pub session_id: &'a str,
    pub subscription_id: &'a str,
    pub required_features: &'a [String],
    pub terminal_requirement:
        Option<&'a botster_terminal_protocol::TerminalCompatibilityRequirement>,
    pub now_seconds: u64,
    pub mux: Option<&'a WebRtcConnectionMux>,
}

pub(crate) fn bind_webrtc_adapter_after_attaching(
    registry: &mut AttachStreamRegistry,
    runtime: &mut HubRuntime,
    request: WebrtcBindRequest<'_>,
) -> Result<Option<WebRtcTerminalAdapterHandle>, ()> {
    let inventory = runtime.list_terminal_subscriptions();
    let Some(generation) = live_generation_for_route(
        &inventory,
        request.client_id,
        request.session_id,
        request.subscription_id,
    ) else {
        fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            None,
        );
        return Err(());
    };
    registry.record_generation(request.session_id, request.subscription_id, generation);
    let capabilities = match negotiated_unix_capability_set(
        request.required_features,
        request.terminal_requirement,
    ) {
        Ok(capabilities) => capabilities,
        Err(_) => {
            fail_closed_pre_bind_attach(
                registry,
                runtime,
                request.client_id,
                request.session_id,
                request.subscription_id,
                request.now_seconds,
                None,
            );
            return Err(());
        }
    };
    let (adapter, handle) = match request.mux {
        Some(mux) => mux.create_adapter(),
        None => WebRtcTerminalAdapter::pair(),
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
        fail_closed_pre_bind_attach(
            registry,
            runtime,
            request.client_id,
            request.session_id,
            request.subscription_id,
            request.now_seconds,
            Some(BoundAdapterHandle::WebRtc(handle)),
        );
        return Err(());
    }
    registry.mark_adapter_bound(
        request.session_id,
        request.subscription_id,
        generation,
        BoundAdapterHandle::WebRtc(handle.clone()),
    );
    if let Some(mux) = request.mux {
        mux.register(
            request.session_id.to_string(),
            request.subscription_id.to_string(),
            generation.0,
            handle.clone(),
        );
    }
    Ok(Some(handle))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachedSubscription {
    pub session_id: String,
    pub subscription_id: String,
}

#[derive(Clone)]
pub(crate) enum AttachedSubscriptionChange {
    Attach(AttachedSubscription),
    Detach(AttachedSubscription),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnixEofAblation {
    None,
    LeaveRoute,
    SkipCoreDetach,
    PairOnlyDetach,
}

pub(crate) fn unix_eof_cleanup_ablation() -> UnixEofAblation {
    if env::var("BOTSTER_ENV").as_deref() != Ok("test") {
        return UnixEofAblation::None;
    }
    match env::var("BOTSTER_HUB_UNIX_EOF_ABLATION").as_deref() {
        Ok("leave_route") => UnixEofAblation::LeaveRoute,
        Ok("skip_core_detach") => UnixEofAblation::SkipCoreDetach,
        Ok("pair_only_detach") => UnixEofAblation::PairOnlyDetach,
        _ => UnixEofAblation::None,
    }
}

pub(crate) fn overlay_live_attach_occupancy(
    status: &mut DaemonStatus,
    daemon: &HubDaemon,
    hub_routes: &BTreeSet<(String, String)>,
    pending: &PendingRuntimeState,
) {
    status.live_attach_occupancy = live_attach_occupancy_rows(
        hub_routes,
        daemon
            .runtime()
            .map(crate::HubRuntime::list_terminal_subscriptions)
            .unwrap_or_default()
            .as_slice(),
        pending,
    );
}

pub(crate) fn live_attach_occupancy_rows(
    hub_routes: &BTreeSet<(String, String)>,
    inventory: &[TerminalSubscriptionRecord],
    pending: &PendingRuntimeState,
) -> Vec<DaemonAttachOccupancy> {
    let mut rows = BTreeMap::new();
    for row in inventory {
        rows.insert(
            (row.session_id.0.clone(), row.subscription_id.0.clone()),
            row.generation.0,
        );
    }
    for (session_id, subscription_id) in hub_routes {
        rows.entry((session_id.clone(), subscription_id.clone()))
            .or_insert_with(|| {
                pending
                    .recorded_generation(session_id, subscription_id)
                    .map(|generation: TerminalSubscriptionGeneration| generation.0)
                    .unwrap_or(0)
            });
    }
    rows.into_iter()
        .map(
            |((session_id, subscription_id), generation)| DaemonAttachOccupancy {
                session_id,
                subscription_id,
                generation,
            },
        )
        .collect()
}

pub(crate) fn apply_attached_subscription_change(
    attached_subscriptions: &mut Vec<AttachedSubscription>,
    active_change: Option<AttachedSubscriptionChange>,
) {
    match active_change {
        Some(AttachedSubscriptionChange::Attach(subscription)) => {
            if !attached_subscriptions.contains(&subscription) {
                attached_subscriptions.push(subscription);
            }
        }
        Some(AttachedSubscriptionChange::Detach(subscription)) => {
            attached_subscriptions.retain(|attached| attached != &subscription);
        }
        None => {}
    }
}

pub(crate) fn record_attached_subscription_change(
    registry: &mut AttachStreamRegistry,
    close: &mut crate::subscription::closed_events::AttachCloseBookkeeping,
    lifecycle: &mut botster_hub_client::DaemonLifecycleCounters,
    change: Option<AttachedSubscriptionChange>,
    owner_grant_id: Option<&str>,
) {
    let Some(change) = change else {
        return;
    };
    match change {
        AttachedSubscriptionChange::Attach(subscription) => {
            let route = (
                subscription.session_id.clone(),
                subscription.subscription_id.clone(),
            );
            let inserted = registry.live_attach_routes.insert(route.clone());
            if !inserted && lifecycle.live_attach_subscriptions > 0 {
                return;
            }
            if close.released_attach_generations > 0 {
                close.released_attach_generations -= 1;
                lifecycle.reconnect_registrations =
                    lifecycle.reconnect_registrations.saturating_add(1);
            }
            lifecycle.live_attach_subscriptions =
                lifecycle.live_attach_subscriptions.saturating_add(1);
            lifecycle.high_water_attach_subscriptions = lifecycle
                .high_water_attach_subscriptions
                .max(lifecycle.live_attach_subscriptions);
            if let Some(grant_id) = owner_grant_id {
                registry
                    .attach_owner_grant_ids
                    .insert(route, grant_id.to_string());
            }
        }
        AttachedSubscriptionChange::Detach(subscription) => {
            let route = (subscription.session_id, subscription.subscription_id);
            if !registry.live_attach_routes.remove(&route) {
                return;
            }
            lifecycle.live_attach_subscriptions =
                lifecycle.live_attach_subscriptions.saturating_sub(1);
            close.released_attach_generations = close.released_attach_generations.saturating_add(1);
            registry.attach_owner_grant_ids.remove(&route);
        }
    }
}

pub(crate) fn response_records_attach_ownership(response: &DaemonResponse) -> bool {
    response.kind != DaemonResponseKind::OperatorError
}

pub(crate) fn attached_subscription_change_for_response(
    request: &DaemonRequest,
    response: &DaemonResponse,
) -> Option<AttachedSubscriptionChange> {
    if response.kind == DaemonResponseKind::OperatorError {
        return None;
    }
    AttachedSubscriptionChange::from_request(request)
}

impl AttachedSubscriptionChange {
    fn from_request(request: &DaemonRequest) -> Option<Self> {
        match request {
            DaemonRequest::Attach {
                session_id,
                subscription_id,
            } => Some(Self::Attach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            DaemonRequest::Detach {
                session_id,
                subscription_id,
            } => Some(Self::Detach(AttachedSubscription {
                session_id: session_id.clone(),
                subscription_id: subscription_id.clone(),
            })),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::{
        CoreSessionMetadata, ResizePayload, SessionSpawnRequest, SpawnEnvironment,
        SpawnWorkingDirectory,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn owner() -> AttachStreamOwner {
        AttachStreamOwner {
            client_id: "client-a".to_string(),
            grant_id: None,
        }
    }

    #[test]
    fn ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let data_directory = std::env::temp_dir().join(format!(
            "hub-adapter-ingress-loss-{}-{nonce}",
            std::process::id()
        ));
        let config = crate::HubStartupOptions {
            host: crate::HostIdentityOptions {
                id: "adapter-ingress-loss".to_string(),
                display_name: "Adapter ingress loss".to_string(),
                fingerprint: None,
            },
            data_directory: crate::DataDirectoryOption::Explicit(data_directory.clone()),
            session_defaults: crate::SessionDefaults {
                shell: "/bin/sh".to_string(),
                working_directory: Some(".".into()),
                initial_rows: 24,
                initial_cols: 80,
            },
            transports: crate::TransportBindings::default(),
            ..crate::HubStartupOptions::default()
        }
        .build_config_for_environment(&crate::RuntimeEnvironment::from_values(None, None))
        .expect("config");
        let mut runtime = HubRuntime::new(config);
        let session_id = SessionId("ingress-loss-session".to_string());
        runtime
            .spawn_session(
                SessionSpawnRequest {
                    request_id: botster_core::RequestId("ingress-loss-spawn".to_string()),
                    session_id: session_id.clone(),
                    executable: "/bin/sleep".to_string(),
                    arguments: vec!["8".to_string()],
                    working_directory: SpawnWorkingDirectory {
                        path: ".".to_string(),
                    },
                    environment: SpawnEnvironment::default(),
                    initial_pty_size: Some(ResizePayload { rows: 24, cols: 80 }),
                },
                CoreSessionMetadata::new(),
                1,
            )
            .expect("spawn session");

        let mut registry = AttachStreamRegistry::default();
        for (client_id, subscription_id) in [
            ("lost-client", "lost-subscription"),
            ("sibling-client", "sibling-subscription"),
        ] {
            registry.start_attach(
                AttachStreamOwner {
                    client_id: client_id.to_string(),
                    grant_id: None,
                },
                session_id.0.clone(),
                subscription_id.to_string(),
            );
            registry
                .begin_core_attach(&mut runtime, &session_id.0, subscription_id, 2)
                .expect("attach through Core");
        }

        let mux = UnixConnectionMux::new();
        let capabilities = negotiated_unix_capability_set(&[], None).expect("capabilities");
        for (client_id, subscription_id) in [
            ("lost-client", "lost-subscription"),
            ("sibling-client", "sibling-subscription"),
        ] {
            bind_unix_adapter_after_attaching(
                &mut registry,
                &mut runtime,
                UnixBindRequest {
                    client_id,
                    session_id: &session_id.0,
                    subscription_id,
                    capabilities: capabilities.clone(),
                    now_seconds: 3,
                    mux: Some(&mux),
                },
            )
            .expect("bind through Hub")
            .expect("bound handle");
        }

        let lost_generation = registry
            .recorded_generation(&session_id.0, "lost-subscription")
            .expect("lost generation");
        let sibling_generation = registry
            .recorded_generation(&session_id.0, "sibling-subscription")
            .expect("sibling generation");
        let lost = mux
            .route_handle(&session_id.0, "lost-subscription", lost_generation.0)
            .expect("production-registered lost handle");
        let sibling = mux
            .route_handle(&session_id.0, "sibling-subscription", sibling_generation.0)
            .expect("production-registered sibling handle");

        lost.mark_ingress_lost();
        // This is the guarded Hub test seam, not a production Hub loop. It proves
        // Core hard-stop behavior and Hub adapter isolation, not production reachability.
        runtime
            .drain_runtime_once(&session_id, 4)
            .expect("drive real Core ingress intake");

        let inventory = runtime.list_terminal_subscriptions();
        assert!(
            !inventory.iter().any(|row| {
                row.session_id == session_id
                    && row.subscription_id.0 == "lost-subscription"
                    && row.generation == lost_generation
            }),
            "Core must retire exactly the route whose adapter reported loss"
        );
        assert!(lost.is_closed(), "hard stop must close the lost adapter");
        assert!(
            inventory.iter().any(|row| {
                row.session_id == session_id
                    && row.subscription_id.0 == "sibling-subscription"
                    && row.generation == sibling_generation
                    && row.adapter_bound
            }),
            "the sibling route must remain bound"
        );
        assert!(!sibling.is_closed(), "the sibling adapter must stay live");
        let output = botster_terminal_protocol::TerminalFrame::from_bytes(
            br#"{"type":"terminal_output","marker":"sibling-live"}"#,
        )
        .expect("opaque terminal output");
        sibling.write_opaque_frame(&output);
        assert!(
            sibling.snapshot_active().is_some(),
            "the surviving sibling must still accept terminal output"
        );

        runtime
            .shutdown_session(session_id, 5)
            .expect("shutdown session");
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn drain_does_not_change_attach_occupancy() {
        let mut registry = AttachStreamRegistry::default();
        let mut close = crate::subscription::closed_events::AttachCloseBookkeeping::default();
        let mut lifecycle = botster_hub_client::DaemonLifecycleCounters::default();
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            })),
            None,
        );
        assert_eq!(lifecycle.live_attach_subscriptions, 1);

        let drain = DaemonRequest::drain_subscription("session", "subscription");
        let drain_ok = crate::client_api_dto::response::daemon_events(Vec::new());
        assert!(attached_subscription_change_for_response(&drain, &drain_ok).is_none());
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            attached_subscription_change_for_response(&drain, &drain_ok),
            None,
        );
        assert_eq!(lifecycle.live_attach_subscriptions, 1);

        let detach = DaemonRequest::Detach {
            session_id: "session".to_string(),
            subscription_id: "subscription".to_string(),
        };
        let change = attached_subscription_change_for_response(&detach, &drain_ok);
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            change.clone(),
            None,
        );
        assert_eq!(lifecycle.live_attach_subscriptions, 0);
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            change,
            None,
        );
        assert_eq!(
            lifecycle.live_attach_subscriptions, 0,
            "a second Detach must not decrement another route"
        );
        assert!(
            !registry
                .live_attach_routes
                .contains(&("session".to_string(), "subscription".to_string()))
        );
    }

    #[test]
    fn occupancy_rows_union_hub_routes_and_core_inventory() {
        let mut hub_routes = BTreeSet::new();
        hub_routes.insert(("session".to_string(), "hub-only".to_string()));
        let inventory = vec![TerminalSubscriptionRecord {
            client_id: ClientId("client".to_string()),
            session_id: SessionId("session".to_string()),
            subscription_id: SubscriptionId("core-only".to_string()),
            generation: TerminalSubscriptionGeneration(4),
            adapter_bound: false,
            capabilities: None,
        }];
        let rows = live_attach_occupancy_rows(
            &hub_routes,
            &inventory,
            &crate::daemon::owner_loop::PendingRuntimeState::default(),
        );
        assert!(
            rows.iter()
                .any(|row| row.session_id == "session" && row.subscription_id == "hub-only"),
            "Hub-only occupancy must stay visible: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row.session_id == "session"
                    && row.subscription_id == "core-only"
                    && row.generation == 4
            }),
            "Core-only occupancy must stay visible: {rows:?}"
        );
    }

    #[test]
    fn independent_counter_sub_does_not_clear_named_occupancy() {
        let mut registry = AttachStreamRegistry::default();
        let mut close = crate::subscription::closed_events::AttachCloseBookkeeping::default();
        let mut lifecycle = botster_hub_client::DaemonLifecycleCounters::default();
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            Some(AttachedSubscriptionChange::Attach(AttachedSubscription {
                session_id: "session".to_string(),
                subscription_id: "subscription".to_string(),
            })),
            None,
        );
        lifecycle.live_attach_subscriptions = 0;
        let rows = live_attach_occupancy_rows(
            &registry.live_attach_routes,
            &[],
            &crate::daemon::owner_loop::PendingRuntimeState::default(),
        );
        assert!(
            rows.iter().any(|row| {
                row.session_id == "session" && row.subscription_id == "subscription"
            }),
            "named occupancy is the oracle, not the counter: {rows:?}"
        );
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
    fn bound_route_keys_are_captured_before_close_clears_the_flag() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "s",
            "sub",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(handle),
        );
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
    fn close_adapters_for_session_closes_only_that_session() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "keep".into(), "sub-keep".into());
        registry.start_attach(owner(), "drop".into(), "sub-drop".into());
        let (_, keep_handle) = UnixTerminalAdapter::pair();
        let (_, drop_handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "keep",
            "sub-keep",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(keep_handle),
        );
        registry.mark_adapter_bound(
            "drop",
            "sub-drop",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(drop_handle),
        );
        let keys = registry.bound_route_keys_for_session("drop");
        registry.close_adapters_for_session("drop");
        assert!(keys.contains(&("drop".to_string(), "sub-drop".to_string())));
        assert!(
            !registry.is_adapter_bound("drop", "sub-drop"),
            "session close must close that session adapter"
        );
        assert!(
            registry.is_adapter_bound("keep", "sub-keep"),
            "session close must not close a sibling session adapter"
        );
    }

    #[test]
    fn cancel_stream_forgets_connection_bound_ledger_and_rejects_stale_owner() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        let (_, handle_a) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "s",
            "sub",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(handle_a),
        );
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
        registry.mark_adapter_bound(
            "s",
            "sub",
            TerminalSubscriptionGeneration(2),
            BoundAdapterHandle::Unix(handle_b),
        );
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
    fn cancel_stream_removes_one_client_route_without_touching_sibling_ledgers() {
        let mut registry = AttachStreamRegistry::default();
        for index in 0..32 {
            let owner = AttachStreamOwner {
                client_id: format!("client-{index:02}"),
                grant_id: None,
            };
            let session = format!("s-{index:02}");
            registry.start_attach(owner, session.clone(), "sub".into());
            let (_, handle) = UnixTerminalAdapter::pair();
            registry.mark_adapter_bound(
                &session,
                "sub",
                TerminalSubscriptionGeneration(1),
                BoundAdapterHandle::Unix(handle),
            );
        }
        registry.cancel_stream("s-07", "sub");
        assert!(
            registry
                .take_connection_bound_routes("client-07")
                .is_empty()
        );
        for index in 0..32 {
            if index == 7 {
                continue;
            }
            let session = format!("s-{index:02}");
            assert!(
                registry.connection_bound_route_still_owned(
                    &format!("client-{index:02}"),
                    &session,
                    "sub",
                    TerminalSubscriptionGeneration(1)
                ),
                "removing one stale row must not walk or clear other clients"
            );
        }
    }

    #[test]
    fn reconcile_releases_routes_missing_from_core_inventory() {
        let mut registry = AttachStreamRegistry::default();
        registry.start_attach(owner(), "s".into(), "sub".into());
        registry.record_generation("s", "sub", TerminalSubscriptionGeneration(1));
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "s",
            "sub",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(handle),
        );
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
    fn reconcile_slice_closes_absence_and_generation_mismatch() {
        let mut registry = AttachStreamRegistry::default();
        for (session, generation) in [("a", 1), ("b", 2), ("c", 3)] {
            registry.start_attach(owner(), session.into(), "sub".into());
            let (_, handle) = UnixTerminalAdapter::pair();
            registry.mark_adapter_bound(
                session,
                "sub",
                TerminalSubscriptionGeneration(generation),
                BoundAdapterHandle::Unix(handle),
            );
        }
        let live = |session: &str, _: &str| match session {
            "a" => Some(TerminalSubscriptionGeneration(1)),
            "b" => Some(TerminalSubscriptionGeneration(9)),
            _ => None,
        };
        let first = registry.reconcile_inventory_slice(live, None, 2);
        assert_eq!(first.validated, 2);
        assert!(first.more);
        assert_eq!(
            registry.stream_owner_client_id("a", "sub").as_deref(),
            Some("client-a")
        );
        assert_eq!(registry.stream_owner_client_id("b", "sub"), None);
        let rebound = owner();
        registry.start_attach(rebound, "c".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "c",
            "sub",
            TerminalSubscriptionGeneration(4),
            BoundAdapterHandle::Unix(handle),
        );
        let second = registry.reconcile_inventory_slice(
            |session, _| (session == "c").then_some(TerminalSubscriptionGeneration(4)),
            first.after,
            8,
        );
        assert_eq!(second.validated, 1);
        assert!(!second.more);
        assert_eq!(
            registry.stream_owner_client_id("c", "sub").as_deref(),
            Some("client-a"),
            "a newer live generation must not close against a stale cursor expectation"
        );
    }

    #[test]
    fn reconcile_slice_bounds_unbound_and_pre_cursor_prefixes() {
        let mut registry = AttachStreamRegistry::default();
        for index in 0..10 {
            registry.start_attach(owner(), format!("unbound-{index:02}"), "sub".into());
        }
        registry.start_attach(owner(), "z-bound".into(), "sub".into());
        let (_, handle) = UnixTerminalAdapter::pair();
        registry.mark_adapter_bound(
            "z-bound",
            "sub",
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(handle),
        );
        let mut lookups = 0;
        let first = registry.reconcile_inventory_slice(
            |_, _| {
                lookups += 1;
                Some(TerminalSubscriptionGeneration(1))
            },
            None,
            8,
        );
        assert_eq!(first.validated, 0);
        assert_eq!(lookups, 0, "unbound prefix must not call membership lookup");
        assert!(first.more);
        assert_eq!(
            first.after.as_ref().map(|(session, _)| session.as_str()),
            Some("unbound-07")
        );
        let second = registry.reconcile_inventory_slice(
            |session, _| {
                lookups += 1;
                assert_eq!(session, "z-bound");
                Some(TerminalSubscriptionGeneration(1))
            },
            first.after,
            8,
        );
        assert_eq!(second.validated, 1);
        assert_eq!(lookups, 1);
        assert!(!second.more);
        assert_eq!(
            registry.stream_owner_client_id("z-bound", "sub").as_deref(),
            Some("client-a")
        );
    }

    #[test]
    fn capability_intersection_includes_snapshot_only_when_hello_requires_it() {
        let without =
            negotiated_unix_capability_set(&[FEATURE_UNIX_TERMINAL_ADAPTER.to_string()], None)
                .expect("advertised tokens");
        assert!(!without.contains(FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY));
        let with = negotiated_unix_capability_set(
            &[
                FEATURE_UNIX_TERMINAL_ADAPTER.to_string(),
                FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY.to_string(),
            ],
            None,
        )
        .expect("advertised tokens");
        assert!(
            !with.contains(FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY),
            "host Hello tokens must not grant snapshot capability"
        );
        let from_terminal = negotiated_unix_capability_set(
            &[FEATURE_UNIX_TERMINAL_ADAPTER.to_string()],
            Some(&botster_terminal_protocol::TerminalCompatibilityRequirement::for_ready_then_history_attach()),
        )
        .expect("terminal requirement tokens");
        assert!(from_terminal.contains(FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY));
    }

    #[test]
    fn attach_stream_source_does_not_branch_on_snapshot_phases() {
        let source = include_str!("attach_routes.rs");
        let production = source.split("mod tests").next().expect("production source");
        for forbidden in [r#""READY""#, r#""PAGE""#, r#""FINISH""#, "GHOSTSNP"] {
            assert!(
                !production.contains(forbidden),
                "attach stream must stay content-blind: found {forbidden}"
            );
        }
    }
}
