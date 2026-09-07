//! Owner registry for subscription-owned attach drains.
//!
//! Core owns incremental frames, FINISH, `attached`, and queued input/resize.
//! Hub authorizes the route and records generation plus adapter-bound flags.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use botster_core::{
    TerminalCapabilitySet, TerminalSubscriptionGeneration, TerminalSubscriptionRecord,
};
use botster_hub_client::{
    DaemonAttachOccupancy, DaemonRequest, DaemonResponse, DaemonResponseKind, DaemonStatus,
    FEATURE_TERMINAL_SUBSCRIPTION_CLOSED, FEATURE_UNIX_TERMINAL_ADAPTER,
    FEATURE_WEBRTC_TERMINAL_ADAPTER,
};
use botster_terminal_protocol::{
    FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY, TerminalCompatibility,
};

use crate::daemon::owner_loop::PendingRuntimeState;
use crate::transport::unix::UnixTerminalAdapterHandle;
use crate::transport::webrtc::WebRtcTerminalAdapterHandle;

#[derive(Clone)]
pub(crate) enum BoundAdapterHandle {
    Unix(UnixTerminalAdapterHandle),
    WebRtc(WebRtcTerminalAdapterHandle),
}

impl BoundAdapterHandle {
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

/// Outcome of reserving a route key in an owner's route set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteReservation {
    Inserted,
    AlreadyHeld,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachStreamOwner {
    pub client_id: String,
    pub grant_id: Option<String>,
}

impl AttachStreamOwner {
    /// The key attach admission and cleanup budget by: the grant for WebRTC
    /// peers, the connection client id otherwise.
    pub(crate) fn budget_key(&self) -> String {
        self.grant_id
            .clone()
            .unwrap_or_else(|| self.client_id.clone())
    }

    fn matches(&self, other: &AttachStreamOwner) -> bool {
        match self.grant_id.as_deref() {
            Some(grant_id) => other.grant_id.as_deref() == Some(grant_id),
            None => other.grant_id.is_none() && other.client_id == self.client_id,
        }
    }
}

/// Identity of one attach stream: the owning client and the registry epoch
/// assigned when the stream started. A route key can be reused by a
/// replacement stream (same client after a reattach, or another client); the
/// epoch is never reused. Every deferred continuation (attach, bind,
/// cleanup) captures the identity when it starts and must match it before
/// mutating the stream, so a late completion cannot touch a replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachmentIdentity {
    pub client_id: String,
    pub epoch: u64,
}

pub(crate) struct AttachStream {
    owner: AttachStreamOwner,
    epoch: u64,
    generation: Option<TerminalSubscriptionGeneration>,
    adapter_bound: bool,
    adapter: Option<BoundAdapterHandle>,
}

impl AttachStream {
    fn new(owner: AttachStreamOwner, epoch: u64) -> Self {
        Self {
            owner,
            epoch,
            generation: None,
            adapter_bound: false,
            adapter: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn owner_client_id(&self) -> String {
        self.owner.client_id.clone()
    }

    fn identity(&self) -> AttachmentIdentity {
        AttachmentIdentity {
            client_id: self.owner.client_id.clone(),
            epoch: self.epoch,
        }
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
    next_epoch: u64,
    /// Route keys each owner (budget key) reserved for an attach, holds
    /// live, or was told it attached and has not detached. Attach admission
    /// caps this union, and cleanup takes all of it, so candidate vectors
    /// stay bounded.
    owner_routes: BTreeMap<String, BTreeSet<(String, String)>>,
    pub(crate) active_subscriptions: BTreeMap<String, BTreeSet<String>>,
    pub(crate) attach_owner_grant_ids: BTreeMap<(String, String), String>,
    pub(crate) live_attach_routes: BTreeSet<(String, String)>,
    connection_bound_routes: BTreeMap<String, BTreeSet<ConnectionBoundRoute>>,
}

impl AttachStreamRegistry {
    /// Start one attach stream and return its identity. Any earlier stream on
    /// the route is cancelled, so a continuation holding the old identity
    /// finds a mismatch afterwards.
    pub(crate) fn start_attach(
        &mut self,
        owner: AttachStreamOwner,
        session_id: String,
        subscription_id: String,
    ) -> AttachmentIdentity {
        self.cancel_stream(&session_id, &subscription_id);
        if let Some(grant_id) = owner.grant_id.clone() {
            self.attach_owner_grant_ids
                .insert((session_id.clone(), subscription_id.clone()), grant_id);
        }
        self.active_subscriptions
            .entry(session_id.clone())
            .or_default()
            .insert(subscription_id.clone());
        self.next_epoch += 1;
        let stream = AttachStream::new(owner, self.next_epoch);
        let identity = stream.identity();
        self.streams.insert((session_id, subscription_id), stream);
        identity
    }

    pub(crate) fn stream_identity(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<AttachmentIdentity> {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(AttachStream::identity)
    }

    /// True when the route is still owned by exactly this attachment.
    #[must_use]
    pub(crate) fn stream_matches(
        &self,
        session_id: &str,
        subscription_id: &str,
        identity: &AttachmentIdentity,
    ) -> bool {
        self.stream_identity(session_id, subscription_id).as_ref() == Some(identity)
    }

    #[cfg(test)]
    /// Streams one owner holds: by grant for WebRTC, by client otherwise.
    pub(crate) fn stream_count_for_owner(&self, owner: &AttachStreamOwner) -> usize {
        self.streams
            .values()
            .filter(|stream| owner.matches(&stream.owner))
            .count()
    }

    /// True when the route's current stream belongs to `owner`.
    pub(crate) fn stream_owner_matches(
        &self,
        session_id: &str,
        subscription_id: &str,
        owner: &AttachStreamOwner,
    ) -> bool {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .is_some_and(|stream| owner.matches(&stream.owner))
    }

    /// Reserve one route key in the owner's route set before an attach
    /// starts. The set is the union of pending, live, and acknowledged keys,
    /// so concurrent attaches cannot exceed `limit` between admission and
    /// completion.
    #[must_use]
    pub(crate) fn reserve_route(
        &mut self,
        budget_key: &str,
        session_id: &str,
        subscription_id: &str,
        limit: usize,
    ) -> RouteReservation {
        let routes = self.owner_routes.entry(budget_key.to_string()).or_default();
        let key = (session_id.to_string(), subscription_id.to_string());
        if routes.contains(&key) {
            return RouteReservation::AlreadyHeld;
        }
        if routes.len() >= limit {
            if routes.is_empty() {
                self.owner_routes.remove(budget_key);
            }
            return RouteReservation::Full;
        }
        routes.insert(key);
        RouteReservation::Inserted
    }

    /// Release one route key from the owner's set (explicit detach, or an
    /// attach that failed while no stream of this owner holds the key).
    pub(crate) fn release_route(
        &mut self,
        budget_key: &str,
        session_id: &str,
        subscription_id: &str,
    ) {
        if let Some(routes) = self.owner_routes.get_mut(budget_key) {
            routes.remove(&(session_id.to_string(), subscription_id.to_string()));
            if routes.is_empty() {
                self.owner_routes.remove(budget_key);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn owner_route_count(&self, budget_key: &str) -> usize {
        self.owner_routes.get(budget_key).map_or(0, BTreeSet::len)
    }

    /// Take every route key one owner reserved or was told it attached;
    /// cleanup covers them all, so nothing historical outlives the owner.
    pub(crate) fn take_owner_routes(&mut self, budget_key: &str) -> BTreeSet<(String, String)> {
        self.owner_routes.remove(budget_key).unwrap_or_default()
    }

    /// The one validated set of routes departing grants own, keyed by the
    /// departing grant. Sources: the peer-close snapshot, each removed
    /// grant's route set, and every stream owned by a removed grant. A key
    /// whose current stream belongs to anyone else (a Unix client or a live
    /// grant) is a replacement and is excluded from every cleanup mutation.
    /// A key with no stream is attributed to its removed index owner, else
    /// to `primary`.
    pub(crate) fn departing_routes_for_grants(
        &self,
        removed: &BTreeSet<String>,
        primary: &str,
        snapshot: &BTreeSet<(String, String)>,
    ) -> BTreeMap<String, BTreeSet<(String, String)>> {
        // A close for grants that hold nothing (a duplicate close) has no
        // departing routes and therefore no effect on any bookkeeping.
        if removed.is_empty() {
            return BTreeMap::new();
        }
        // Each key keeps the grant it is already known to belong to: a
        // route-set entry belongs to that grant, a stream to its owner. A
        // snapshot key carries no attribution of its own.
        let mut keys: BTreeMap<(String, String), Option<String>> =
            snapshot.iter().map(|key| (key.clone(), None)).collect();
        for grant in removed {
            if let Some(routes) = self.owner_routes.get(grant) {
                for key in routes {
                    keys.insert(key.clone(), Some(grant.clone()));
                }
            }
        }
        for (key, stream) in &self.streams {
            if let Some(grant) = stream
                .owner
                .grant_id
                .as_ref()
                .filter(|grant| removed.contains(*grant))
            {
                keys.insert(key.clone(), Some(grant.clone()));
            }
        }
        let mut departing: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
        for (key, known_owner) in keys {
            let grant = match self.streams.get(&key) {
                Some(stream) => match stream.owner.grant_id.as_deref() {
                    Some(grant) if removed.contains(grant) => grant.to_string(),
                    _ => continue,
                },
                None => match self
                    .attach_owner_grant_ids
                    .get(&key)
                    .filter(|grant| removed.contains(grant.as_str()))
                    .cloned()
                    .or(known_owner)
                {
                    Some(grant) => grant,
                    // An unattributed streamless snapshot key falls back to
                    // the primary only when the primary is being cleaned.
                    None if removed.contains(primary) => primary.to_string(),
                    None => continue,
                },
            };
            departing.entry(grant).or_default().insert(key);
        }
        departing
    }

    /// Streams one client owns that never bound an adapter. Cleanup cancels
    /// them before the Core turn so a late completion sees the mismatch.
    pub(crate) fn unbound_routes_for_client(
        &self,
        client_id: &str,
    ) -> Vec<(String, String, AttachmentIdentity)> {
        self.streams
            .iter()
            .filter(|(_, stream)| stream.owner.client_id == client_id && !stream.adapter_bound)
            .map(|((session_id, subscription_id), stream)| {
                (
                    session_id.clone(),
                    subscription_id.clone(),
                    stream.identity(),
                )
            })
            .collect()
    }

    /// Cancel the stream only when it is still this attachment. Returns
    /// whether anything was removed.
    #[must_use]
    pub(crate) fn cancel_stream_if(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        identity: &AttachmentIdentity,
    ) -> bool {
        if !self.stream_matches(session_id, subscription_id, identity) {
            return false;
        }
        self.cancel_stream(session_id, subscription_id);
        true
    }

    /// Close the bound adapter only when the stream is still this attachment.
    #[must_use]
    pub(crate) fn close_adapter_if(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        identity: &AttachmentIdentity,
    ) -> bool {
        if !self.stream_matches(session_id, subscription_id, identity) {
            return false;
        }
        self.close_adapter(session_id, subscription_id);
        true
    }

    /// Record the Core generation only when the stream is still this
    /// attachment.
    #[must_use]
    pub(crate) fn record_generation_if(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        identity: &AttachmentIdentity,
        generation: TerminalSubscriptionGeneration,
    ) -> bool {
        if !self.stream_matches(session_id, subscription_id, identity) {
            return false;
        }
        self.record_generation(session_id, subscription_id, generation);
        true
    }

    /// Bind the adapter only when the stream is still this attachment. A
    /// `false` return leaves the registry untouched; the caller owns the
    /// handle and the Core generation it was bound with.
    #[must_use]
    pub(crate) fn mark_adapter_bound_if(
        &mut self,
        session_id: &str,
        subscription_id: &str,
        identity: &AttachmentIdentity,
        generation: TerminalSubscriptionGeneration,
        adapter: BoundAdapterHandle,
    ) -> bool {
        if !self.stream_matches(session_id, subscription_id, identity) {
            return false;
        }
        self.mark_adapter_bound(session_id, subscription_id, generation, adapter);
        true
    }

    #[cfg(test)]
    pub(crate) fn stream_owner_client_id(
        &self,
        session_id: &str,
        subscription_id: &str,
    ) -> Option<String> {
        self.streams
            .get(&(session_id.to_string(), subscription_id.to_string()))
            .map(AttachStream::owner_client_id)
    }

    pub(crate) fn cancel_stream(&mut self, session_id: &str, subscription_id: &str) {
        self.forget_connection_bound_route(session_id, subscription_id);
        self.close_adapter(session_id, subscription_id);
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

/// Overlay live attach occupancy from one Core inventory read the caller
/// already holds. Status handlers fetch the inventory through a ticket.
pub(crate) fn overlay_live_attach_occupancy(
    status: &mut DaemonStatus,
    inventory: &[TerminalSubscriptionRecord],
    hub_routes: &BTreeSet<(String, String)>,
    pending: &PendingRuntimeState,
) {
    status.live_attach_occupancy = live_attach_occupancy_rows(hub_routes, inventory, pending);
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
    use crate::HubRuntime;
    use crate::transport::unix::{UnixConnectionMux, UnixTerminalAdapter};
    use botster_core::{
        ClientId, CoreSessionMetadata, ResizePayload, SessionId, SessionSpawnRequest,
        SpawnEnvironment, SpawnWorkingDirectory, SubscriptionId,
    };
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn owner() -> AttachStreamOwner {
        AttachStreamOwner {
            client_id: "client-a".to_string(),
            grant_id: None,
        }
    }

    #[test]
    fn ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling() {
        use crate::runtime::AttachBindPlan;
        use botster_terminal_protocol::{RouteId, RoutedTerminalFrame, encode_output};

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
        let runtime = HubRuntime::new(config).expect("runtime");
        let session_id = SessionId("ingress-loss-session".to_string());
        runtime
            .spawn_session_for_test(
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
            )
            .expect("spawn session");

        let mut registry = AttachStreamRegistry::default();
        let mux = UnixConnectionMux::new();
        let capabilities = negotiated_unix_capability_set(&[], None).expect("capabilities");
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
            let (adapter, handle) = mux.create_adapter();
            // One Core owner turn attaches the route and binds the adapter.
            let generation = runtime
                .attach_and_bind_terminal(AttachBindPlan {
                    client_id: ClientId(client_id.to_string()),
                    session_id: session_id.clone(),
                    subscription_id: SubscriptionId(subscription_id.to_string()),
                    capabilities: capabilities.clone(),
                    now_seconds: 2,
                    adapter: Box::new(adapter),
                })
                .wait(crate::runtime::STARTUP_CORE_WAIT)
                .expect("attach turn")
                .expect("attach and bind through Core");
            registry.mark_adapter_bound(
                &session_id.0,
                subscription_id,
                generation,
                BoundAdapterHandle::Unix(handle.clone()),
            );
            assert!(mux.register(
                session_id.0.clone(),
                subscription_id.to_string(),
                generation.0,
                handle,
            ));
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
        // The production data-plane driver consumes the adapter wake and pumps
        // only the affected Core route. Inventory reads do not advance progress.
        let deadline = Instant::now() + Duration::from_secs(5);
        let inventory = loop {
            let inventory = runtime.list_terminal_subscriptions_for_test();
            let lost_route_present = inventory.iter().any(|row| {
                row.session_id == session_id
                    && row.subscription_id.0 == "lost-subscription"
                    && row.generation == lost_generation
            });
            if !lost_route_present {
                break inventory;
            }
            assert!(
                Instant::now() < deadline,
                "production wake pump did not retire the lost route; last inventory: {inventory:?}"
            );
            thread::sleep(Duration::from_millis(10));
        };
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
        let output = RoutedTerminalFrame::new(
            RouteId::new("sibling-subscription").expect("route"),
            sibling_generation.0,
            0,
            encode_output(b"sibling-live").expect("output frame"),
        );
        sibling.write_opaque_frame(&output);
        assert!(
            sibling.snapshot_active().is_some(),
            "the surviving sibling must still accept terminal output"
        );

        runtime
            .shutdown_session_for_test(session_id)
            .expect("shutdown session");
        let _ = std::fs::remove_dir_all(data_directory);
    }

    #[test]
    fn status_does_not_change_attach_occupancy() {
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

        let status = DaemonRequest::Status;
        let events_ok = crate::client_api_dto::response::daemon_events(Vec::new());
        assert!(attached_subscription_change_for_response(&status, &events_ok).is_none());
        record_attached_subscription_change(
            &mut registry,
            &mut close,
            &mut lifecycle,
            attached_subscription_change_for_response(&status, &events_ok),
            None,
        );
        assert_eq!(lifecycle.live_attach_subscriptions, 1);

        let detach = DaemonRequest::Detach {
            session_id: "session".to_string(),
            subscription_id: "subscription".to_string(),
        };
        let change = attached_subscription_change_for_response(&detach, &events_ok);
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

    /// H2: the attaching connection is cleaned up before its deferred attach
    /// completes. The late completion holds the original identity and must
    /// not bind, and a replacement on the same route key is untouched.
    #[test]
    fn late_attach_completion_after_disconnect_cannot_bind_or_touch_a_replacement() {
        let mut registry = AttachStreamRegistry::default();
        let stale = registry.start_attach(owner(), "s".into(), "sub".into());
        // Connection cleanup cancels the client's unbound streams first.
        let pending = registry.unbound_routes_for_client("client-a");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].2, stale);
        assert!(registry.cancel_stream_if("s", "sub", &stale));
        assert!(!registry.stream_matches("s", "sub", &stale));

        // A replacement client attaches the same route key.
        let replacement = registry.start_attach(
            AttachStreamOwner {
                client_id: "client-b".to_string(),
                grant_id: None,
            },
            "s".into(),
            "sub".into(),
        );
        assert_ne!(replacement, stale);
        assert_ne!(replacement.epoch, stale.epoch);

        // The stale continuation now completes: every checked mutation refuses.
        let (_, handle) = UnixTerminalAdapter::pair();
        assert!(!registry.record_generation_if(
            "s",
            "sub",
            &stale,
            TerminalSubscriptionGeneration(7)
        ));
        assert!(!registry.mark_adapter_bound_if(
            "s",
            "sub",
            &stale,
            TerminalSubscriptionGeneration(7),
            BoundAdapterHandle::Unix(handle),
        ));
        assert!(!registry.cancel_stream_if("s", "sub", &stale));
        assert!(!registry.close_adapter_if("s", "sub", &stale));
        assert!(registry.stream_matches("s", "sub", &replacement));
        assert!(!registry.is_adapter_bound("s", "sub"));
        assert_eq!(registry.recorded_generation("s", "sub"), None);
        assert_eq!(
            registry.stream_owner_client_id("s", "sub").as_deref(),
            Some("client-b")
        );
        assert!(registry.take_connection_bound_routes("client-a").is_empty());
    }

    /// H3: cleanup for a closed connection reports back after a replacement
    /// bound the same route key. Identity-checked close and cancel must leave
    /// the replacement's adapter bound; the same identity rule applies to the
    /// same client reattaching (a new epoch) as to a different client.
    #[test]
    fn delayed_old_cleanup_does_not_close_a_replacement_route() {
        for replacement_client in ["client-a", "client-b"] {
            let mut registry = AttachStreamRegistry::default();
            let old = registry.start_attach(owner(), "s".into(), "sub".into());
            // Keep the adapter halves alive: dropping an adapter closes its
            // slot, which would make `is_closed` true without any host close.
            let (_old_adapter, old_handle) = UnixTerminalAdapter::pair();
            assert!(registry.mark_adapter_bound_if(
                "s",
                "sub",
                &old,
                TerminalSubscriptionGeneration(1),
                BoundAdapterHandle::Unix(old_handle),
            ));
            // Cleanup takes the old connection's bound routes and captures
            // the identity before its Core turn.
            let taken = registry.take_connection_bound_routes("client-a");
            assert_eq!(taken.len(), 1);
            let captured = registry.stream_identity("s", "sub").expect("old stream");
            assert_eq!(captured, old);

            // Meanwhile a replacement attaches and binds the same key.
            let replacement = registry.start_attach(
                AttachStreamOwner {
                    client_id: replacement_client.to_string(),
                    grant_id: None,
                },
                "s".into(),
                "sub".into(),
            );
            let (_new_adapter, new_handle) = UnixTerminalAdapter::pair();
            assert!(registry.mark_adapter_bound_if(
                "s",
                "sub",
                &replacement,
                TerminalSubscriptionGeneration(2),
                BoundAdapterHandle::Unix(new_handle.clone()),
            ));

            // The delayed cleanup outcome arrives with the captured identity.
            assert!(!registry.stream_matches("s", "sub", &captured));
            assert!(!registry.close_adapter_if("s", "sub", &captured));
            assert!(!registry.cancel_stream_if("s", "sub", &captured));
            assert!(
                registry.is_adapter_bound("s", "sub"),
                "replacement by {replacement_client} stays bound"
            );
            assert!(
                !new_handle.host_closed(),
                "cleanup never host-closed the replacement adapter"
            );
            assert_eq!(
                registry.recorded_generation("s", "sub"),
                Some(TerminalSubscriptionGeneration(2))
            );
            let bound = registry.take_connection_bound_routes(replacement_client);
            assert_eq!(
                bound.len(),
                1,
                "the replacement keeps its bound route claim"
            );
        }
    }

    /// H3: an old peer's close snapshot names a key a Unix client has since
    /// replaced. The validated departing set excludes it, keeps the peer's
    /// own bound route, and attributes a streamless snapshot key to the peer.
    #[test]
    fn departing_routes_exclude_a_unix_replacement() {
        let mut registry = AttachStreamRegistry::default();
        let peer = AttachStreamOwner {
            client_id: "botster-hub-daemon-subscription-k2".to_string(),
            grant_id: Some("grant-old".to_string()),
        };
        // K1: the peer's own live route.
        let own = registry.start_attach(peer.clone(), "s".into(), "k1".into());
        let (_, own_handle) = UnixTerminalAdapter::pair();
        assert!(registry.mark_adapter_bound_if(
            "s",
            "k1",
            &own,
            TerminalSubscriptionGeneration(1),
            BoundAdapterHandle::Unix(own_handle),
        ));
        // K2: the peer attached it once; a Unix client replaced it.
        registry.start_attach(peer.clone(), "s".into(), "k2".into());
        let unix = registry.start_attach(owner(), "s".into(), "k2".into());
        let (_unix_adapter, unix_handle) = UnixTerminalAdapter::pair();
        assert!(registry.mark_adapter_bound_if(
            "s",
            "k2",
            &unix,
            TerminalSubscriptionGeneration(2),
            BoundAdapterHandle::Unix(unix_handle.clone()),
        ));
        let removed: BTreeSet<String> = ["grant-old".to_string()].into_iter().collect();
        let snapshot: BTreeSet<(String, String)> = [
            ("s".to_string(), "k1".to_string()),
            ("s".to_string(), "k2".to_string()),
            ("s".to_string(), "k3".to_string()),
        ]
        .into_iter()
        .collect();
        let departing = registry.departing_routes_for_grants(&removed, "grant-old", &snapshot);
        let keys = departing.get("grant-old").expect("departing grant");
        assert!(keys.contains(&("s".to_string(), "k1".to_string())));
        assert!(keys.contains(&("s".to_string(), "k3".to_string())));
        assert!(
            !keys.contains(&("s".to_string(), "k2".to_string())),
            "a key replaced by a Unix client is not the departing peer's"
        );
        assert_eq!(departing.len(), 1);
        assert!(registry.is_adapter_bound("s", "k2"));
        assert!(!unix_handle.host_closed());
    }

    /// A duplicate close arrives with a streamless snapshot after the first
    /// close consumed the grant's permit: no cleaning grants, so no
    /// departing routes and no bookkeeping mutation.
    #[test]
    fn duplicate_streamless_close_has_no_departing_routes() {
        let mut registry = AttachStreamRegistry::default();
        registry
            .attach_owner_grant_ids
            .insert(("s".to_string(), "k".to_string()), "grant-old".to_string());
        let snapshot: BTreeSet<(String, String)> =
            [("s".to_string(), "k".to_string())].into_iter().collect();
        let none: BTreeSet<String> = BTreeSet::new();
        assert!(
            registry
                .departing_routes_for_grants(&none, "grant-old", &snapshot)
                .is_empty()
        );
        // A sibling-only close does not attribute streamless keys to a
        // primary that is not being cleaned.
        let sibling: BTreeSet<String> = ["grant-sibling".to_string()].into_iter().collect();
        registry.attach_owner_grant_ids.clear();
        assert!(
            registry
                .departing_routes_for_grants(&sibling, "grant-old", &snapshot)
                .is_empty()
        );
    }

    /// A sibling grant's reserved key with no stream and no index entry
    /// stays attributed to the sibling when only the sibling is cleaned.
    #[test]
    fn sibling_route_set_entry_keeps_its_owner() {
        let mut registry = AttachStreamRegistry::default();
        assert_eq!(
            registry.reserve_route("grant-sibling", "s", "k", 4),
            RouteReservation::Inserted
        );
        let cleaning: BTreeSet<String> = ["grant-sibling".to_string()].into_iter().collect();
        let empty: BTreeSet<(String, String)> = BTreeSet::new();
        let departing = registry.departing_routes_for_grants(&cleaning, "grant-old", &empty);
        assert_eq!(departing.len(), 1);
        assert!(
            departing
                .get("grant-sibling")
                .is_some_and(|keys| keys.contains(&("s".to_string(), "k".to_string()))),
            "the sibling's own route-set key keeps its attribution"
        );
    }

    #[test]
    fn route_set_is_a_union_bounded_at_reservation() {
        let mut registry = AttachStreamRegistry::default();
        for index in 0..3 {
            assert_eq!(
                registry.reserve_route("a", &format!("s{index}"), "sub", 3),
                RouteReservation::Inserted
            );
        }
        assert_eq!(
            registry.reserve_route("a", "s0", "sub", 3),
            RouteReservation::AlreadyHeld
        );
        assert_eq!(
            registry.reserve_route("a", "s3", "sub", 3),
            RouteReservation::Full
        );
        assert_eq!(registry.owner_route_count("a"), 3);
        registry.release_route("a", "s1", "sub");
        assert_eq!(
            registry.reserve_route("a", "s3", "sub", 3),
            RouteReservation::Inserted
        );
        assert_eq!(registry.take_owner_routes("a").len(), 3);
        assert_eq!(registry.owner_route_count("a"), 0);
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

    fn inventory_row(
        client_id: &str,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> TerminalSubscriptionRecord {
        TerminalSubscriptionRecord {
            client_id: ClientId(client_id.to_string()),
            session_id: SessionId(session_id.to_string()),
            subscription_id: SubscriptionId(subscription_id.to_string()),
            generation: TerminalSubscriptionGeneration(generation),
            adapter_bound: true,
            capabilities: None,
        }
    }

    fn bind_unix(
        registry: &mut AttachStreamRegistry,
        identity: &AttachmentIdentity,
        session_id: &str,
        subscription_id: &str,
        generation: u64,
    ) -> (
        UnixTerminalAdapter,
        crate::transport::unix::UnixTerminalAdapterHandle,
    ) {
        // The adapter half stays alive: dropping it closes the slot and
        // would make `is_closed` true without any host close.
        let (adapter, handle) = UnixTerminalAdapter::pair();
        assert!(registry.mark_adapter_bound_if(
            session_id,
            subscription_id,
            identity,
            TerminalSubscriptionGeneration(generation),
            BoundAdapterHandle::Unix(handle.clone()),
        ));
        (adapter, handle)
    }

    // Inventory-reconcile timing table (architect0060 rows 1-6). Each test
    // models one Core inventory read as the row vector that read returned:
    // the vector is fixed at the moment the read is submitted, and applied
    // later through the real `reconcile_inventory_slice`. Rows that attach
    // after the read was submitted cannot be in that vector; the current
    // implementation closes them as stale. The production correction must
    // carry the registry attach epoch captured at read submission and skip
    // streams whose epoch is newer.

    /// Row 1 (positive control): a stream bound before the read and absent
    /// from it was ended by Core; reconcile closes and cancels it.
    #[test]
    fn reconcile_closes_a_bound_stream_older_than_the_read_and_absent_from_it() {
        let mut registry = AttachStreamRegistry::default();
        let old = registry.start_attach(owner(), "s".into(), "gone".into());
        let (_adapter, handle) = bind_unix(&mut registry, &old, "s", "gone", 5);
        // Read submitted after the bind; Core ended the route before the read
        // ran, so the vector lacks it.
        let read: Vec<TerminalSubscriptionRecord> = Vec::new();
        registry.reconcile_inventory(&read);
        assert!(handle.host_closed(), "Core-ended route is host-closed");
        assert!(registry.stream_identity("s", "gone").is_none());
    }

    /// Row 2 (red on the current implementation): a stream attached and
    /// bound after the read was submitted is absent from that read's rows
    /// and must survive the late application.
    #[test]
    fn reconcile_must_not_close_a_stream_attached_after_the_read_was_taken() {
        let mut registry = AttachStreamRegistry::default();
        let before = registry.start_attach(owner(), "s".into(), "before".into());
        let (_a1, before_handle) = bind_unix(&mut registry, &before, "s", "before", 1);
        // Read submitted now: it can only ever contain "before".
        let read = vec![inventory_row("client-a", "s", "before", 1)];
        // The newer attach lands (Core turn, then owner bind) before apply.
        let later = registry.start_attach(owner(), "s".into(), "later".into());
        let (_a2, later_handle) = bind_unix(&mut registry, &later, "s", "later", 2);
        registry.reconcile_inventory(&read);
        assert!(!before_handle.host_closed(), "present row survives");
        assert!(
            !later_handle.host_closed(),
            "a route attached after the read must not be closed by that read"
        );
        assert!(registry.stream_matches("s", "later", &later));
        assert!(registry.is_adapter_bound("s", "later"));
        assert_eq!(
            registry.recorded_generation("s", "later"),
            Some(TerminalSubscriptionGeneration(2))
        );
    }

    /// Row 3: an attach submitted before the read but still unbound at apply
    /// is not visited; it is judged by a later read once bound.
    #[test]
    fn reconcile_does_not_visit_an_attach_still_unbound_at_apply() {
        let mut registry = AttachStreamRegistry::default();
        let pending = registry.start_attach(owner(), "s".into(), "pending".into());
        let read: Vec<TerminalSubscriptionRecord> = Vec::new();
        let progress = registry.reconcile_inventory_slice(|_, _| None, None, usize::MAX);
        let _ = read;
        assert_eq!(progress.validated, 0, "unbound rows are not validated");
        assert!(
            registry.stream_matches("s", "pending", &pending),
            "untouched"
        );
        assert!(!registry.is_adapter_bound("s", "pending"));
        assert_eq!(registry.recorded_generation("s", "pending"), None);
    }

    /// Row 4: a replacement owner (new client, new generation) on the same
    /// key. start_attach already cancelled and host-closed the old owner's
    /// stream; the read holds only the current owner's row, and the current
    /// stream survives under its own identity.
    #[test]
    fn reconcile_keeps_the_replacement_owner_when_inventory_holds_only_its_row() {
        let mut registry = AttachStreamRegistry::default();
        let old = registry.start_attach(owner(), "s".into(), "k".into());
        let (_a1, old_handle) = bind_unix(&mut registry, &old, "s", "k", 1);
        let replacement = registry.start_attach(
            AttachStreamOwner {
                client_id: "client-b".to_string(),
                grant_id: None,
            },
            "s".into(),
            "k".into(),
        );
        assert!(
            old_handle.host_closed(),
            "start_attach closed the old owner"
        );
        let (_a2, new_handle) = bind_unix(&mut registry, &replacement, "s", "k", 2);
        let read = vec![inventory_row("client-b", "s", "k", 2)];
        registry.reconcile_inventory(&read);
        assert!(!new_handle.host_closed());
        assert!(registry.stream_matches("s", "k", &replacement));
        assert_eq!(
            registry
                .stream_identity("s", "k")
                .map(|identity| identity.client_id),
            Some("client-b".to_string())
        );
    }

    /// Row 5 (red on the current implementation): with one route per slice,
    /// a stream attached between slices is absent from a second read that
    /// was submitted before its attach and must survive that slice.
    #[test]
    fn reconcile_slice_must_not_close_a_stream_attached_between_slices_when_the_read_predates_it() {
        let mut registry = AttachStreamRegistry::default();
        let first = registry.start_attach(owner(), "s".into(), "a-first".into());
        let (_a1, first_handle) = bind_unix(&mut registry, &first, "s", "a-first", 1);
        let read_one = vec![inventory_row("client-a", "s", "a-first", 1)];
        let progress = registry.reconcile_inventory_slice(
            |session_id, subscription_id| {
                read_one
                    .iter()
                    .find(|row| {
                        row.session_id.0 == session_id && row.subscription_id.0 == subscription_id
                    })
                    .map(|row| row.generation)
            },
            None,
            1,
        );
        assert_eq!(progress.validated, 1);
        assert!(!first_handle.host_closed());
        // Second read submitted before the next attach lands.
        let read_two = read_one.clone();
        let second = registry.start_attach(owner(), "s".into(), "b-second".into());
        let (_a2, second_handle) = bind_unix(&mut registry, &second, "s", "b-second", 2);
        let _ = registry.reconcile_inventory_slice(
            |session_id, subscription_id| {
                read_two
                    .iter()
                    .find(|row| {
                        row.session_id.0 == session_id && row.subscription_id.0 == subscription_id
                    })
                    .map(|row| row.generation)
            },
            progress.after,
            1,
        );
        assert!(
            !second_handle.host_closed(),
            "a route attached after the slice's read must survive that slice"
        );
        assert!(registry.stream_matches("s", "b-second", &second));
    }

    /// Row 5 control: the same paging, but the second read was submitted
    /// after the attach and includes it; it survives today.
    #[test]
    fn reconcile_slice_keeps_a_stream_attached_between_slices_when_the_next_read_includes_it() {
        let mut registry = AttachStreamRegistry::default();
        let first = registry.start_attach(owner(), "s".into(), "a-first".into());
        let (_a1, _first_handle) = bind_unix(&mut registry, &first, "s", "a-first", 1);
        let progress = registry.reconcile_inventory_slice(
            |_, _| Some(TerminalSubscriptionGeneration(1)),
            None,
            1,
        );
        let second = registry.start_attach(owner(), "s".into(), "b-second".into());
        let (_a2, second_handle) = bind_unix(&mut registry, &second, "s", "b-second", 2);
        let read_two = vec![
            inventory_row("client-a", "s", "a-first", 1),
            inventory_row("client-a", "s", "b-second", 2),
        ];
        let _ = registry.reconcile_inventory_slice(
            |session_id, subscription_id| {
                read_two
                    .iter()
                    .find(|row| {
                        row.session_id.0 == session_id && row.subscription_id.0 == subscription_id
                    })
                    .map(|row| row.generation)
            },
            progress.after,
            1,
        );
        assert!(!second_handle.host_closed());
    }

    /// Row 6: WebRTC split timing. attach_route lands at Core turn N (the
    /// generation is recorded, no adapter yet); the read is submitted after
    /// that turn, so its rows include the route; the reserved bind lands at
    /// turn N+k before apply. The stream is visited after the bind, the row
    /// is present, and it survives.
    #[test]
    fn reconcile_visits_a_split_attach_bind_stream_after_bind_with_its_row_present() {
        let mut registry = AttachStreamRegistry::default();
        let peer = AttachStreamOwner {
            client_id: "botster-hub-daemon-subscription-w".to_string(),
            grant_id: Some("grant-w".to_string()),
        };
        let identity = registry.start_attach(peer, "s".into(), "w".into());
        assert!(registry.record_generation_if(
            "s",
            "w",
            &identity,
            TerminalSubscriptionGeneration(7)
        ));
        let read = vec![inventory_row(
            "botster-hub-daemon-subscription-w",
            "s",
            "w",
            7,
        )];
        let (_adapter, handle) = bind_unix(&mut registry, &identity, "s", "w", 7);
        registry.reconcile_inventory(&read);
        assert!(!handle.host_closed());
        assert!(registry.stream_matches("s", "w", &identity));
    }

    /// Row 6 (red on the current implementation): the read is submitted
    /// before the attach_route turn; the bind lands before apply. The route
    /// cannot be in that read and must survive.
    #[test]
    fn reconcile_must_not_close_a_split_attach_bind_stream_whose_attach_followed_the_read() {
        let mut registry = AttachStreamRegistry::default();
        let read: Vec<TerminalSubscriptionRecord> = Vec::new();
        let peer = AttachStreamOwner {
            client_id: "botster-hub-daemon-subscription-w".to_string(),
            grant_id: Some("grant-w".to_string()),
        };
        let identity = registry.start_attach(peer, "s".into(), "w".into());
        assert!(registry.record_generation_if(
            "s",
            "w",
            &identity,
            TerminalSubscriptionGeneration(7)
        ));
        let (_adapter, handle) = bind_unix(&mut registry, &identity, "s", "w", 7);
        registry.reconcile_inventory(&read);
        assert!(
            !handle.host_closed(),
            "a split attach/bind that followed the read must survive it"
        );
        assert!(registry.stream_matches("s", "w", &identity));
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
