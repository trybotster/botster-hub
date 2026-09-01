//! Hub-owned WebRTC terminal subscription label reservations.
//!
//! Labels are opaque. Hub never derives peer-visible meaning from their
//! contents. Core-minted generations stay recorded values.

use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use botster_hub_client::{
    DaemonSubscriptionReservation, DaemonSubscriptionReservationKind, DaemonTerminalReservation,
};
use tokio::sync::mpsc;

use crate::admission::connection_budget::ChannelClass;
use crate::subscription::package_events::ClientEventMailbox;

/// Whole seconds a peer has to open a reserved subscription channel.
pub(crate) const TERMINAL_RESERVATION_EXPIRES_IN_SECONDS: u32 = 30;

const TEST_RESERVATION_EXPIRES_IN_SECONDS_ENV: &str =
    "BOTSTER_HUB_TEST_RESERVATION_EXPIRES_IN_SECONDS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReservationState {
    Live,
    Expired,
    Bound,
}

#[derive(Debug, Clone)]
pub(crate) struct TerminalReservation {
    pub class: ChannelClass,
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
    pub peer_generation: u64,
    pub label: String,
    pub expires_at_seconds: u64,
    #[allow(dead_code)]
    pub expires_in_seconds: u32,
    pub state: ReservationState,
    pub binding: ReservationBinding,
}

#[derive(Debug, Clone)]
pub(crate) enum ReservationBinding {
    Terminal,
    Entity {
        receiver: std::sync::Arc<
            std::sync::Mutex<Option<mpsc::Receiver<botster_hub_client::DaemonEntityFrame>>>,
        >,
    },
    Event {
        mailbox: std::sync::Arc<ClientEventMailbox>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReserveError {
    LabelConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReservationLookup {
    Unknown,
    Expired,
    Bound,
    Live,
}

#[derive(Debug, Default)]
pub(crate) struct TerminalReservationRegistry {
    by_key: BTreeMap<(ChannelClass, String, String, u64, u64), TerminalReservation>,
    by_label: BTreeMap<String, (ChannelClass, String, String, u64, u64)>,
}

impl TerminalReservationRegistry {
    pub(crate) fn has_live_for_route(
        &self,
        session_id: &str,
        subscription_id: &str,
        peer_generation: u64,
        now_seconds: u64,
    ) -> bool {
        self.by_key.values().any(|reservation| {
            reservation.session_id == session_id
                && reservation.subscription_id == subscription_id
                && reservation.peer_generation == peer_generation
                && reservation.state == ReservationState::Live
                && reservation.expires_at_seconds > now_seconds
        })
    }

    pub(crate) fn reserve(
        &mut self,
        session_id: String,
        subscription_id: String,
        generation: u64,
        peer_generation: u64,
        now_seconds: u64,
    ) -> Result<DaemonTerminalReservation, ReserveError> {
        let key = (
            ChannelClass::Terminal,
            session_id.clone(),
            subscription_id.clone(),
            generation,
            peer_generation,
        );
        if let Some(existing) = self.by_key.get(&key)
            && existing.state == ReservationState::Live
            && existing.expires_at_seconds > now_seconds
        {
            return Err(ReserveError::LabelConflict);
        }
        if self.has_live_for_route(&session_id, &subscription_id, peer_generation, now_seconds) {
            return Err(ReserveError::LabelConflict);
        }
        let expires_in_seconds = reservation_expires_in_seconds();
        let label = unique_label(&self.by_label);
        let reservation = TerminalReservation {
            class: ChannelClass::Terminal,
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            generation,
            peer_generation,
            label: label.clone(),
            expires_at_seconds: now_seconds.saturating_add(u64::from(expires_in_seconds)),
            expires_in_seconds,
            state: ReservationState::Live,
            binding: ReservationBinding::Terminal,
        };
        self.by_label.insert(label.clone(), key.clone());
        self.by_key.insert(key, reservation);
        Ok(DaemonTerminalReservation::new(
            session_id,
            subscription_id,
            generation,
            peer_generation,
            label,
            expires_in_seconds,
        ))
    }

    pub(crate) fn reserve_subscription(
        &mut self,
        class: ChannelClass,
        subscription_id: String,
        generation: u64,
        peer_generation: u64,
        now_seconds: u64,
        binding: ReservationBinding,
    ) -> Result<DaemonSubscriptionReservation, ReserveError> {
        debug_assert!(matches!(class, ChannelClass::Entity | ChannelClass::Event));
        let key = (
            class,
            String::new(),
            subscription_id.clone(),
            generation,
            peer_generation,
        );
        if self.by_key.values().any(|reservation| {
            reservation.class == class
                && reservation.subscription_id == subscription_id
                && reservation.peer_generation == peer_generation
                && reservation.state == ReservationState::Live
                && reservation.expires_at_seconds > now_seconds
        }) {
            return Err(ReserveError::LabelConflict);
        }
        let expires_in_seconds = reservation_expires_in_seconds();
        let label = unique_label(&self.by_label);
        self.by_label.insert(label.clone(), key.clone());
        self.by_key.insert(
            key,
            TerminalReservation {
                class,
                session_id: String::new(),
                subscription_id: subscription_id.clone(),
                generation,
                peer_generation,
                label: label.clone(),
                expires_at_seconds: now_seconds.saturating_add(u64::from(expires_in_seconds)),
                expires_in_seconds,
                state: ReservationState::Live,
                binding,
            },
        );
        let kind = match class {
            ChannelClass::Entity => DaemonSubscriptionReservationKind::Entity,
            ChannelClass::Event => DaemonSubscriptionReservationKind::PackageEvent,
            ChannelClass::Control | ChannelClass::Terminal => unreachable!(),
        };
        Ok(DaemonSubscriptionReservation::new(
            kind,
            subscription_id,
            generation,
            peer_generation,
            label,
            expires_in_seconds,
        ))
    }

    pub(crate) fn lookup_label(
        &self,
        label: &str,
        peer_generation: u64,
        now_seconds: u64,
    ) -> ReservationLookup {
        let Some(key) = self.by_label.get(label) else {
            return ReservationLookup::Unknown;
        };
        let Some(reservation) = self.by_key.get(key) else {
            return ReservationLookup::Unknown;
        };
        if reservation.peer_generation != peer_generation {
            return ReservationLookup::Unknown;
        }
        match reservation.state {
            ReservationState::Bound => ReservationLookup::Bound,
            ReservationState::Expired => ReservationLookup::Expired,
            ReservationState::Live if reservation.expires_at_seconds <= now_seconds => {
                ReservationLookup::Expired
            }
            ReservationState::Live => ReservationLookup::Live,
        }
    }

    pub(crate) fn reservation_for_label(
        &self,
        label: &str,
        peer_generation: u64,
    ) -> Option<&TerminalReservation> {
        let key = self.by_label.get(label)?;
        self.by_key
            .get(key)
            .filter(|reservation| reservation.peer_generation == peer_generation)
    }

    pub(crate) fn label_peer_generation(&self, label: &str) -> Option<u64> {
        let key = self.by_label.get(label)?;
        self.by_key
            .get(key)
            .map(|reservation| reservation.peer_generation)
    }

    pub(crate) fn expire_label(
        &mut self,
        label: &str,
        peer_generation: u64,
        now_seconds: u64,
    ) -> Option<TerminalReservation> {
        let key = self.by_label.get(label)?.clone();
        let reservation = self.by_key.get_mut(&key)?;
        if reservation.peer_generation != peer_generation {
            return None;
        }
        if reservation.state == ReservationState::Bound {
            return None;
        }
        if reservation.state == ReservationState::Live
            && reservation.expires_at_seconds > now_seconds
        {
            return None;
        }
        reservation.state = ReservationState::Expired;
        Some(reservation.clone())
    }

    pub(crate) fn mark_bound(
        &mut self,
        label: &str,
        peer_generation: u64,
    ) -> Option<TerminalReservation> {
        let key = self.by_label.get(label)?.clone();
        let reservation = self.by_key.get_mut(&key)?;
        if reservation.peer_generation != peer_generation
            || reservation.state != ReservationState::Live
        {
            return None;
        }
        reservation.state = ReservationState::Bound;
        Some(reservation.clone())
    }

    pub(crate) fn forget_label(&mut self, label: &str, peer_generation: u64) -> bool {
        let Some(key) = self.by_label.get(label).cloned() else {
            return false;
        };
        if self
            .by_key
            .get(&key)
            .is_none_or(|reservation| reservation.peer_generation != peer_generation)
        {
            return false;
        }
        self.by_label.remove(label);
        self.by_key.remove(&key).is_some()
    }

    pub(crate) fn forget_subscription(
        &mut self,
        class: ChannelClass,
        subscription_id: &str,
        peer_generation: u64,
    ) -> Vec<String> {
        let labels: Vec<_> = self
            .by_key
            .values()
            .filter(|reservation| {
                reservation.class == class
                    && reservation.subscription_id == subscription_id
                    && reservation.peer_generation == peer_generation
            })
            .map(|reservation| reservation.label.clone())
            .collect();
        for label in &labels {
            let _ = self.forget_label(label, peer_generation);
        }
        labels
    }

    pub(crate) fn forget_unbound_subscription(
        &mut self,
        class: ChannelClass,
        subscription_id: &str,
        peer_generation: u64,
    ) -> Vec<String> {
        let labels: Vec<_> = self
            .by_key
            .values()
            .filter(|reservation| {
                reservation.class == class
                    && reservation.subscription_id == subscription_id
                    && reservation.peer_generation == peer_generation
                    && reservation.state != ReservationState::Bound
            })
            .map(|reservation| reservation.label.clone())
            .collect();
        for label in &labels {
            let _ = self.forget_label(label, peer_generation);
        }
        labels
    }

    pub(crate) fn retire_expired(&mut self, now_seconds: u64) -> Vec<TerminalReservation> {
        let mut expired = Vec::new();
        for reservation in self.by_key.values_mut() {
            if reservation.state == ReservationState::Live
                && reservation.expires_at_seconds <= now_seconds
            {
                reservation.state = ReservationState::Expired;
                expired.push(reservation.clone());
            }
        }
        expired
    }

    pub(crate) fn forget_route(&mut self, session_id: &str, subscription_id: &str) {
        let keys: Vec<_> = self
            .by_key
            .keys()
            .filter(|(_, route_session, route_subscription, _, _)| {
                route_session == session_id && route_subscription == subscription_id
            })
            .cloned()
            .collect();
        for key in keys {
            if let Some(reservation) = self.by_key.remove(&key) {
                self.by_label.remove(&reservation.label);
            }
        }
    }

    pub(crate) fn forget_peer(&mut self, peer_generation: u64) -> Vec<String> {
        let keys: Vec<_> = self
            .by_key
            .keys()
            .filter(|(_, _, _, _, generation)| *generation == peer_generation)
            .cloned()
            .collect();
        let mut labels = Vec::new();
        for key in keys {
            if let Some(reservation) = self.by_key.remove(&key) {
                self.by_label.remove(&reservation.label);
                labels.push(reservation.label);
            }
        }
        labels
    }
}

pub(crate) fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn reservation_expires_in_seconds() -> u32 {
    if env::var("BOTSTER_ENV").as_deref() == Ok("test")
        && let Ok(value) = env::var(TEST_RESERVATION_EXPIRES_IN_SECONDS_ENV)
        && let Ok(seconds) = value.parse::<u32>()
    {
        return seconds;
    }
    TERMINAL_RESERVATION_EXPIRES_IN_SECONDS
}

fn unique_label(existing: &BTreeMap<String, (ChannelClass, String, String, u64, u64)>) -> String {
    loop {
        let mut bytes = [0_u8; 16];
        if getrandom::fill(&mut bytes).is_err() {
            continue;
        }
        let label = format!(
            "r-{}",
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        if !existing.contains_key(&label) {
            return label;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_reserve_for_the_same_route_conflicts() {
        let mut registry = TerminalReservationRegistry::default();
        let first = registry
            .reserve("session".into(), "sub".into(), 1, 9, 100)
            .expect("first reserve");
        assert_eq!(first.generation, 1);
        assert_eq!(first.peer_generation, 9);
        assert_eq!(
            registry.reserve("session".into(), "sub".into(), 1, 9, 100),
            Err(ReserveError::LabelConflict)
        );
        assert_eq!(
            registry.lookup_label(&first.label, 9, 100),
            ReservationLookup::Live
        );
    }

    #[test]
    fn expired_label_stays_distinguishable_from_unknown() {
        let mut registry = TerminalReservationRegistry::default();
        let reserved = registry
            .reserve("session".into(), "sub".into(), 2, 3, 10)
            .expect("reserve");
        registry.retire_expired(10 + u64::from(reserved.expires_in_seconds));
        assert_eq!(
            registry.lookup_label(
                &reserved.label,
                3,
                10 + u64::from(reserved.expires_in_seconds)
            ),
            ReservationLookup::Expired
        );
        assert_eq!(
            registry.lookup_label("never-reserved", 3, 10),
            ReservationLookup::Unknown
        );
    }

    #[test]
    fn wrong_peer_cannot_inspect_expire_or_bind_a_reservation() {
        let mut registry = TerminalReservationRegistry::default();
        let reserved = registry
            .reserve("session".into(), "sub".into(), 2, 3, 10)
            .expect("reserve");

        assert_eq!(
            registry.lookup_label(&reserved.label, 4, u64::MAX),
            ReservationLookup::Unknown
        );
        assert!(registry.reservation_for_label(&reserved.label, 4).is_none());
        assert!(
            registry
                .expire_label(&reserved.label, 4, u64::MAX)
                .is_none()
        );
        assert!(registry.mark_bound(&reserved.label, 4).is_none());
        assert_eq!(
            registry.lookup_label(&reserved.label, 3, 10),
            ReservationLookup::Live
        );
    }

    #[test]
    fn reused_grant_generation_cannot_bind_an_old_reservation() {
        let mut registry = TerminalReservationRegistry::default();
        let reserved = registry
            .reserve("session".into(), "sub".into(), 2, 3, 10)
            .expect("reserve");

        assert_eq!(
            registry.lookup_label(&reserved.label, 5, 10),
            ReservationLookup::Unknown
        );
        assert!(registry.mark_bound(&reserved.label, 5).is_none());
        assert_eq!(
            registry.lookup_label(&reserved.label, 3, 10),
            ReservationLookup::Live
        );
    }

    #[test]
    fn entity_and_event_reservations_are_class_scoped_and_peer_scoped() {
        let mut registry = TerminalReservationRegistry::default();
        let (_entity_tx, entity_rx) = tokio::sync::mpsc::channel(1);
        let entity = registry
            .reserve_subscription(
                ChannelClass::Entity,
                "same".into(),
                1,
                7,
                10,
                ReservationBinding::Entity {
                    receiver: std::sync::Arc::new(std::sync::Mutex::new(Some(entity_rx))),
                },
            )
            .expect("entity reserve");
        let event = registry
            .reserve_subscription(
                ChannelClass::Event,
                "same".into(),
                1,
                7,
                10,
                ReservationBinding::Event {
                    mailbox: std::sync::Arc::new(ClientEventMailbox::new(
                        crate::config::PackageEventPlanePolicy::default(),
                    )),
                },
            )
            .expect("event reserve");
        assert_ne!(entity.label, event.label);
        assert_eq!(
            registry.lookup_label(&entity.label, 8, 10),
            ReservationLookup::Unknown
        );
        assert_eq!(
            registry.lookup_label(&event.label, 7, 10),
            ReservationLookup::Live
        );
    }

    #[test]
    fn peer_forget_retires_every_channel_class_once() {
        let mut registry = TerminalReservationRegistry::default();
        let terminal = registry
            .reserve("session".into(), "terminal".into(), 1, 9, 10)
            .expect("terminal reserve");
        let (_entity_tx, entity_rx) = tokio::sync::mpsc::channel(1);
        let entity = registry
            .reserve_subscription(
                ChannelClass::Entity,
                "entity".into(),
                2,
                9,
                10,
                ReservationBinding::Entity {
                    receiver: std::sync::Arc::new(std::sync::Mutex::new(Some(entity_rx))),
                },
            )
            .expect("entity reserve");
        let labels = registry.forget_peer(9);
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&terminal.label));
        assert!(labels.contains(&entity.label));
        assert!(registry.forget_peer(9).is_empty());
    }
}
