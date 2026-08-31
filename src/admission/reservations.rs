//! Hub-owned WebRTC terminal subscription label reservations.
//!
//! Labels are opaque. Hub never derives peer-visible meaning from their
//! contents. Core-minted generations stay recorded values.

use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use botster_hub_client::DaemonTerminalReservation;

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
    pub session_id: String,
    pub subscription_id: String,
    pub generation: u64,
    pub peer_generation: u64,
    pub label: String,
    pub expires_at_seconds: u64,
    #[allow(dead_code)]
    pub expires_in_seconds: u32,
    pub state: ReservationState,
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
    by_key: BTreeMap<(String, String, u64, u64), TerminalReservation>,
    by_label: BTreeMap<String, (String, String, u64, u64)>,
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
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
            generation,
            peer_generation,
            label: label.clone(),
            expires_at_seconds: now_seconds.saturating_add(u64::from(expires_in_seconds)),
            expires_in_seconds,
            state: ReservationState::Live,
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

    pub(crate) fn lookup_label(&self, label: &str, now_seconds: u64) -> ReservationLookup {
        let Some(key) = self.by_label.get(label) else {
            return ReservationLookup::Unknown;
        };
        let Some(reservation) = self.by_key.get(key) else {
            return ReservationLookup::Unknown;
        };
        match reservation.state {
            ReservationState::Bound => ReservationLookup::Bound,
            ReservationState::Expired => ReservationLookup::Expired,
            ReservationState::Live if reservation.expires_at_seconds <= now_seconds => {
                ReservationLookup::Expired
            }
            ReservationState::Live => ReservationLookup::Live,
        }
    }

    pub(crate) fn reservation_for_label(&self, label: &str) -> Option<&TerminalReservation> {
        let key = self.by_label.get(label)?;
        self.by_key.get(key)
    }

    pub(crate) fn expire_label(
        &mut self,
        label: &str,
        now_seconds: u64,
    ) -> Option<TerminalReservation> {
        let key = self.by_label.get(label)?.clone();
        let reservation = self.by_key.get_mut(&key)?;
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

    pub(crate) fn mark_bound(&mut self, label: &str) -> Option<TerminalReservation> {
        let key = self.by_label.get(label)?.clone();
        let reservation = self.by_key.get_mut(&key)?;
        if reservation.state != ReservationState::Live {
            return None;
        }
        reservation.state = ReservationState::Bound;
        Some(reservation.clone())
    }

    pub(crate) fn retire_expired(&mut self, now_seconds: u64) {
        for reservation in self.by_key.values_mut() {
            if reservation.state == ReservationState::Live
                && reservation.expires_at_seconds <= now_seconds
            {
                reservation.state = ReservationState::Expired;
            }
        }
    }

    pub(crate) fn forget_route(&mut self, session_id: &str, subscription_id: &str) {
        let keys: Vec<_> = self
            .by_key
            .keys()
            .filter(|(route_session, route_subscription, _, _)| {
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

    #[allow(dead_code)]
    pub(crate) fn forget_peer(&mut self, peer_generation: u64) {
        let keys: Vec<_> = self
            .by_key
            .keys()
            .filter(|(_, _, _, generation)| *generation == peer_generation)
            .cloned()
            .collect();
        for key in keys {
            if let Some(reservation) = self.by_key.remove(&key) {
                self.by_label.remove(&reservation.label);
            }
        }
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

fn unique_label(existing: &BTreeMap<String, (String, String, u64, u64)>) -> String {
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
            registry.lookup_label(&first.label, 100),
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
            registry.lookup_label(&reserved.label, 10 + u64::from(reserved.expires_in_seconds)),
            ReservationLookup::Expired
        );
        assert_eq!(
            registry.lookup_label("never-reserved", 10),
            ReservationLookup::Unknown
        );
    }
}
