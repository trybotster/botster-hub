//! Hub ownership of explicit Core session reservations after Reserve.
//!
//! Core frees an explicit admission entry only when its holder releases the
//! token; `RemoveSession` does not. One record per reserved session id tracks
//! who owns that release. While a record exists, its unreleased token keeps
//! the id occupied in Core, so no second reservation (and no second record)
//! can exist for that id.
//!
//! - A spawn registers `Launching` after Reserve and before it submits
//!   SpawnReserved, so any later removal finds the record.
//! - A spawn that succeeds hands its token to the record (`Installed`).
//! - An authoritative removal of an installed session releases that token.
//!   A removal during launch only marks the record; the spawn releases at
//!   its handoff.
//! - The record is deleted only when its token's release is confirmed, or in
//!   the same step that moves the token to the retained-release list.
//!
//! Each record charges its storage to the Hub state budget before Reserve,
//! for as long as the record exists.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use botster_core::{SessionReservation, SessionReservationIdentity};

use crate::shared_view::{SharedViewBudget, SharedViewCapacityError, SharedViewCharge};

/// Storage charge for one record, taken before Core reserves the id.
pub(crate) struct RecordCharge {
    session_id: String,
    charge: SharedViewCharge,
}

enum RecordState {
    Launching { removed: bool },
    Installed(SessionReservation),
    Releasing,
}

struct Record {
    identity: SessionReservationIdentity,
    state: RecordState,
    _charge: SharedViewCharge,
}

/// What the spawn that owned the token must do at its success handoff.
pub(crate) enum Handoff {
    /// The record now owns the token.
    Kept,
    /// The session was removed during launch: release this token now, then
    /// report the result with `retire`.
    ReleaseNow(SessionReservation),
    /// No matching record: the spawn keeps its old disposition.
    Unregistered(SessionReservation),
}

/// What an authoritative removal must do.
pub(crate) enum Removal {
    /// Release this token, then report the result with `retire`.
    ReleaseNow(SessionReservation),
    /// The owning spawn releases at its handoff.
    Deferred,
    /// No record for the captured identity.
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RegisterError {
    /// A record for this id already exists. Core admission makes this
    /// unreachable; reaching it means a lost release obligation.
    Duplicate,
}

#[derive(Default)]
pub(crate) struct SessionReservationRecords {
    records: Mutex<BTreeMap<String, Record>>,
}

/// A poisoned lock keeps its records: every mutation leaves the map
/// consistent, so recovery never reports a record as absent.
fn lock(
    records: &Mutex<BTreeMap<String, Record>>,
) -> std::sync::MutexGuard<'_, BTreeMap<String, Record>> {
    records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn record_bytes(session_id: &str) -> Option<usize> {
    crate::lua_memory::layout::btree_nodes_checked::<String, Record>(1)?
        .checked_add(session_id.len())
}

impl SessionReservationRecords {
    /// Charge one record's storage before Reserve.
    pub(crate) fn charge(
        budget: &Arc<SharedViewBudget>,
        session_id: &str,
    ) -> Result<RecordCharge, SharedViewCapacityError> {
        let bytes = record_bytes(session_id).ok_or(SharedViewCapacityError {
            requested: usize::MAX,
            available: 0,
        })?;
        // Take the charge before allocating the record's own storage.
        let charge = budget.reserve(bytes)?;
        Ok(RecordCharge {
            session_id: session_id.to_string(),
            charge,
        })
    }

    /// Register a reserved id before SpawnReserved is submitted.
    pub(crate) fn register(
        &self,
        charge: RecordCharge,
        identity: SessionReservationIdentity,
    ) -> Result<(), RegisterError> {
        let mut records = lock(&self.records);
        if records.contains_key(&charge.session_id) {
            return Err(RegisterError::Duplicate);
        }
        records.insert(
            charge.session_id,
            Record {
                identity,
                state: RecordState::Launching { removed: false },
                _charge: charge.charge,
            },
        );
        Ok(())
    }

    /// The identity a removal must match when its receipt arrives.
    pub(crate) fn capture(&self, session_id: &str) -> Option<SessionReservationIdentity> {
        lock(&self.records)
            .get(session_id)
            .map(|record| record.identity)
    }

    /// A spawn succeeded and hands over its token.
    pub(crate) fn install(&self, session_id: &str, token: SessionReservation) -> Handoff {
        let mut records = lock(&self.records);
        let Some(record) = records
            .get_mut(session_id)
            .filter(|record| record.identity == token.identity())
        else {
            return Handoff::Unregistered(token);
        };
        match record.state {
            RecordState::Launching { removed: false } => {
                record.state = RecordState::Installed(token);
                Handoff::Kept
            }
            RecordState::Launching { removed: true } => {
                record.state = RecordState::Releasing;
                Handoff::ReleaseNow(token)
            }
            RecordState::Installed(_) | RecordState::Releasing => Handoff::Unregistered(token),
        }
    }

    /// Core confirmed removal of the session whose record had `captured`.
    pub(crate) fn removed(
        &self,
        session_id: &str,
        captured: SessionReservationIdentity,
    ) -> Removal {
        let mut records = lock(&self.records);
        let Some(record) = records
            .get_mut(session_id)
            .filter(|record| record.identity == captured)
        else {
            return Removal::None;
        };
        match std::mem::replace(&mut record.state, RecordState::Releasing) {
            RecordState::Installed(token) => Removal::ReleaseNow(token),
            RecordState::Launching { .. } => {
                record.state = RecordState::Launching { removed: true };
                Removal::Deferred
            }
            RecordState::Releasing => Removal::None,
        }
    }

    /// Whether a client already removed the session this exact token owns.
    pub(crate) fn removed_during_launch(
        &self,
        session_id: &str,
        identity: SessionReservationIdentity,
    ) -> bool {
        lock(&self.records).get(session_id).is_some_and(|record| {
            record.identity == identity
                && matches!(record.state, RecordState::Launching { removed: true })
        })
    }

    /// Delete the record: its token's release was confirmed, or the token
    /// moved to the retained-release list in the same step.
    pub(crate) fn retire(&self, session_id: &str, identity: SessionReservationIdentity) {
        let mut records = lock(&self.records);
        if records
            .get(session_id)
            .is_some_and(|record| record.identity == identity)
        {
            let removed = records.remove(session_id);
            if records.is_empty() {
                drop(std::mem::take(&mut *records));
            }
            drop(removed);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        lock(&self.records).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: &str) -> SessionReservation {
        botster_core::SessionAdmission::default()
            .reserve(botster_core::SessionId(id.to_string()))
            .expect("reserve test token")
    }

    #[test]
    fn install_then_removal_releases_the_exact_token() {
        let budget = SharedViewBudget::new();
        let records = SessionReservationRecords::default();
        let token = token("s1");
        records
            .register(
                SessionReservationRecords::charge(&budget, "s1").unwrap(),
                token.identity(),
            )
            .unwrap();
        assert!(budget.used() > 0);
        assert!(matches!(
            records.install("s1", token.clone()),
            Handoff::Kept
        ));
        let captured = records.capture("s1").unwrap();
        let Removal::ReleaseNow(released) = records.removed("s1", captured) else {
            panic!("an installed record releases on removal");
        };
        assert!(released.identity() == token.identity());
        records.retire("s1", captured);
        assert_eq!(records.len(), 0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn removal_during_launch_defers_release_to_the_handoff() {
        let budget = SharedViewBudget::new();
        let records = SessionReservationRecords::default();
        let token = token("s2");
        records
            .register(
                SessionReservationRecords::charge(&budget, "s2").unwrap(),
                token.identity(),
            )
            .unwrap();
        let captured = records.capture("s2").unwrap();
        assert!(matches!(records.removed("s2", captured), Removal::Deferred));
        assert!(records.removed_during_launch("s2", token.identity()));
        assert!(matches!(
            records.install("s2", token.clone()),
            Handoff::ReleaseNow(_)
        ));
        // The record stays until the release result arrives.
        assert_eq!(records.len(), 1);
        records.retire("s2", token.identity());
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn a_stale_capture_never_acts_on_a_replacement_record() {
        let budget = SharedViewBudget::new();
        let records = SessionReservationRecords::default();
        let old = token("s3");
        records
            .register(
                SessionReservationRecords::charge(&budget, "s3").unwrap(),
                old.identity(),
            )
            .unwrap();
        let stale = records.capture("s3").unwrap();
        records.retire("s3", old.identity());
        let replacement = token("s3");
        records
            .register(
                SessionReservationRecords::charge(&budget, "s3").unwrap(),
                replacement.identity(),
            )
            .unwrap();
        assert!(matches!(records.install("s3", replacement), Handoff::Kept));
        assert!(matches!(records.removed("s3", stale), Removal::None));
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn a_duplicate_registration_is_reported_and_keeps_the_first_record() {
        let budget = SharedViewBudget::new();
        let records = SessionReservationRecords::default();
        let first = token("s4");
        records
            .register(
                SessionReservationRecords::charge(&budget, "s4").unwrap(),
                first.identity(),
            )
            .unwrap();
        assert_eq!(
            records.register(
                SessionReservationRecords::charge(&budget, "s4").unwrap(),
                token("s4").identity(),
            ),
            Err(RegisterError::Duplicate)
        );
        assert!(records.capture("s4") == Some(first.identity()));
    }

    #[test]
    fn a_refused_charge_takes_nothing() {
        let budget = SharedViewBudget::with_capacity(1);
        assert!(SessionReservationRecords::charge(&budget, "s5").is_err());
        assert_eq!(budget.used(), 0);
    }
}
