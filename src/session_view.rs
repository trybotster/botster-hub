//! The Hub's session rows, readable from plugin threads without the owner.
//!
//! The owner writes this view from the session projection's own mutators, one
//! row per change, so the view has no second source of truth: it is the
//! projection's rows, projected the way entity frames project them. A reader
//! copies one bounded page under the read lock and never waits on the owner
//! for more than that copy.
//!
//! The view is unavailable, never empty, when no complete baseline has been
//! applied (at start and during baseline recovery), and when a writer
//! panicked while holding the lock.

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock};

use botster_hub_client::DaemonSessionEntity;

/// Rows in one page.
pub(crate) const SESSION_VIEW_PAGE_ROWS: usize = 8;

/// Why a read cannot answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionViewError {
    /// No complete baseline is applied yet.
    NotReady,
    /// A writer panicked while holding the lock.
    Unavailable,
}

/// One page of rows and the cursor for the next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionViewPage {
    pub(crate) rows: Vec<DaemonSessionEntity>,
    /// The last session id in `rows` when more rows follow.
    pub(crate) next_after: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    sealed: bool,
    rows: BTreeMap<String, DaemonSessionEntity>,
}

/// The shared row map. Only the session projection writes it.
#[derive(Debug, Default)]
pub(crate) struct SessionView {
    inner: RwLock<Inner>,
}

impl SessionView {
    /// Insert or replace one row.
    pub(crate) fn upsert(&self, entity: DaemonSessionEntity) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        inner.rows.insert(entity.session_uuid.clone(), entity);
    }

    /// Drop one row.
    pub(crate) fn remove(&self, session_id: &str) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        inner.rows.remove(session_id);
    }

    /// Drop every row and mark the view not ready (baseline recovery).
    pub(crate) fn reset(&self) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        inner.rows = BTreeMap::new();
        inner.sealed = false;
    }

    /// Mark whether a complete baseline is applied.
    pub(crate) fn set_sealed(&self, sealed: bool) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        inner.sealed = sealed;
    }

    /// One row, or `None` for an unknown session.
    pub(crate) fn get(
        &self,
        session_id: &str,
    ) -> Result<Option<DaemonSessionEntity>, SessionViewError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| SessionViewError::Unavailable)?;
        if !inner.sealed {
            return Err(SessionViewError::NotReady);
        }
        Ok(inner.rows.get(session_id).cloned())
    }

    /// The page of rows after `after`, in session id order.
    pub(crate) fn page(&self, after: Option<&str>) -> Result<SessionViewPage, SessionViewError> {
        use std::ops::Bound;
        let inner = self
            .inner
            .read()
            .map_err(|_| SessionViewError::Unavailable)?;
        if !inner.sealed {
            return Err(SessionViewError::NotReady);
        }
        let start = match after {
            Some(after) => Bound::Excluded(after),
            None => Bound::Unbounded,
        };
        let mut rows = Vec::with_capacity(SESSION_VIEW_PAGE_ROWS);
        let mut more = false;
        for (_, row) in inner.rows.range::<str, _>((start, Bound::Unbounded)) {
            if rows.len() == SESSION_VIEW_PAGE_ROWS {
                more = true;
                break;
            }
            rows.push(row.clone());
        }
        let next_after = more
            .then(|| rows.last().map(|row| row.session_uuid.clone()))
            .flatten();
        Ok(SessionViewPage { rows, next_after })
    }

    /// A poisoned lock is a permanent fault for readers.
    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
            panic!("poison the session view");
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(id: &str) -> DaemonSessionEntity {
        DaemonSessionEntity {
            session_uuid: id.to_string(),
            registry_state: "running".to_string(),
            lifecycle: Some("running".to_string()),
            lifecycle_class: "current".to_string(),
            rows: 24,
            cols: 80,
            updated_at: 1,
            exit_code: None,
            failure_reason: None,
            session_type_id: None,
            session_type_source: None,
            role: None,
            traits: Vec::new(),
            interaction: None,
            session_type_lifecycle: None,
            restartable: false,
        }
    }

    #[test]
    fn an_unsealed_view_is_not_ready_never_empty() {
        let view = SessionView::default();
        assert_eq!(view.page(None), Err(SessionViewError::NotReady));
        assert_eq!(view.get("a"), Err(SessionViewError::NotReady));
        view.set_sealed(true);
        assert_eq!(view.page(None).unwrap().rows.len(), 0);
        assert_eq!(view.get("a"), Ok(None));
    }

    #[test]
    fn pages_follow_session_id_order_with_a_cursor() {
        let view = SessionView::default();
        view.set_sealed(true);
        for index in 0..(SESSION_VIEW_PAGE_ROWS + 3) {
            view.upsert(entity(&format!("s{index:02}")));
        }
        let first = view.page(None).unwrap();
        assert_eq!(first.rows.len(), SESSION_VIEW_PAGE_ROWS);
        let cursor = first.next_after.expect("more rows follow");
        assert_eq!(cursor, "s07");
        let second = view.page(Some(&cursor)).unwrap();
        assert_eq!(second.rows.len(), 3);
        assert_eq!(second.next_after, None);
        assert_eq!(second.rows[0].session_uuid, "s08");
    }

    #[test]
    fn reset_drops_rows_and_returns_the_view_to_not_ready() {
        let view = SessionView::default();
        view.set_sealed(true);
        view.upsert(entity("a"));
        view.reset();
        assert_eq!(view.get("a"), Err(SessionViewError::NotReady));
        view.set_sealed(true);
        assert_eq!(view.get("a"), Ok(None));
    }

    #[test]
    fn a_poisoned_lock_is_unavailable_for_readers() {
        let view = SessionView::default();
        view.set_sealed(true);
        view.poison_for_test();
        assert_eq!(view.page(None), Err(SessionViewError::Unavailable));
        assert_eq!(view.get("a"), Err(SessionViewError::Unavailable));
    }
}
