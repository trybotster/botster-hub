//! Owner-only quarantine of repository session-type roots.
//!
//! A repository session-types write whose outcome is uncertain after its
//! rename, or unknown because its Host job ended without a mutation result,
//! blocks further writes to that repository root until an operator resolves
//! it or the Hub restarts. Status lists every entry. Reads, sessions, and transport continue. The
//! entry's bytes are reserved from the Hub-state view budget before the file
//! effect, so recording it never needs new capacity.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use botster_hub_client::DaemonQuarantine;

use crate::host_mutations::{RepoWriteEvidence, SessionTypeOperation};
use crate::session_types::RepoPublicationCause;
use crate::shared_view::{SharedViewBudget, SharedViewCapacityError, SharedViewCharge};

/// The client-visible refusal for a write to a quarantined repository root.
pub(crate) const REPO_SESSION_TYPE_QUARANTINED: &str = "repo_session_type_quarantined";

/// Why a repository root is quarantined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineCause {
    /// The rename completed, but its durable result is unconfirmed.
    PublishedUncertain(RepoPublicationCause),
    /// The Host job ended without a mutation result after the commit started.
    UnknownOutcome(UnknownOutcomeKind),
}

/// The known reason a Host job returned no mutation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnknownOutcomeKind {
    WorkerPanicked,
    ResultTooLarge,
    Other,
}

impl UnknownOutcomeKind {
    /// Classify a non-mutation Host failure by its typed code.
    pub(crate) fn from_host_code(code: &str) -> Self {
        match code {
            "host_worker_panicked" => Self::WorkerPanicked,
            "host_result_too_large" => Self::ResultTooLarge,
            _ => Self::Other,
        }
    }
}

/// One quarantined repository root and the evidence that justifies it. The
/// Status listing reads these fields; the Hub log records them at
/// installation and at resolution.
pub(crate) struct RepoSessionTypeQuarantine {
    id: u64,
    evidence: RepoWriteEvidence,
    cause: QuarantineCause,
    quarantined_at: SystemTime,
    _charge: SharedViewCharge,
}

/// Why an operator resolve of a repository root was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolveRefusal {
    /// The root has no quarantine entry.
    NotFound,
    /// A recovery write for the root is still in flight.
    WriteInFlight,
}

/// The evidence and reserved charge a repository commit carries until its
/// outcome is known. Dropping it releases the reservation.
pub(crate) struct PendingRepoQuarantine {
    evidence: RepoWriteEvidence,
    charge: SharedViewCharge,
}

impl PendingRepoQuarantine {
    /// Reserve an entry's bytes before the file effect.
    /// Reserve an entry's bytes before the file effect, then copy the
    /// borrowed evidence under that charge. A refusal allocates nothing.
    pub(crate) fn reserve(
        evidence: &RepoWriteEvidence,
        budget: &std::sync::Arc<SharedViewBudget>,
    ) -> Result<Self, SharedViewCapacityError> {
        let charge = budget.reserve(entry_bytes(evidence))?;
        Ok(Self {
            evidence: evidence.clone(),
            charge,
        })
    }
}

/// Where a write to a quarantined root was refused. Test evidence only.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineStage {
    Intake,
    CommitAdmission,
}

/// Owner-only map of quarantined roots, keyed by canonical root.
#[derive(Default)]
pub(crate) struct RepoSessionTypeQuarantines {
    next_id: u64,
    entries: BTreeMap<PathBuf, RepoSessionTypeQuarantine>,
    #[cfg(test)]
    refusals: Vec<(PathBuf, QuarantineStage)>,
}

impl RepoSessionTypeQuarantines {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn contains(&self, root: &Path) -> bool {
        self.entries.contains_key(root)
    }

    /// Install the entry funded by `pending`. The map is touched only on the
    /// owner, and the bytes were reserved before the effect.
    /// `detail` is the full cause text; it goes to the local Hub log only.
    pub(crate) fn install(
        &mut self,
        pending: PendingRepoQuarantine,
        cause: QuarantineCause,
        detail: &str,
    ) {
        let PendingRepoQuarantine { evidence, charge } = pending;
        self.next_id = self.next_id.saturating_add(1);
        crate::hub_log::hub_log!(
            "repo_session_type_quarantined id={} root={} target={} session_type_id={} operation={:?} prior_sha256={} candidate_sha256={} cause={cause:?} detail={detail}",
            self.next_id,
            evidence.root.display(),
            evidence.target_path.display(),
            evidence.session_type_id,
            evidence.operation,
            evidence
                .prior_sha256
                .map_or_else(|| "missing".to_string(), |digest| hex(&digest)),
            hex(&evidence.candidate_sha256),
        );
        let root = evidence.root.clone();
        let entry = RepoSessionTypeQuarantine {
            id: self.next_id,
            evidence,
            cause,
            quarantined_at: SystemTime::now(),
            _charge: charge,
        };
        // One unresolved entry per root: every later write to it is refused.
        let previous = self.entries.insert(root, entry);
        debug_assert!(previous.is_none(), "a quarantined root admits no write");
    }

    /// Remove a root's entry at an operator's request. Dropping the entry
    /// releases its charge. The next session-type listing re-reads the
    /// repository file, which is the source of truth; nothing is rewritten.
    /// `root` must equal the canonical root that Status lists.
    pub(crate) fn resolve(
        &mut self,
        root: &Path,
        write_in_flight: bool,
    ) -> Result<(), ResolveRefusal> {
        if !self.entries.contains_key(root) {
            return Err(ResolveRefusal::NotFound);
        }
        if write_in_flight {
            return Err(ResolveRefusal::WriteInFlight);
        }
        if let Some(entry) = self.entries.remove(root) {
            crate::hub_log::hub_log!(
                "repo_session_type_quarantine_resolved id={} root={}",
                entry.id,
                root.display(),
            );
        }
        Ok(())
    }

    /// Logical bytes of the Status rows `status_rows` builds, without
    /// allocating them. `None` when they exceed `limit`.
    pub(crate) fn status_rows_bytes(&self, limit: usize) -> Option<usize> {
        let mut bytes = self
            .entries
            .len()
            .checked_mul(size_of::<DaemonQuarantine>())?;
        for entry in self.entries.values() {
            let mut detail = CountingWriter(0);
            write_detail(&mut detail, entry).ok()?;
            bytes = bytes
                .checked_add(entry.evidence.root.as_os_str().len())?
                .checked_add(entry.cause.code().len())?
                .checked_add(detail.0)?;
            if bytes > limit {
                return None;
            }
        }
        Some(bytes)
    }

    /// Status rows in root order.
    pub(crate) fn status_rows(&self) -> impl Iterator<Item = DaemonQuarantine> + '_ {
        self.entries.values().map(|entry| {
            let mut detail = String::new();
            let _ = write_detail(&mut detail, entry);
            DaemonQuarantine::RepositorySessionTypes {
                root: entry.evidence.root.clone(),
                cause: entry.cause.code().to_string(),
                detail,
                quarantined_at_ms: unix_millis(entry.quarantined_at),
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn note_refusal(&mut self, root: &Path, stage: QuarantineStage) {
        self.refusals.push((root.to_path_buf(), stage));
    }

    #[cfg(test)]
    pub(crate) fn refusals_for(&self, root: &Path) -> Vec<QuarantineStage> {
        self.refusals
            .iter()
            .filter(|(refused, _)| refused == root)
            .map(|(_, stage)| *stage)
            .collect()
    }

    /// The installed entry's cause, for tests.
    #[cfg(test)]
    pub(crate) fn cause_for(&self, root: &Path) -> Option<QuarantineCause> {
        self.entries.get(root).map(|entry| entry.cause)
    }
}

impl QuarantineCause {
    /// The stable code Status reports.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::PublishedUncertain(RepoPublicationCause::SyncFailed(_)) => {
                "published_uncertain_sync_failed"
            }
            Self::PublishedUncertain(RepoPublicationCause::IdentityUnconfirmed(_)) => {
                "published_uncertain_identity_unconfirmed"
            }
            Self::PublishedUncertain(RepoPublicationCause::ChangedAfterRename) => {
                "published_uncertain_changed_after_rename"
            }
            #[cfg(test)]
            Self::PublishedUncertain(RepoPublicationCause::Injected) => {
                "published_uncertain_injected"
            }
            Self::UnknownOutcome(UnknownOutcomeKind::WorkerPanicked) => {
                "unknown_outcome_worker_panicked"
            }
            Self::UnknownOutcome(UnknownOutcomeKind::ResultTooLarge) => {
                "unknown_outcome_result_too_large"
            }
            Self::UnknownOutcome(UnknownOutcomeKind::Other) => "unknown_outcome_other",
        }
    }
}

/// The Status detail: the write's target, identity, and digests. One writer
/// serves both the byte count and the row, so the two cannot disagree.
fn write_detail(
    out: &mut impl std::fmt::Write,
    entry: &RepoSessionTypeQuarantine,
) -> std::fmt::Result {
    let evidence = &entry.evidence;
    let operation = match evidence.operation {
        SessionTypeOperation::Create => "create",
        SessionTypeOperation::Update => "update",
        SessionTypeOperation::Delete => "delete",
    };
    write!(
        out,
        "target={} session_type_id={} operation={operation} prior_sha256=",
        evidence.target_path.display(),
        evidence.session_type_id,
    )?;
    match evidence.prior_sha256 {
        Some(digest) => write_hex(out, &digest)?,
        None => out.write_str("missing")?,
    }
    out.write_str(" candidate_sha256=")?;
    write_hex(out, &evidence.candidate_sha256)?;
    if let QuarantineCause::PublishedUncertain(
        RepoPublicationCause::SyncFailed(kind) | RepoPublicationCause::IdentityUnconfirmed(kind),
    ) = entry.cause
    {
        write!(out, " io_error={kind:?}")?;
    }
    Ok(())
}

fn write_hex(out: &mut impl std::fmt::Write, digest: &[u8; 32]) -> std::fmt::Result {
    for byte in digest {
        write!(out, "{byte:02x}")?;
    }
    Ok(())
}

/// Counts formatted bytes without storing them.
struct CountingWriter(usize);

impl std::fmt::Write for CountingWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

/// Milliseconds since the Unix epoch; zero for a clock before it.
pub(crate) fn unix_millis(at: SystemTime) -> u64 {
    at.duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Logical bytes an installed entry retains: the map node and every variable
/// field, all known before the effect.
fn entry_bytes(evidence: &RepoWriteEvidence) -> usize {
    let node =
        crate::lua_memory::layout::btree_nodes_checked::<PathBuf, RepoSessionTypeQuarantine>(1)
            .unwrap_or(usize::MAX);
    let key = evidence.root.as_os_str().len();
    let root = evidence.root.as_os_str().len();
    let target = evidence.target_path.as_os_str().len();
    let id = evidence.session_type_id.len();
    [node, key, root, target, id]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .unwrap_or(usize::MAX)
}

fn hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(64);
    for byte in digest {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence() -> RepoWriteEvidence {
        RepoWriteEvidence {
            root: PathBuf::from("/repo/root"),
            target_path: PathBuf::from("/repo/root/.botster/session-types.json"),
            session_type_id: "review".to_string(),
            operation: crate::host_mutations::SessionTypeOperation::Create,
            prior_sha256: None,
            candidate_sha256: [7; 32],
        }
    }

    #[test]
    fn an_unfunded_entry_is_refused_before_anything_is_retained() {
        let evidence = evidence();
        let needed = entry_bytes(&evidence);
        let short = SharedViewBudget::with_capacity(needed - 1);
        assert!(PendingRepoQuarantine::reserve(&evidence, &short).is_err());
        assert_eq!(short.used(), 0, "a refused reservation charges nothing");

        let exact = SharedViewBudget::with_capacity(needed);
        let pending = PendingRepoQuarantine::reserve(&evidence, &exact).expect("exact fit");
        assert_eq!(exact.used(), needed);
        let mut quarantines = RepoSessionTypeQuarantines::default();
        quarantines.install(
            pending,
            QuarantineCause::UnknownOutcome(UnknownOutcomeKind::Other),
            "test",
        );
        assert_eq!(exact.used(), needed, "installation needs no new capacity");
        drop(quarantines);
        assert_eq!(exact.used(), 0, "the entry returns its charge when dropped");
    }

    fn installed(budget: &std::sync::Arc<SharedViewBudget>) -> RepoSessionTypeQuarantines {
        let pending = PendingRepoQuarantine::reserve(&evidence(), budget).expect("reserve");
        let mut quarantines = RepoSessionTypeQuarantines::default();
        quarantines.install(
            pending,
            QuarantineCause::PublishedUncertain(RepoPublicationCause::SyncFailed(
                std::io::ErrorKind::Other,
            )),
            "test",
        );
        quarantines
    }

    #[test]
    fn resolve_removes_the_entry_and_releases_its_charge() {
        let budget = SharedViewBudget::with_capacity(entry_bytes(&evidence()));
        let mut quarantines = installed(&budget);
        let root = evidence().root;
        assert_eq!(
            quarantines.resolve(Path::new("/repo/other"), false),
            Err(ResolveRefusal::NotFound)
        );
        assert_eq!(
            quarantines.resolve(&root, true),
            Err(ResolveRefusal::WriteInFlight),
            "a write still in flight for the root refuses the resolve"
        );
        assert!(quarantines.contains(&root));
        assert_eq!(budget.used(), entry_bytes(&evidence()));
        assert_eq!(quarantines.resolve(&root, false), Ok(()));
        assert!(!quarantines.contains(&root));
        assert_eq!(budget.used(), 0, "the resolved entry returns its charge");
        assert_eq!(
            quarantines.resolve(&root, false),
            Err(ResolveRefusal::NotFound)
        );
    }

    #[test]
    fn status_row_bytes_match_the_rows_built() {
        let budget = SharedViewBudget::with_capacity(entry_bytes(&evidence()));
        let quarantines = installed(&budget);
        let rows: Vec<_> = quarantines.status_rows().collect();
        assert_eq!(rows.len(), 1);
        let DaemonQuarantine::RepositorySessionTypes {
            root,
            cause,
            detail,
            quarantined_at_ms,
        } = &rows[0]
        else {
            panic!("a repository row: {rows:?}");
        };
        assert_eq!(root, &evidence().root);
        assert_eq!(cause, "published_uncertain_sync_failed");
        assert!(detail.contains("session_type_id=review"), "{detail}");
        assert!(detail.contains("prior_sha256=missing"), "{detail}");
        assert!(detail.ends_with("io_error=Other"), "{detail}");
        assert!(*quarantined_at_ms > 0);
        let expected =
            size_of::<DaemonQuarantine>() + root.as_os_str().len() + cause.len() + detail.len();
        assert_eq!(quarantines.status_rows_bytes(usize::MAX), Some(expected));
        assert_eq!(quarantines.status_rows_bytes(expected), Some(expected));
        assert_eq!(quarantines.status_rows_bytes(expected - 1), None);
    }
}
