//! Owner-only quarantine of repository session-type roots.
//!
//! A repository session-types write whose outcome is uncertain after its
//! rename, or unknown because its Host job ended without a mutation result,
//! blocks further writes to that repository root until an operator resolves
//! it or the Hub restarts. Reads, sessions, and transport continue. The
//! entry's bytes are reserved from the Hub-state view budget before the file
//! effect, so recording it never needs new capacity.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::host_mutations::RepoWriteEvidence;
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
/// Status listing and the operator resolve read these fields; until then the
/// entry holds them and the Hub log records them at installation.
pub(crate) struct RepoSessionTypeQuarantine {
    _id: u64,
    _evidence: RepoWriteEvidence,
    _cause: QuarantineCause,
    _quarantined_at: SystemTime,
    _charge: SharedViewCharge,
}

/// The evidence and reserved charge a repository commit carries until its
/// outcome is known. Dropping it releases the reservation.
pub(crate) struct PendingRepoQuarantine {
    evidence: RepoWriteEvidence,
    charge: SharedViewCharge,
}

impl PendingRepoQuarantine {
    /// Reserve an entry's bytes before the file effect.
    pub(crate) fn reserve(
        evidence: RepoWriteEvidence,
        budget: &std::sync::Arc<SharedViewBudget>,
    ) -> Result<Self, SharedViewCapacityError> {
        let charge = budget.reserve(entry_bytes(&evidence))?;
        Ok(Self { evidence, charge })
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
    pub(crate) fn contains(&self, root: &Path) -> bool {
        self.entries.contains_key(root)
    }

    /// Install the entry funded by `pending`. The map is touched only on the
    /// owner, and the bytes were reserved before the effect.
    pub(crate) fn install(&mut self, pending: PendingRepoQuarantine, cause: QuarantineCause) {
        let PendingRepoQuarantine { evidence, charge } = pending;
        self.next_id = self.next_id.saturating_add(1);
        crate::hub_log::hub_log!(
            "repo_session_type_quarantined id={} root={} target={} session_type_id={} operation={:?} prior_sha256={} candidate_sha256={} cause={cause:?}",
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
            _id: self.next_id,
            _evidence: evidence,
            _cause: cause,
            _quarantined_at: SystemTime::now(),
            _charge: charge,
        };
        // One unresolved entry per root: every later write to it is refused.
        let previous = self.entries.insert(root, entry);
        debug_assert!(previous.is_none(), "a quarantined root admits no write");
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
        self.entries.get(root).map(|entry| entry._cause)
    }
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
