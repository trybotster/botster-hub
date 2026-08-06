//! The managed installation prefix layout.
//!
//! ```text
//! <prefix>/
//!   daemon.lock                                   # the installation lease
//!   generations/
//!     <hub-sha>-<core-sha>/                       # one revision-coupled pair
//!       botster-hub
//!       botster-session-worker
//!   current -> generations/<hub-sha>-<core-sha>   # the pointer
//!   bin/
//!     botster-hub -> ../current/botster-hub
//! ```
//!
//! The Hub and its locked-Core worker are one indivisible generation. The
//! generation id *is* the revision pair, so the directory name states the
//! coupling rather than merely implying it, and both binaries are only ever
//! reachable through one pointer that flips atomically.

use std::path::{Path, PathBuf};

use crate::receipt::is_canonical_object_id;

/// Directory holding every installed generation.
pub const GENERATIONS_DIRECTORY: &str = "generations";
/// The single pointer naming the live generation.
pub const CURRENT_POINTER: &str = "current";
/// Directory holding the stable launch entrypoint.
pub const BIN_DIRECTORY: &str = "bin";
/// Installed Hub binary name.
pub const HUB_BINARY_NAME: &str = "botster-hub";
/// Installed session-worker binary name.
pub const WORKER_BINARY_NAME: &str = "botster-session-worker";
/// The only target `bin/botster-hub` may have.
pub const BIN_HUB_SYMLINK_TARGET: &str = "../current/botster-hub";
/// Installation-scoped lease file, kept above `generations/` so it survives a
/// generation switch. A lock inside a generation would be swapped out from
/// under its own holder.
pub const DAEMON_LOCK_FILE: &str = "daemon.lock";
/// Prefix marking an in-progress, not-yet-published generation.
pub const STAGING_PREFIX: &str = ".staging-";

/// Build the generation directory name for a revision pair.
///
/// Returns `None` unless both revisions are canonical lowercase-hex object ids.
/// A signature over the manifest proves authorship, not path safety, and
/// descriptor-relative `renameat` confines nothing when the name itself carries
/// `/` or `..` — so the name is constructed only from validated values.
#[must_use]
pub fn generation_name(hub_revision: &str, core_revision: &str) -> Option<String> {
    if !is_canonical_object_id(hub_revision) || !is_canonical_object_id(core_revision) {
        return None;
    }
    Some(format!("{hub_revision}-{core_revision}"))
}

/// Derive the managed installation prefix from a running executable path.
///
/// Matching the layout *shape* rather than walking a fixed number of levels is
/// what makes this correct whether or not `current_exe()` resolves the `bin`
/// symlink on a given platform. Counting levels would silently produce two
/// different prefixes — and two independent leases — across platforms, which is
/// enforcement that appears to work and does not.
///
/// Anything not matching a managed layout — a development build — derives no
/// prefix and therefore takes no lease.
#[must_use]
pub fn derive_managed_prefix(executable: &Path) -> Option<PathBuf> {
    if executable.file_name()? != std::ffi::OsStr::new(HUB_BINARY_NAME) {
        return None;
    }
    let parent = executable.parent()?;
    let candidate = if parent.file_name()? == std::ffi::OsStr::new(BIN_DIRECTORY) {
        parent.parent()?
    } else {
        let generations = parent.parent()?;
        if generations.file_name()? != std::ffi::OsStr::new(GENERATIONS_DIRECTORY) {
            return None;
        }
        generations.parent()?
    };
    if !is_managed_prefix(candidate) {
        return None;
    }
    Some(candidate.to_path_buf())
}

/// Whether a candidate directory carries both marks of a managed prefix.
///
/// This is a shape test on a path this process derived from its own
/// `current_exe()`, not a security boundary — the security boundary is the
/// descriptor-relative discipline in [`crate::safety`] — so a path-based stat is
/// the right tool here.
#[must_use]
pub fn is_managed_prefix(candidate: &Path) -> bool {
    let generations = candidate.join(GENERATIONS_DIRECTORY);
    let pointer = candidate.join(CURRENT_POINTER);
    std::fs::symlink_metadata(&generations).is_ok_and(|metadata| metadata.is_dir())
        && std::fs::symlink_metadata(&pointer).is_ok()
}
