//! `cargo xtask taint`: whole-word match of the old-world names over every tracked file (BUILD.md "Starting fresh" rule
//! 5), with the rule of `botster_gate_kit::taint`. The denylist is the one of botster-contracts at the pinned tag; the Hub
//! adds no names of its own yet.

use crate::contracts;
use anyhow::{bail, Context, Result};
use botster_gate_kit::files::tracked_files;
use botster_gate_kit::taint::{parse_denylist, scan_files};
use std::path::Path;

/// The check over the repository's tracked files.
///
/// # Errors
/// A file could not be read, or a banned name was found.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        bail!("unknown argument '{arg}'");
    }
    let denylist = contracts::root(root)?.join("docs/taint-denylist.txt");
    let tokens = parse_denylist(
        &std::fs::read_to_string(&denylist)
            .with_context(|| format!("read {}", denylist.display()))?,
    );
    let mut files = Vec::new();
    for file in tracked_files(root)? {
        if let Ok(bytes) = std::fs::read(root.join(&file)) {
            files.push((file, String::from_utf8_lossy(&bytes).into_owned()));
        }
    }
    let (scanned, hits) = scan_files(&files, &tokens);
    for (file, line, token) in &hits {
        eprintln!("{file}:{line}: banned name '{token}'");
    }
    println!(
        "taint: {scanned} files scanned, {} names from botster-contracts",
        tokens.len()
    );
    if !hits.is_empty() {
        bail!("{} hit(s)", hits.len());
    }
    Ok(())
}
