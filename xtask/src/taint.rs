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
    let (lines, verdict) = report(scanned, tokens.len(), &hits);
    for line in lines {
        println!("{line}");
    }
    verdict.map_err(anyhow::Error::msg)
}

/// The report of a scan of `scanned` files with `names` banned names: one line for each hit, then the summary; and the
/// verdict, which fails on any hit.
pub fn report(
    scanned: usize,
    names: usize,
    hits: &[(String, usize, String)],
) -> (Vec<String>, Result<(), String>) {
    let mut lines: Vec<String> = hits
        .iter()
        .map(|(file, line, token)| format!("{file}:{line}: banned name '{token}'"))
        .collect();
    lines.push(format!(
        "taint: {scanned} files scanned, {names} names from botster-contracts"
    ));
    let verdict = if hits.is_empty() {
        Ok(())
    } else {
        Err(format!("{} hit(s)", hits.len()))
    };
    (lines, verdict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_hit_is_reported_and_any_hit_fails() {
        assert_eq!(
            report(3, 2, &[]),
            (
                vec!["taint: 3 files scanned, 2 names from botster-contracts".to_string()],
                Ok(())
            )
        );
        let hits = [
            ("a.rs".to_string(), 4, "old".to_string()),
            ("b.md".to_string(), 1, "older".to_string()),
        ];
        assert_eq!(
            report(3, 2, &hits),
            (
                vec![
                    "a.rs:4: banned name 'old'".to_string(),
                    "b.md:1: banned name 'older'".to_string(),
                    "taint: 3 files scanned, 2 names from botster-contracts".to_string(),
                ],
                Err("2 hit(s)".to_string())
            )
        );
    }
}
