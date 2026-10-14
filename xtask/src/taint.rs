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
    let files = read_files(root, &tracked_files(root)?)?;
    let (scanned, hits) = scan_files(&files, &tokens);
    let (lines, verdict) = report(scanned, tokens.len(), &hits);
    for line in lines {
        println!("{line}");
    }
    verdict.map_err(anyhow::Error::msg)
}

/// The text of each file under `root`.
///
/// # Errors
/// A file could not be read: a scan that skips a file passes without checking it.
fn read_files(root: &Path, names: &[String]) -> Result<Vec<(String, String)>> {
    names
        .iter()
        .map(|file| {
            let bytes = std::fs::read(root.join(file)).with_context(|| format!("read {file}"))?;
            Ok((file.clone(), String::from_utf8_lossy(&bytes).into_owned()))
        })
        .collect()
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
    fn a_file_that_cannot_be_read_fails_the_scan() {
        let dir = std::env::temp_dir().join(format!("hub-taint-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), "text").unwrap();
        let read = read_files(&dir, &["a.txt".to_string()]).unwrap();
        assert_eq!(read, [("a.txt".to_string(), "text".to_string())]);
        let error = read_files(&dir, &["a.txt".to_string(), "gone.txt".to_string()]).unwrap_err();
        assert!(
            format!("{error:#}").starts_with("read gone.txt"),
            "{error:#}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

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
