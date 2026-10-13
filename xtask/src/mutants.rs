//! `cargo xtask mutants`: mutation tests of the HIGH-path code that the PR changes (repo plan section 5: mutants on HIGH
//! paths only; lead 2026-10-12: one stage, the default tier).
//!
//! The diff is `base...HEAD` limited to the entries of `ci/high-tier-paths.txt`. cargo-mutants mutates the tree in place
//! and runs the tests with nextest's `mutants` profile and `--max-fail 1:immediate`, so a mutant that a test fails is
//! CAUGHT even when it also hangs another test, and a mutant that hangs and fails no test is a TIMEOUT. A missed mutant,
//! a timeout or a failed baseline fails the step. Each catch is printed with its test (`botster_gate_kit::mutant_logs`).

use anyhow::{bail, Context, Result};
use botster_gate_kit::high_tier::LIST;
use botster_gate_kit::mutant_logs::stage_report;
use std::path::Path;
use std::process::Command;

/// The output directory of cargo-mutants, relative to the root (`<it>/mutants.out`).
const OUTPUT: &str = "target";

/// The git pathspecs of the HIGH-path list: each entry before its `#` reason, a `dir/**` entry as a glob.
pub fn pathspecs(list: &str) -> Vec<String> {
    list.lines()
        .map(|line| line.split('#').next().unwrap_or_default().trim())
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            if entry.ends_with("/**") {
                format!(":(glob){entry}")
            } else {
                entry.to_string()
            }
        })
        .collect()
}

/// Whether the diff changes a Rust source: a new path (`+++ b/...`) that ends in `.rs`. Without one there is no mutant.
pub fn changes_rust(diff: &str) -> bool {
    diff.lines()
        .filter_map(|line| line.strip_prefix("+++ "))
        .any(|path| {
            path.trim_end_matches('\t')
                .trim_matches('"')
                .ends_with(".rs")
        })
}

/// The arguments of the run after `cargo`.
pub fn run_args(diff: &str) -> Vec<String> {
    [
        "mutants",
        "--in-diff",
        diff,
        "--in-place",
        "--no-shuffle",
        "--test-tool",
        "nextest",
        "--timeout-multiplier",
        "5",
        "--caught",
        "--unviable",
        "--output",
        OUTPUT,
        "--",
        "--profile",
        "mutants",
        "--max-fail",
        "1:immediate",
        "--no-tests=pass",
    ]
    .map(String::from)
    .to_vec()
}

/// The verdict of cargo-mutants' exit code: 0 passes; 2 a missed mutant, 3 a timeout, 4 a failed baseline and any other
/// end fail.
///
/// # Errors
/// The reason the step fails.
pub fn verdict(code: Option<i32>) -> Result<(), String> {
    match code {
        Some(0) => Ok(()),
        Some(2) => Err("a mutant was missed: a review finding".into()),
        Some(3) => Err("a mutant timed out: a review finding".into()),
        Some(4) => Err("the unmutated baseline failed".into()),
        other => Err(format!("cargo mutants failed (exit {other:?})")),
    }
}

/// The step.
///
/// # Errors
/// git or cargo-mutants fails, or the verdict fails.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        bail!("unknown argument '{arg}'");
    }
    let base = std::env::var("BOTSTER_CI_BASE_REF").unwrap_or_else(|_| "origin/v1".into());
    let list = std::fs::read_to_string(root.join(LIST)).with_context(|| format!("read {LIST}"))?;
    let diff = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "diff",
            "--no-color",
            "--no-ext-diff",
            &format!("{base}...HEAD"),
            "--",
        ])
        .args(pathspecs(&list))
        .output()
        .context("run git diff")?;
    if !diff.status.success() {
        bail!("git diff failed: {}", String::from_utf8_lossy(&diff.stderr));
    }
    let diff_text = String::from_utf8_lossy(&diff.stdout);
    if !changes_rust(&diff_text) {
        println!("mutants: the diff from {base} changes no Rust source on a HIGH path: no mutant");
        return Ok(());
    }
    let diff_file = root.join(OUTPUT).join("mutants.diff");
    std::fs::create_dir_all(root.join(OUTPUT)).context("create the output directory")?;
    std::fs::write(&diff_file, diff.stdout).context("write the diff")?;
    let status = Command::new("cargo")
        .current_dir(root)
        .args(run_args(&diff_file.display().to_string()))
        .status()
        .context("run cargo mutants")?;
    let out = root.join(OUTPUT).join("mutants.out");
    if let Ok(json) = std::fs::read_to_string(out.join("outcomes.json")) {
        let read = |log: &str| {
            std::fs::read_to_string(out.join(log)).with_context(|| format!("read {log}"))
        };
        match stage_report(&json, &[], read) {
            Ok(report) => report
                .lines
                .iter()
                .for_each(|line| println!("mutants: {line}")),
            Err(error) => println!("mutants: the logs could not be read: {error}"),
        }
    }
    verdict(status.code()).map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_entry_is_a_pathspec_and_a_directory_entry_a_glob() {
        let list = "# head\n\nxtask/** # gate code\nCargo.toml  # the lints\n   # only a comment\n";
        assert_eq!(pathspecs(list), [":(glob)xtask/**", "Cargo.toml"]);
    }

    #[test]
    fn only_a_new_rust_path_is_a_change_to_rust() {
        assert!(changes_rust("--- a/x.rs\n+++ b/x.rs\n@@\n"));
        assert!(changes_rust("+++ \"b/a b.rs\"\t\n"));
        assert!(!changes_rust("--- a/x.rs\n+++ /dev/null\n"));
        assert!(!changes_rust("+++ b/Cargo.toml\n+ fn f() {} // a.rs\n"));
        assert!(!changes_rust(""));
    }

    #[test]
    fn the_run_is_in_place_with_the_mutants_profile() {
        assert_eq!(
            run_args("d.diff").join(" "),
            "mutants --in-diff d.diff --in-place --no-shuffle --test-tool nextest --timeout-multiplier 5 --caught \
             --unviable --output target -- --profile mutants --max-fail 1:immediate --no-tests=pass"
        );
    }

    #[test]
    fn only_exit_zero_passes() {
        assert_eq!(verdict(Some(0)), Ok(()));
        assert_eq!(
            verdict(Some(2)),
            Err("a mutant was missed: a review finding".into())
        );
        assert_eq!(
            verdict(Some(3)),
            Err("a mutant timed out: a review finding".into())
        );
        assert_eq!(
            verdict(Some(4)),
            Err("the unmutated baseline failed".into())
        );
        assert_eq!(
            verdict(Some(1)),
            Err("cargo mutants failed (exit Some(1))".into())
        );
        assert_eq!(
            verdict(None),
            Err("cargo mutants failed (exit None)".into())
        );
    }
}
