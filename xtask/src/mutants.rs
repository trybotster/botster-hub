//! `cargo xtask mutants`: mutation tests of the HIGH-path code that the PR changes (repo plan section 5: mutants on HIGH
//! paths only; lead 2026-10-12: one stage, the default tier).
//!
//! The diff is `base...HEAD` limited to the entries of `ci/high-tier-paths.txt`. cargo-mutants mutates the tree in place
//! and runs the tests with nextest's `mutants` profile and `--max-fail 1:immediate`, so a mutant that a test fails is
//! CAUGHT even when it also hangs another test, and a mutant that hangs and fails no test is a TIMEOUT. The verdict is
//! Core's (`botster_gate_kit::mutant_outcomes`, H212-I1): the run must test exactly the listed mutants and write their
//! outcomes, its log report must be valid (a retried baseline or a catch without a failed test fails), and a missed
//! mutant, a timeout or a failed baseline fails the step. Each catch is printed with its test.

use crate::ci::succeeded;
use anyhow::{bail, Context, Result};
use botster_gate_kit::high_tier::LIST;
use botster_gate_kit::mutant_outcomes::{
    count_difference, mutation_verdict, parse_listing, parse_stage_outcomes, report_lines,
    stage_outcomes, Stage,
};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

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

/// The arguments of the listing after `cargo`: the run's own filters, as JSON (`parse_listing`).
pub fn list_args(diff: &str) -> Vec<String> {
    ["mutants", "--in-diff", diff, "--list", "--json"]
        .map(String::from)
        .to_vec()
}

/// The note of a diff with no Rust source on a HIGH path, or of a listing with no mutant: then no run starts.
pub fn no_run(base: &str, rust: bool, listed: usize) -> Option<String> {
    match (rust, listed) {
        (false, _) => Some(format!(
            "mutants: the diff from {base} changes no Rust source on a HIGH path: no mutant"
        )),
        (true, 0) => Some(format!(
            "mutants: the diff from {base} has no mutant (cargo mutants --list): no run"
        )),
        (true, _) => None,
    }
}

/// The removal of the output of an earlier run: an absent output is fine; any other error fails, so an earlier
/// `outcomes.json` can never stand for this run.
///
/// # Errors
/// The output exists and cannot be removed.
pub fn removed(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// The verdict of the one stage (lead 2026-10-12): the run tested exactly the `listed` mutants by full name
/// (`count_difference`), it wrote outcomes for each one, its log report is valid (a rejected report fails, H212-I1), and
/// its exit code is 0 (`mutation_verdict`). `out` gets the stage's line and its report lines.
///
/// # Errors
/// A selection error, no outcomes or outcomes for another number of mutants, a rejected log report, or a failing exit code.
pub fn decision(listed: &[String], stage: Stage, out: &mut impl FnMut(String)) -> Result<()> {
    if let Some(outcomes) = &stage.outcomes {
        let (extra, absent) = count_difference(listed, &outcomes.tested);
        if !extra.is_empty() || !absent.is_empty() {
            bail!(
                "mutants: {STAGE}: selection error: it tested {extra:?}, which the listing does not hold, and did not test \
                 {absent:?}"
            );
        }
    }
    let (code, outcomes, line) = stage_outcomes(STAGE, listed.len(), stage)?;
    out(line.clone());
    report_lines(STAGE, &outcomes.report, out)?;
    mutation_verdict(code).context(line)
}

/// The name of the one stage in its lines.
const STAGE: &str = "the default tier";

/// The step.
///
/// # Errors
/// git or cargo-mutants fails, or the `decision` fails.
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
    succeeded(
        "git diff",
        diff.status.success(),
        &String::from_utf8_lossy(&diff.stderr),
    )
    .map_err(anyhow::Error::msg)?;
    let rust = changes_rust(&String::from_utf8_lossy(&diff.stdout));
    let diff_file = root.join(OUTPUT).join("mutants.diff");
    std::fs::create_dir_all(root.join(OUTPUT)).context("create the output directory")?;
    std::fs::write(&diff_file, &diff.stdout).context("write the diff")?;
    let diff_arg = diff_file.display().to_string();
    let listed = if rust {
        let listing = Command::new("cargo")
            .current_dir(root)
            .args(list_args(&diff_arg))
            .output()
            .context("run cargo mutants --list")?;
        parse_listing(&listing)?
    } else {
        Vec::new()
    };
    if let Some(note) = no_run(&base, rust, listed.len()) {
        println!("{note}");
        return Ok(());
    }
    let out = root.join(OUTPUT).join("mutants.out");
    removed(std::fs::remove_dir_all(&out)).with_context(|| format!("remove {}", out.display()))?;
    let start = Instant::now();
    let status = Command::new("cargo")
        .current_dir(root)
        .args(run_args(&diff_arg))
        .status()
        .context("run cargo mutants")?;
    let read = |log: &str| {
        std::fs::read_to_string(out.join(log))
            .with_context(|| format!("read {}", out.join(log).display()))
    };
    let outcomes = std::fs::read_to_string(out.join("outcomes.json"))
        .ok()
        .map(|json| parse_stage_outcomes(&json, &[], read))
        .transpose()?;
    let stage = Stage {
        code: status.code(),
        outcomes,
        took: start.elapsed(),
    };
    decision(&listed, stage, &mut |line| println!("{line}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_gate_kit::mutant_outcomes::{MutantSummary, Outcomes};
    use std::time::Duration;

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
    fn the_listing_has_the_runs_diff() {
        assert_eq!(
            list_args("d.diff").join(" "),
            "mutants --in-diff d.diff --list --json"
        );
    }

    #[test]
    fn no_run_starts_without_rust_source_or_without_a_mutant() {
        assert_eq!(
            no_run("v1", false, 3),
            Some(
                "mutants: the diff from v1 changes no Rust source on a HIGH path: no mutant"
                    .to_string()
            )
        );
        assert_eq!(
            no_run("v1", true, 0),
            Some(
                "mutants: the diff from v1 has no mutant (cargo mutants --list): no run"
                    .to_string()
            )
        );
        assert_eq!(no_run("v1", true, 1), None);
    }

    #[test]
    fn only_an_absent_earlier_output_or_a_removed_one_is_fine() {
        use std::io::{Error, ErrorKind};
        assert!(removed(Ok(())).is_ok());
        assert!(removed(Err(Error::from(ErrorKind::NotFound))).is_ok());
        assert_eq!(
            removed(Err(Error::from(ErrorKind::PermissionDenied)))
                .unwrap_err()
                .kind(),
            ErrorKind::PermissionDenied
        );
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    /// A stage whose tested mutants are `tested`, with `caught` of them caught and the rest missed.
    fn stage(
        code: Option<i32>,
        tested: &[&str],
        caught: u64,
        report: Result<Vec<String>, String>,
    ) -> Stage {
        let total = tested.len() as u64;
        Stage {
            code,
            outcomes: Some(Outcomes {
                summary: MutantSummary {
                    total,
                    caught,
                    missed: total - caught,
                    timeout: 0,
                    unviable: 0,
                },
                missed: names(&tested[usize::try_from(caught).unwrap()..]),
                tested: names(tested),
                report,
            }),
            took: Duration::from_millis(1500),
        }
    }

    fn decide(listed: &[&str], stage: Stage) -> (Vec<String>, std::result::Result<(), String>) {
        let mut lines = Vec::new();
        let result = decision(&names(listed), stage, &mut |line| lines.push(line))
            .map_err(|e| format!("{e:#}"));
        (lines, result)
    }

    /// The step passes only when the run tested exactly the listed mutants, wrote their outcomes, gave a valid log report
    /// and exited 0; each other case fails with its reason (H212-I1).
    #[test]
    fn the_step_passes_only_with_every_listed_outcome_a_valid_report_and_exit_0() {
        let caught = Ok(vec!["caught `a` by w t::a".to_string()]);
        let (lines, result) = decide(&["a"], stage(Some(0), &["a"], 1, caught.clone()));
        assert_eq!(result, Ok(()));
        assert_eq!(
            lines,
            [
                "mutants: the default tier: 1 mutants: 1 caught, 0 missed, 0 timeout, 0 unviable, in 1.5 s",
                "mutants: the default tier: caught `a` by w t::a"
            ]
        );
        // Exit 0, but the report rejects the logs (a retried baseline).
        let (lines, result) = decide(
            &["a"],
            stage(
                Some(0),
                &["a"],
                1,
                Err("the unmutated baseline retried".into()),
            ),
        );
        assert_eq!(
            result,
            Err(
                "mutants: the default tier: the logs fail the step: the unmutated baseline retried"
                    .to_string()
            )
        );
        assert_eq!(lines.len(), 1);
        // No outcomes.json.
        let none = Stage {
            code: Some(0),
            outcomes: None,
            took: Duration::ZERO,
        };
        assert!(decide(&["a"], none)
            .1
            .unwrap_err()
            .contains("wrote no outcomes.json"));
        // A missed mutant.
        let (_, result) = decide(&["a", "b"], stage(Some(2), &["a", "b"], 1, Ok(vec![])));
        assert!(result
            .unwrap_err()
            .starts_with("mutants: the default tier: 2 mutants: 1 caught, 1 missed"));
    }

    #[test]
    fn a_run_that_tests_another_set_than_the_listing_is_a_selection_error() {
        for (listed, tested) in [
            (vec!["a", "b"], vec!["a"]),
            (vec!["a"], vec!["a", "c"]),
            (vec!["a"], vec!["c"]),
        ] {
            let (lines, result) = decide(
                &listed,
                stage(Some(0), &tested, tested.len() as u64, Ok(vec![])),
            );
            assert!(
                result.unwrap_err().contains("selection error"),
                "{listed:?} {tested:?}"
            );
            assert!(lines.is_empty());
        }
    }
}
