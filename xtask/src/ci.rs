//! `cargo xtask ci`: the Hub's gate (repo plan section 5, the minimal gate). Each step runs, in order; the summary names
//! each step's result and time, and the gate fails when a step fails.

use crate::{budget, forbid_unsafe, lists, mutants, taint};
use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

/// One step: its name and how it runs.
type Step = (&'static str, fn(&Path) -> Result<()>);

fn cargo(root: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("cargo")
        .current_dir(root)
        .args(args)
        .status()?;
    if !status.success() {
        bail!("cargo {} failed ({status})", args.join(" "));
    }
    Ok(())
}

/// The steps of the gate, in order.
pub const STEPS: [Step; 9] = [
    ("fmt", |root| cargo(root, &["fmt", "--all", "--check"])),
    ("clippy", |root| {
        cargo(
            root,
            &[
                "clippy",
                "--workspace",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ],
        )
    }),
    ("taint", |root| taint::command(root, &[])),
    ("unsafe-code", |root| forbid_unsafe::command(root, &[])),
    ("high-tier", |root| {
        botster_gate_kit::high_tier::command(root, &[])
    }),
    ("lists", |root| lists::command(root, &[])),
    ("test-budget", |root| budget::command(root, &[])),
    ("slow", |root| {
        budget::command(root, &["--slow".to_string()])
    }),
    ("mutants", |root| mutants::command(root, &[])),
];

/// The summary table: one line a step, `PASS` or `FAIL` with its time in seconds.
pub fn summary(results: &[(&str, bool, f64)]) -> Vec<String> {
    let mut lines = vec!["=== ci summary ===".to_string()];
    lines.extend(results.iter().map(|(name, passed, secs)| {
        format!(
            "{name:<16} {:<6} {secs:>8.1} s",
            if *passed { "PASS" } else { "FAIL" }
        )
    }));
    lines
}

/// The gate.
///
/// # Errors
/// A step failed.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        bail!("unknown argument '{arg}'");
    }
    let mut results = Vec::new();
    for (name, run) in STEPS {
        println!("=== ci: {name} ===");
        let start = Instant::now();
        let outcome = run(root);
        if let Err(error) = &outcome {
            println!("ci: {name} FAILED: {error:#}");
        }
        results.push((name, outcome.is_ok(), start.elapsed().as_secs_f64()));
    }
    for line in summary(&results) {
        println!("{line}");
    }
    let failed: Vec<&str> = results.iter().filter(|r| !r.1).map(|r| r.0).collect();
    if !failed.is_empty() {
        bail!("ci failed: {}", failed.join(", "));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_steps_are_the_minimal_gate_in_order() {
        let names: Vec<&str> = STEPS.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            [
                "fmt",
                "clippy",
                "taint",
                "unsafe-code",
                "high-tier",
                "lists",
                "test-budget",
                "slow",
                "mutants"
            ]
        );
    }

    #[test]
    fn the_summary_names_each_result_and_time() {
        assert_eq!(
            summary(&[("fmt", true, 0.25), ("mutants", false, 88.06)]),
            [
                "=== ci summary ===".to_string(),
                "fmt              PASS        0.2 s".to_string(),
                "mutants          FAIL       88.1 s".to_string(),
            ]
        );
    }
}
