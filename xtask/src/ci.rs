//! `cargo xtask ci`: the Hub's gate (repo plan section 5, the minimal gate). Each step runs, in order; the summary names
//! each step's result and time, and the gate fails when a step fails.

use crate::{budget, forbid_unsafe, lists, taint, zig_packages};
use anyhow::{bail, Result};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

/// One step: its name and how it runs.
type Step = (&'static str, fn(&Path) -> Result<()>);

/// `Ok` when a process succeeded, or the error that names it with `detail` (its stderr, or its exit status).
///
/// # Errors
/// The process failed.
pub fn succeeded(what: &str, success: bool, detail: &str) -> Result<(), String> {
    if success {
        Ok(())
    } else {
        Err(format!("{what} failed: {}", detail.trim_end()))
    }
}

/// Runs cargo with `args`, with its output on the terminal.
fn cargo(root: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("cargo")
        .current_dir(root)
        .args(args)
        .status()?;
    succeeded(
        &format!("cargo {}", args.join(" ")),
        status.success(),
        &status.to_string(),
    )
    .map_err(anyhow::Error::msg)
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
    ("zig-packages", |root| zig_packages::command(root, &[])),
    ("test-budget", |root| budget::command(root, &[])),
    ("slow", |root| {
        budget::command(root, &["--slow".to_string()])
    }),
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

/// The verdict of the gate: it fails when a step failed, and names each failed step.
///
/// # Errors
/// The failed steps.
pub fn verdict(results: &[(&str, bool, f64)]) -> Result<(), String> {
    let failed: Vec<&str> = results
        .iter()
        .filter(|(_, passed, _)| !passed)
        .map(|(name, _, _)| *name)
        .collect();
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!("ci failed: {}", failed.join(", ")))
    }
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
    verdict(&results).map_err(anyhow::Error::msg)
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
                "zig-packages",
                "test-budget",
                "slow"
            ]
        );
    }

    /// The value of `key = "..."` on a line of a TOML file that this test reads as text.
    fn quoted(file: &str, key: &str) -> String {
        let text =
            std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(file))
                .unwrap();
        text.lines()
            .find_map(|line| {
                line.strip_prefix(key)?
                    .trim_start()
                    .strip_prefix("= \"")?
                    .split('"')
                    .next()
                    .map(String::from)
            })
            .unwrap_or_else(|| panic!("{file} has no {key}"))
    }

    /// The pool installs the tools of mise.toml: its Rust is the channel of rust-toolchain.toml.
    #[test]
    fn the_pool_installs_the_rust_toolchain_of_the_repository() {
        assert_eq!(
            quoted("mise.toml", "rust"),
            quoted("rust-toolchain.toml", "channel")
        );
    }

    #[test]
    fn a_failed_process_is_named_with_its_detail() {
        assert_eq!(succeeded("cargo fmt", true, "x"), Ok(()));
        assert_eq!(
            succeeded("cargo fmt", false, "exit status: 1\n"),
            Err("cargo fmt failed: exit status: 1".to_string())
        );
    }

    #[test]
    fn the_gate_fails_when_any_step_failed_and_names_each() {
        assert_eq!(verdict(&[("fmt", true, 0.1), ("lists", true, 0.2)]), Ok(()));
        assert_eq!(verdict(&[]), Ok(()));
        assert_eq!(
            verdict(&[
                ("fmt", false, 0.1),
                ("lists", true, 0.2),
                ("budget", false, 3.0)
            ]),
            Err("ci failed: fmt, budget".to_string())
        );
    }

    #[test]
    fn the_summary_names_each_result_and_time() {
        assert_eq!(
            summary(&[("fmt", true, 0.25), ("budget", false, 88.06)]),
            [
                "=== ci summary ===".to_string(),
                "fmt              PASS        0.2 s".to_string(),
                "budget           FAIL       88.1 s".to_string(),
            ]
        );
    }
}
