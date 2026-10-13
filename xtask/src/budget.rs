//! `cargo xtask test-budget [--slow]`: a test tier under cargo-nextest with its budgets (BUILD.md "Testing: fast and
//! lean"; the values of Core's xtask).
//!
//! The default tier: nextest's `ci` profile terminates a test at 2 s and fails it (`.config/nextest.toml`), and the tier
//! as a whole must finish in 60 s. The slow tier runs the `slow` features of the members that have one, with no per-test
//! limit and a 600 s deadline; with no such member it has no test and passes. The slowest tests are printed from nextest's
//! JUnit file. The run is Core's bounded run (`botster_gate_kit::bounded`, H212-I3): nextest runs in its own process
//! group, the group is killed at the deadline, and every process that the run left behind is reported, killed, and fails
//! the tier.

use crate::ci::succeeded;
use anyhow::{Context, Result};
use botster_gate_kit::bounded::{clean_up, exit_failures, remove_stale, run_bounded};
use botster_gate_kit::junit::{junit_path, parse_junit};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// A default-tier test over this fails (nextest terminates it; the JUnit check reports it too).
pub const TEST_LIMIT_SECS: f64 = 2.0;
/// The default tier as a whole must finish within this.
pub const TIER_LIMIT: Duration = Duration::from_secs(60);
/// The slow tier's deadline.
pub const SLOW_DEADLINE: Duration = Duration::from_secs(600);

/// The tier that the arguments name: `false` the default tier, `true` the slow tier.
///
/// # Errors
/// An argument other than `--slow`.
pub fn parse_tier(args: &[String]) -> Result<bool, String> {
    match args {
        [] => Ok(false),
        [slow] if slow == "--slow" => Ok(true),
        _ => Err(format!(
            "usage: cargo xtask test-budget [--slow] (got {args:?})"
        )),
    }
}

/// The members that declare a `slow` feature, from the JSON of `cargo metadata --no-deps`.
///
/// # Errors
/// The text is not cargo metadata.
pub fn slow_members(json: &str) -> Result<Vec<String>, String> {
    let doc: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("cargo metadata is not JSON: {e}"))?;
    let packages = doc["packages"]
        .as_array()
        .ok_or("cargo metadata has no packages")?;
    let mut members: Vec<String> = packages
        .iter()
        .filter(|package| package["features"].get("slow").is_some())
        .filter_map(|package| package["name"].as_str().map(str::to_string))
        .collect();
    members.sort();
    Ok(members)
}

/// The tests over `limit` seconds, slowest first, as report lines.
pub fn over_limit(times: &BTreeMap<String, f64>, limit: f64) -> Vec<String> {
    let mut over: Vec<(&String, f64)> = times
        .iter()
        .filter(|(_, secs)| **secs > limit)
        .map(|(name, secs)| (name, *secs))
        .collect();
    over.sort_by(|a, b| b.1.total_cmp(&a.1));
    over.into_iter()
        .map(|(name, secs)| format!("{secs:.3} s over the {limit} s limit: {name}"))
        .collect()
}

/// The `count` slowest tests, slowest first, as report lines.
pub fn slowest(times: &BTreeMap<String, f64>, count: usize) -> Vec<String> {
    let mut rows: Vec<(&String, f64)> = times.iter().map(|(name, secs)| (name, *secs)).collect();
    rows.sort_by(|a, b| b.1.total_cmp(&a.1));
    rows.into_iter()
        .take(count)
        .map(|(name, secs)| format!("{secs:.3} s {name}"))
        .collect()
}

/// The nextest arguments of a tier: the profile, and for the slow tier the slow features.
pub fn nextest_args(slow: bool, members: &[String]) -> Vec<String> {
    let mut args: Vec<String> = [
        "nextest",
        "run",
        "--workspace",
        "--locked",
        "--no-tests=pass",
    ]
    .map(String::from)
    .to_vec();
    args.push("--profile".into());
    if slow {
        args.push("slow".into());
        args.push("--features".into());
        args.push(
            members
                .iter()
                .map(|m| format!("{m}/slow"))
                .collect::<Vec<_>>()
                .join(","),
        );
    } else {
        args.push("ci".into());
    }
    args
}

/// The times of the tier's JUnit file (`read`: its text, or why it could not be read). A run that passed its deadline may
/// have written none: its times are empty, and the deadline is its failure.
///
/// # Errors
/// The run ended by itself and wrote no readable JUnit file.
pub fn junit_times(
    read: Result<String, String>,
    timed_out: bool,
) -> Result<BTreeMap<String, f64>, String> {
    match read {
        Ok(xml) => Ok(parse_junit(&xml)),
        Err(_) if timed_out => Ok(BTreeMap::new()),
        Err(error) => Err(error),
    }
}

/// The failures of a tier's run, in order: what it left behind (`botster_gate_kit::bounded::clean_up`), how nextest ended
/// (`exit_failures`), a JUnit file that could not be read, and for the default tier each test over the limit.
pub fn tier_failures(
    slow: bool,
    leftover: Option<String>,
    exit: Vec<String>,
    times: &Result<BTreeMap<String, f64>, String>,
) -> Vec<String> {
    let mut failures: Vec<String> = leftover.into_iter().chain(exit).collect();
    match times {
        Err(error) => failures.push(error.clone()),
        Ok(times) if !slow => failures.extend(over_limit(times, TEST_LIMIT_SECS)),
        Ok(_) => {}
    }
    failures
}

/// The tier's run: build the tests, then nextest under the tier's deadline with `botster_gate_kit::bounded` (its own
/// process group, the process tracker, the kill at the deadline, and the report and kill of every process that it left),
/// then the JUnit report.
///
/// # Errors
/// The build fails, the run cannot start, or a failure of `tier_failures`.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    let slow = parse_tier(args).map_err(anyhow::Error::msg)?;
    let out = Command::new("cargo")
        .current_dir(root)
        .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
        .output()
        .context("run cargo metadata")?;
    succeeded(
        "cargo metadata",
        out.status.success(),
        &String::from_utf8_lossy(&out.stderr),
    )
    .map_err(anyhow::Error::msg)?;
    let metadata = String::from_utf8_lossy(&out.stdout);
    let members = slow_members(&metadata).map_err(anyhow::Error::msg)?;
    let Some((profile, deadline)) = tier(slow, &members) else {
        println!("test-budget: slow tier: no member has a `slow` feature, so the tier has no test");
        return Ok(());
    };
    // The build is not part of the tier's time: build first, then start the clock.
    let built = Command::new("cargo")
        .current_dir(root)
        .args(nextest_args(slow, &members))
        .arg("--no-run")
        .status()
        .context("build the tests")?;
    succeeded("the test build", built.success(), &built.to_string()).map_err(anyhow::Error::msg)?;
    // nextest's store is `target/nextest` under the workspace root, whatever the cargo target directory is.
    let junit = junit_path(&root.join("target"), profile);
    // A report of an earlier run must never count for this one.
    remove_stale(&junit)?;
    let pidfile = junit.with_file_name("test-pids");
    remove_stale(&pidfile)?;
    std::fs::create_dir_all(junit.parent().unwrap_or(root))
        .with_context(|| format!("create the directory of {}", junit.display()))?;
    let mut nextest = Command::new("cargo");
    nextest
        .current_dir(root)
        .args(nextest_args(slow, &members))
        .env("BOTSTER_TEST_PIDFILE", &pidfile);
    let run = run_bounded(nextest, deadline, &pidfile, None, "start cargo nextest")?;
    // First, before any step that can fail: report and kill the leftovers.
    let leftover = clean_up(&run);
    let times = junit_times(
        std::fs::read_to_string(&junit)
            .map_err(|e| format!("no JUnit file at {}: {e}", junit.display())),
        run.timed_out,
    );
    if let Ok(times) = &times {
        println!(
            "test-budget: {profile}: {} tests in {:.1} s; the slowest:",
            times.len(),
            run.wall.as_secs_f64()
        );
        for line in slowest(times, 10) {
            println!("  {line}");
        }
    }
    let exit = exit_failures(run.timed_out, &run.status, profile, deadline);
    let failures = tier_failures(slow, leftover, exit, &times);
    for failure in &failures {
        eprintln!("test-budget: FAIL {failure}");
    }
    succeeded(
        &format!("the {profile} tier"),
        failures.is_empty(),
        &format!("{} budget failure(s)", failures.len()),
    )
    .map_err(anyhow::Error::msg)
}

/// The profile and the deadline of a tier, or `None` for a slow tier with no member that has a `slow` feature.
pub fn tier(slow: bool, members: &[String]) -> Option<(&'static str, Duration)> {
    match (slow, members.is_empty()) {
        (false, _) => Some(("ci", TIER_LIMIT)),
        (true, false) => Some(("slow", SLOW_DEADLINE)),
        (true, true) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn times(rows: &[(&str, f64)]) -> BTreeMap<String, f64> {
        rows.iter().map(|(n, s)| (n.to_string(), *s)).collect()
    }

    #[test]
    fn the_tier_is_default_or_slow() {
        assert_eq!(parse_tier(&[]), Ok(false));
        assert_eq!(parse_tier(&["--slow".to_string()]), Ok(true));
        for bad in [vec!["--fast"], vec!["--slow", "--slow"]] {
            let args: Vec<String> = bad.iter().map(|s| s.to_string()).collect();
            assert!(
                parse_tier(&args).unwrap_err().starts_with("usage: "),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_slow_members_are_those_with_a_slow_feature() {
        let json = r#"{"packages": [
            {"name": "b", "features": {"slow": []}},
            {"name": "c", "features": {"other": []}},
            {"name": "a", "features": {"slow": [], "x": []}}
        ]}"#;
        assert_eq!(
            slow_members(json),
            Ok(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(slow_members(r#"{"packages": []}"#), Ok(vec![]));
        assert_eq!(
            slow_members("{}"),
            Err("cargo metadata has no packages".to_string())
        );
        assert!(slow_members("x")
            .unwrap_err()
            .starts_with("cargo metadata is not JSON"));
    }

    #[test]
    fn only_a_test_over_the_limit_is_reported_slowest_first() {
        let t = times(&[("a", 1.0), ("b", 2.0), ("c", 2.5), ("d", 3.25)]);
        assert_eq!(
            over_limit(&t, 2.0),
            [
                "3.250 s over the 2 s limit: d".to_string(),
                "2.500 s over the 2 s limit: c".to_string(),
            ]
        );
        assert!(over_limit(&t, 5.0).is_empty());
    }

    #[test]
    fn the_slowest_come_first_and_the_count_bounds_them() {
        let t = times(&[("a", 0.5), ("b", 0.25), ("c", 1.5)]);
        assert_eq!(
            slowest(&t, 2),
            ["1.500 s c".to_string(), "0.500 s a".to_string()]
        );
        assert_eq!(slowest(&t, 5).len(), 3);
    }

    #[test]
    fn the_default_tier_has_60_s_and_the_slow_tier_600_s_or_no_test() {
        let members = ["a".to_string()];
        assert_eq!(tier(false, &[]), Some(("ci", Duration::from_secs(60))));
        assert_eq!(tier(false, &members), Some(("ci", Duration::from_secs(60))));
        assert_eq!(
            tier(true, &members),
            Some(("slow", Duration::from_secs(600)))
        );
        assert_eq!(tier(true, &[]), None);
    }

    #[test]
    fn a_missing_junit_file_fails_unless_the_run_passed_its_deadline() {
        let xml = r#"<testsuites><testcase name="t::a" classname="x" time="0.5"/></testsuites>"#;
        assert_eq!(junit_times(Ok(xml.into()), false), Ok(parse_junit(xml)));
        assert_eq!(junit_times(Ok(xml.into()), false).unwrap().len(), 1);
        assert_eq!(
            junit_times(Err("no JUnit".into()), true),
            Ok(BTreeMap::new())
        );
        assert_eq!(
            junit_times(Err("no JUnit".into()), false),
            Err("no JUnit".to_string())
        );
    }

    /// H212-I3: a leftover and the way nextest ended fail the tier, before the per-test limits; the slow tier has no
    /// per-test limit.
    #[test]
    fn a_leftover_the_exit_a_missing_report_and_a_slow_test_each_fail_the_tier() {
        let t = Ok(times(&[("a", 1.0), ("b", 2.5)]));
        assert!(tier_failures(false, None, vec![], &Ok(times(&[("a", 1.0)]))).is_empty());
        assert_eq!(
            tier_failures(
                false,
                Some("left: pid 9".into()),
                vec!["nextest failed".into()],
                &t
            ),
            [
                "left: pid 9".to_string(),
                "nextest failed".to_string(),
                "2.500 s over the 2 s limit: b".to_string()
            ]
        );
        assert!(tier_failures(true, None, vec![], &t).is_empty());
        assert_eq!(
            tier_failures(true, None, vec![], &Err("no JUnit file".into())),
            ["no JUnit file"]
        );
    }

    #[test]
    fn each_tier_has_its_profile_and_the_slow_tier_its_features() {
        assert_eq!(
            nextest_args(false, &[]).join(" "),
            "nextest run --workspace --locked --no-tests=pass --profile ci"
        );
        assert_eq!(
            nextest_args(true, &["a".to_string(), "b".to_string()]).join(" "),
            "nextest run --workspace --locked --no-tests=pass --profile slow --features a/slow,b/slow"
        );
    }
}
