//! `cargo xtask test-budget [--slow]`: a test tier under cargo-nextest with its budgets (BUILD.md "Testing: fast and
//! lean"; the values of Core's xtask).
//!
//! The default tier: nextest's `ci` profile terminates a test at 2 s and fails it (`.config/nextest.toml`), and the tier
//! as a whole must finish in 60 s. The slow tier runs the `slow` features of the members that have one, with no per-test
//! limit and a 600 s deadline; with no such member it has no test and passes. The slowest tests are printed from nextest's
//! JUnit file.

use anyhow::{bail, Context, Result};
use botster_gate_kit::junit::parse_junit;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

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

/// The tier's run: nextest under the tier's deadline, then the JUnit report.
///
/// # Errors
/// nextest fails, the tier passes its deadline, or a test passes its limit.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    let slow = parse_tier(args).map_err(anyhow::Error::msg)?;
    let members = if slow {
        let out = Command::new("cargo")
            .current_dir(root)
            .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
            .output()
            .context("run cargo metadata")?;
        if !out.status.success() {
            bail!(
                "cargo metadata failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let members =
            slow_members(&String::from_utf8_lossy(&out.stdout)).map_err(anyhow::Error::msg)?;
        if members.is_empty() {
            println!(
                "test-budget: slow tier: no member has a `slow` feature, so the tier has no test"
            );
            return Ok(());
        }
        members
    } else {
        Vec::new()
    };
    let (profile, deadline) = if slow {
        ("slow", SLOW_DEADLINE)
    } else {
        ("ci", TIER_LIMIT)
    };
    // The build is not part of the tier's time: build first, then start the clock.
    let built = Command::new("cargo")
        .current_dir(root)
        .args(nextest_args(slow, &members))
        .arg("--no-run")
        .status()
        .context("build the tests")?;
    if !built.success() {
        bail!("the tests did not build ({built})");
    }
    let start = Instant::now();
    let mut child = Command::new("cargo")
        .current_dir(root)
        .args(nextest_args(slow, &members))
        .spawn()
        .context("run cargo nextest")?;
    let status = loop {
        if let Some(status) = child.try_wait().context("wait for cargo nextest")? {
            break status;
        }
        if start.elapsed() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "the {profile} tier passed its deadline of {} s",
                deadline.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let junit = root.join(format!("target/nextest/{profile}/junit.xml"));
    let times = parse_junit(
        &std::fs::read_to_string(&junit).with_context(|| format!("read {}", junit.display()))?,
    );
    println!(
        "test-budget: {profile}: {} tests in {:.1} s; the slowest:",
        times.len(),
        start.elapsed().as_secs_f64()
    );
    for line in slowest(&times, 10) {
        println!("  {line}");
    }
    if !status.success() {
        bail!("cargo nextest failed ({status})");
    }
    let over = if slow {
        Vec::new()
    } else {
        over_limit(&times, TEST_LIMIT_SECS)
    };
    if !over.is_empty() {
        bail!("{}", over.join("\n"));
    }
    Ok(())
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
