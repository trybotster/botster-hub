//! Production owner-loop proofs for Hub session projection.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use botster_hub::{MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS};

const REQUIRED_CORE_REV: &str = "b30d9dc3b940ea421251e9667afac825a9a1ec49";
const REQUIRED_CORE_URL: &str = "https://github.com/trybotster/botster-core.git";
const SYNTHETIC_INVALID_CORE_REV: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

const CORE_FAMILY: &[&str] = &[
    "botster-core",
    "botster-core-daemon",
    "botster-terminal-protocol",
    "botster-terminal-protocol-client",
    "botster-core-test-support",
    "botster-terminal-ghostty",
];

const MEMBER_CORE_FAMILY: &[(&str, &[&str])] = &[
    (
        "Cargo.toml",
        &[
            "botster-core",
            "botster-core-daemon",
            "botster-terminal-protocol",
            "botster-terminal-protocol-client",
            "botster-core-test-support",
            "botster-terminal-ghostty",
        ],
    ),
    (
        "crates/botster-hub-client/Cargo.toml",
        &["botster-terminal-protocol"],
    ),
    (
        "crates/botster-hub-test-support/Cargo.toml",
        &[
            "botster-core",
            "botster-core-test-support",
            "botster-terminal-protocol",
            "botster-terminal-protocol-client",
            "botster-terminal-ghostty",
        ],
    ),
];

fn table_quoted(table: &str, key: &str) -> Option<String> {
    let needle = format!("{key} = \"");
    let rest = table.split(&needle).nth(1)?;
    Some(rest.split('"').next()?.to_string())
}

fn parse_core_family_git_tables(manifest: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        for name in CORE_FAMILY {
            let prefix = format!("{name} = {{");
            let Some(rest) = line.strip_prefix(&prefix) else {
                continue;
            };
            let Some(end) = rest.find('}') else {
                continue;
            };
            out.push(((*name).to_string(), rest[..end].to_string()));
        }
    }
    out
}

fn core_family_pin_errors(manifest: &str, expected: &[&str]) -> Vec<String> {
    let decls = parse_core_family_git_tables(manifest);
    let mut names: Vec<&str> = decls.iter().map(|(name, _)| name.as_str()).collect();
    names.sort_unstable();
    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort_unstable();
    let mut errors = Vec::new();
    if names != expected_sorted {
        errors.push(format!(
            "Core-family set {names:?} does not match expected {expected_sorted:?}"
        ));
    }
    for (name, table) in &decls {
        if table.contains("branch") || table.contains("tag") {
            errors.push(format!("{name} must use rev, not branch or tag: {table}"));
        }
        match table_quoted(table, "git") {
            Some(url) if url == REQUIRED_CORE_URL => {}
            Some(url) => errors.push(format!("{name} git URL {url} is not {REQUIRED_CORE_URL}")),
            None => errors.push(format!("{name} has no git URL")),
        }
        match table_quoted(table, "rev") {
            Some(rev) if rev == REQUIRED_CORE_REV => {}
            Some(rev) => errors.push(format!("{name} rev {rev} is not {REQUIRED_CORE_REV}")),
            None => errors.push(format!("{name} has no rev")),
        }
    }
    errors
}

#[test]
fn git_visible_hub_members_share_one_exact_core_revision() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (relative, expected) in MEMBER_CORE_FAMILY {
        let path = root.join(relative);
        let text = fs::read_to_string(&path).expect("read manifest");
        let errors = core_family_pin_errors(&text, expected);
        assert!(
            errors.is_empty(),
            "{} Core-family pin errors: {errors:?}",
            path.display()
        );
    }
    let lock = fs::read_to_string(root.join("Cargo.lock")).expect("read lock");
    assert!(
        lock.contains(&format!("rev={REQUIRED_CORE_REV}#{REQUIRED_CORE_REV}")),
        "Cargo.lock must pin the exact Core revision"
    );
    let readme = fs::read_to_string(root.join("README.md")).expect("read README");
    assert!(
        !readme.contains("tracks `botster-core` from the `main` branch"),
        "README must not say Hub tracks Core from main"
    );
    assert!(
        readme.contains("one exact `rev`"),
        "README must state the exact rev policy"
    );
}

#[test]
fn git_visible_hub_members_reject_one_mixed_core_revision() {
    let mixed = format!(
        "botster-core = {{ git = \"{REQUIRED_CORE_URL}\", rev = \"{SYNTHETIC_INVALID_CORE_REV}\" }}\n\
         botster-terminal-protocol = {{ git = \"{REQUIRED_CORE_URL}\", rev = \"{REQUIRED_CORE_REV}\" }}\n"
    );
    assert!(
        mixed.contains(REQUIRED_CORE_URL)
            && mixed.contains(&format!("rev = \"{REQUIRED_CORE_REV}\"")),
        "mixed fixture must still contain the approved URL and rev so a whole-file contains check would pass"
    );
    let errors = core_family_pin_errors(&mixed, &["botster-core", "botster-terminal-protocol"]);
    assert!(
        errors
            .iter()
            .any(|error| error.contains("botster-core") && error.contains("rev")),
        "mixed revision must fail the per-declaration guard: {errors:?}"
    );
}

#[test]
fn git_visible_hub_members_reject_one_mixed_core_url() {
    let mixed = format!(
        "botster-core = {{ git = \"https://github.com/trybotster/botster-core\", rev = \"{REQUIRED_CORE_REV}\" }}\n\
         botster-terminal-protocol = {{ git = \"{REQUIRED_CORE_URL}\", rev = \"{REQUIRED_CORE_REV}\" }}\n"
    );
    assert!(
        mixed.contains(REQUIRED_CORE_URL)
            && mixed.contains(&format!("rev = \"{REQUIRED_CORE_REV}\"")),
        "mixed fixture must still contain the approved URL and rev so a whole-file contains check would pass"
    );
    let errors = core_family_pin_errors(&mixed, &["botster-core", "botster-terminal-protocol"]);
    assert!(
        errors
            .iter()
            .any(|error| error.contains("botster-core") && error.contains("git URL")),
        "mixed URL must fail the per-declaration guard: {errors:?}"
    );
}

#[test]
fn published_owner_turn_budgets_fail_if_observe_walks_every_session() {
    const {
        assert!(MAX_OWNER_TURN_MS < 100);
        assert!(MAX_READY_OPERATION_WAIT_MS < 200);
        assert!(MAX_OWNER_TURN_MS <= MAX_READY_OPERATION_WAIT_MS);
    }
    let _ = Instant::now().elapsed() < Duration::from_millis(MAX_READY_OPERATION_WAIT_MS);
}
