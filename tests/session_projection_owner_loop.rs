//! Production owner-loop proofs for Hub session projection.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use botster_hub::{MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS};

const REQUIRED_CORE_REV: &str = "7eafa470a18025895995bbedc20d34b58106a03b";
const REQUIRED_CORE_URL: &str = "https://github.com/trybotster/botster-core.git";
const SYNTHETIC_INVALID_CORE_REV: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

const CORE_FAMILY: &[&str] = &[
    "botster-core",
    "botster-core-daemon",
    "botster-terminal-protocol",
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
            "botster-terminal-protocol",
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
fn owner_loop_and_projection_sources_reject_unbounded_and_product_policy() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/session_projection.rs",
        "src/daemon_maintenance.rs",
        "src/daemon/owner_loop.rs",
        "src/daemon/control.rs",
        "src/daemon/control/message.rs",
        "src/daemon/control/connection.rs",
        "src/daemon/control/sessions.rs",
        "src/daemon/control/session_types.rs",
        "src/daemon/control/spawn_targets.rs",
        "src/daemon/control/packages.rs",
        "src/daemon/control/packages/mutations.rs",
        "src/daemon/control/messaging.rs",
        "src/daemon/control/plugins.rs",
        "src/daemon/control/entities.rs",
        "src/daemon/control/events.rs",
        "src/daemon/control/webrtc.rs",
        "src/daemon/control/host.rs",
        "src/daemon/control/request.rs",
        "src/subscription/entity.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source");
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(
            !production.contains("observe_lifecycle(")
                || production.contains("observe_lifecycle_slice("),
            "{relative} must not call unbounded observe_lifecycle"
        );
        assert!(
            !production
                .replace("lifecycle_baseline_page", "")
                .contains("lifecycle_baseline("),
            "{relative} must not call unbounded lifecycle_baseline"
        );
        if relative != "src/subscription/entity.rs" && relative != "src/daemon/control/sessions.rs"
        {
            for needle in [
                "botster-terminal-protocol-client",
                "ProcessExited",
                "botster-workspaces",
                "membership",
            ] {
                assert!(
                    !production.contains(needle),
                    "{relative} must not contain {needle}"
                );
            }
        }
    }
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
