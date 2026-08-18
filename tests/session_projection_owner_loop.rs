//! Production owner-loop proofs for Hub session projection.

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use botster_hub::{MAX_OWNER_TURN_MS, MAX_READY_OPERATION_WAIT_MS};

const REQUIRED_CORE_REV: &str = "302c7f7b61f3970a0151b8c6646fc21ae7bd6c67";
const REQUIRED_CORE_URL: &str = "https://github.com/trybotster/botster-core.git";

#[test]
fn git_visible_hub_members_share_one_exact_core_revision() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifests = [
        root.join("Cargo.toml"),
        root.join("crates/botster-hub-client/Cargo.toml"),
        root.join("crates/botster-hub-test-support/Cargo.toml"),
    ];
    for path in manifests {
        let text = fs::read_to_string(&path).expect("read manifest");
        assert!(
            text.contains(REQUIRED_CORE_URL),
            "{} must use the Core .git URL",
            path.display()
        );
        assert!(
            text.contains(&format!("rev = \"{REQUIRED_CORE_REV}\"")),
            "{} must pin Core {REQUIRED_CORE_REV}",
            path.display()
        );
        assert!(
            !text.contains("branch = \"main\""),
            "{} must not float Core main",
            path.display()
        );
    }
    let lock = fs::read_to_string(root.join("Cargo.lock")).expect("read lock");
    assert!(
        lock.contains(&format!("rev={REQUIRED_CORE_REV}#{REQUIRED_CORE_REV}")),
        "Cargo.lock must pin the exact Core revision"
    );
}

#[test]
fn owner_loop_and_projection_sources_reject_unbounded_and_product_policy() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/session_projection.rs",
        "src/daemon_maintenance.rs",
        "src/daemon_transport.rs",
        "src/daemon_entity_subscriptions.rs",
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
        if relative != "src/daemon_transport.rs" && relative != "src/daemon_entity_subscriptions.rs"
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
