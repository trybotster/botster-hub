//! The `--e2e` mode against a real daemon process. It needs the candidate
//! Hub binaries that the Hub's own gate builds: `BOTSTER_HUB_BIN`,
//! `BOTSTER_SESSION_WORKER_BIN`, and `BOTSTER_CANDIDATE_MANIFEST`
//! (`./test.sh` sets them).

use std::path::PathBuf;

use botster_plugin_test_kit::e2e::run_conformance;
use botster_plugin_test_kit::spec::{Mode, run_spec_file_in};

fn crate_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn the_e2e_specs_pass_against_a_real_daemon() {
    let outcomes = run_spec_file_in(
        Mode::E2e,
        &crate_path("fixtures"),
        &crate_path("specs/e2e_spec.lua"),
    );
    assert_eq!(outcomes.len(), 4, "{outcomes:?}");
    let failures: Vec<_> = outcomes
        .iter()
        .filter(|outcome| outcome.failure.is_some())
        .collect();
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_plugin_contract_matrix_conformance_passes_on_a_real_daemon() {
    let report = run_conformance().expect("conformance passes");
    assert!(
        report.contains("botster.plugin-contract-matrix"),
        "{report}"
    );
}
