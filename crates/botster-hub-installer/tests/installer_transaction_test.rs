//! The installation transaction, proven through the real installer binary.
//!
//! These run the actual `botster-hub-installer` executable against a loopback
//! origin and synthetic revision-coupled artifacts. Installer *mechanics* —
//! replace, verify, roll back, crash — are what synthetic artifacts can prove;
//! receipt/binary agreement against a *real* Hub is proven separately in
//! `tests/hub_daemon_lifecycle_test.rs`. Both halves are needed and neither
//! alone is sufficient.

mod support;

use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

use support::{Harness, Release, Route, output_text};

fn install_ok(harness: &Harness, release: &Release) {
    let source = harness.publish(release);
    let output = harness.install(&source);
    assert!(output.status.success(), "{}", output_text(&output));
}

#[test]
fn a_fresh_install_publishes_one_coupled_generation_and_a_safe_receipt() {
    let harness = Harness::new("fresh");
    let release = Release::new("0.2.0", 'a', 'b');
    install_ok(&harness, &release);

    let generation = release.generation();
    let directory = harness.prefix.join("generations").join(&generation);
    assert!(directory.join("botster-hub").exists());
    assert!(directory.join("botster-session-worker").exists());
    assert_eq!(
        harness.current_target().as_deref(),
        Some(format!("generations/{generation}").as_str())
    );
    assert_eq!(
        std::fs::read_link(harness.entrypoint()).expect("entrypoint symlink"),
        std::path::Path::new("../current/botster-hub"),
        "bin/botster-hub resolves through current, so PATH never changes across upgrades"
    );

    let receipt = harness.receipt().expect("receipt written");
    assert_eq!(receipt["schema_version"], 2);
    assert_eq!(receipt["installation_mode"], "managed");
    assert_eq!(receipt["binary_version"], release.version);
    assert_eq!(receipt["build_revision"], release.hub_revision);
    assert_eq!(
        receipt["source_revisions"]["botster_hub"],
        release.hub_revision
    );
    assert_eq!(
        receipt["source_revisions"]["botster_core"],
        release.core_revision
    );
    assert_ne!(
        receipt["source_revisions"]["botster_hub"], receipt["source_revisions"]["botster_core"],
        "Hub and locked-Core provenance are recorded separately"
    );
    assert_eq!(receipt["signature"]["algorithm"], "ed25519");
    assert_eq!(receipt["installer"]["id"], "botster-hub-installer");

    let receipt_metadata = std::fs::metadata(harness.receipt_path()).expect("receipt metadata");
    assert_eq!(receipt_metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(receipt_metadata.uid(), unsafe { libc::geteuid() });
    let installations = std::fs::metadata(harness.home.join(".botster/installations"))
        .expect("installations metadata");
    assert_eq!(installations.permissions().mode() & 0o777, 0o700);
    assert_eq!(installations.uid(), unsafe { libc::geteuid() });
}

#[test]
fn an_upgrade_moves_the_pointer_and_retains_the_previous_generation() {
    let harness = Harness::new("upgrade");
    let first = Release::new("0.2.0", 'a', 'b');
    let second = Release::new("0.3.0", 'c', 'd');
    install_ok(&harness, &first);
    install_ok(&harness, &second);

    assert_eq!(
        harness.current_target().as_deref(),
        Some(format!("generations/{}", second.generation()).as_str())
    );
    let generations = harness.generations();
    assert!(
        generations.contains(&first.generation()) && generations.contains(&second.generation()),
        "the previous generation is retained so rollback stays a pointer reversal: {generations:?}"
    );
    assert_eq!(
        harness.receipt().expect("receipt")["binary_version"],
        second.version
    );
    assert_eq!(
        harness.live_pair(),
        Some((second.version.clone(), second.version.clone()))
    );
}

/// The defect class the generation design exists to eliminate.
///
/// Crash injection immediately before and immediately after the switch, plus
/// during the artifact writes, must never leave the Hub and worker reachable
/// through `current` coming from different releases.
#[test]
fn a_mixed_hub_and_worker_pair_is_unreachable_at_every_crash_boundary() {
    for injection in [
        "abort:artifact_write",
        "abort:before_staging_rename",
        "abort:before_switch",
        "abort:after_switch",
        "abort:before_receipt",
    ] {
        let harness = Harness::new(&format!("mixed-{}", injection.replace(':', "-")));
        let first = Release::new("0.2.0", 'a', 'b');
        let second = Release::new("0.3.0", 'c', 'd');
        install_ok(&harness, &first);

        let source = harness.publish(&second);
        let crashed = harness.install_with_injection(&source, Some(injection));
        assert!(
            !crashed.status.success(),
            "injection {injection} must not report success: {}",
            output_text(&crashed)
        );

        let (hub_version, worker_version) = harness
            .live_pair()
            .unwrap_or_else(|| panic!("a coherent pair must remain reachable after {injection}"));
        assert_eq!(
            hub_version, worker_version,
            "injection {injection} produced a mixed pair"
        );
        assert!(
            hub_version == first.version || hub_version == second.version,
            "injection {injection} produced an unknown pair {hub_version}"
        );

        // The ordering invariant: the receipt is written last, so it can only
        // ever be *behind* the live generation, never ahead of it. A receipt
        // naming the new release therefore proves the pointer already moved.
        //
        // A receipt still naming the old release beside a new generation is the
        // expected `after switch, before receipt` state, and it degrades
        // honestly: `binary_version` disagrees with the running binary, so the
        // Hub diagnoses `receipt_binary_mismatch` and reports **unmanaged**
        // rather than falsely claiming managed.
        let receipt_version = harness
            .receipt()
            .map(|receipt| receipt["binary_version"].as_str().unwrap_or("").to_string());
        if let Some(receipt_version) = receipt_version {
            assert!(
                receipt_version == first.version || receipt_version == second.version,
                "injection {injection} left an unknown receipt version {receipt_version}"
            );
            if receipt_version == second.version {
                assert_eq!(
                    hub_version, second.version,
                    "injection {injection} placed a new receipt beside an old generation"
                );
            }
        }

        // A re-run converges to a correct managed installation.
        let recovered = harness.install(&source);
        assert!(recovered.status.success(), "{}", output_text(&recovered));
        assert_eq!(
            harness.live_pair(),
            Some((second.version.clone(), second.version.clone())),
            "a re-run after {injection} must converge"
        );
        assert_eq!(
            harness.receipt().expect("receipt")["binary_version"],
            second.version
        );
    }
}

/// A returned error after the switch reverses the pointer with the same single
/// operation and leaves the previous receipt byte-identical.
#[test]
fn a_recoverable_error_after_the_switch_reverses_the_pointer_and_keeps_the_old_receipt() {
    for injection in ["fail:after_switch", "fail:post_switch_verify"] {
        let harness = Harness::new(&format!("rollback-{}", injection.replace(':', "-")));
        let first = Release::new("0.2.0", 'a', 'b');
        let second = Release::new("0.3.0", 'c', 'd');
        install_ok(&harness, &first);
        let previous_receipt = std::fs::read(harness.receipt_path()).expect("previous receipt");

        let source = harness.publish(&second);
        let failed = harness.install_with_injection(&source, Some(injection));
        assert!(!failed.status.success(), "{}", output_text(&failed));

        assert_eq!(
            harness.current_target().as_deref(),
            Some(format!("generations/{}", first.generation()).as_str()),
            "{injection} must reverse the pointer to the retained previous generation"
        );
        assert_eq!(
            harness.live_pair(),
            Some((first.version.clone(), first.version.clone())),
            "the pair is coherent on the previous generation after {injection}"
        );
        assert_eq!(
            std::fs::read(harness.receipt_path()).expect("receipt after rollback"),
            previous_receipt,
            "{injection} leaves the previous receipt byte-identical"
        );
    }
}

/// A first install has no previous generation to reverse to, so a recoverable
/// error removes what this run created and leaves **no receipt** — nothing can
/// falsely report managed.
#[test]
fn a_first_install_failure_leaves_no_installation_and_no_receipt() {
    for injection in [
        "fail:after_current",
        "fail:after_bin",
        "fail:post_switch_verify",
    ] {
        let harness = Harness::new(&format!("bootstrap-{}", injection.replace(':', "-")));
        let release = Release::new("0.2.0", 'a', 'b');
        let source = harness.publish(&release);

        let failed = harness.install_with_injection(&source, Some(injection));
        assert!(!failed.status.success(), "{}", output_text(&failed));
        assert!(
            !harness.receipt_path().exists(),
            "{injection} must leave no receipt"
        );
        assert!(
            std::fs::symlink_metadata(harness.prefix.join("current")).is_err(),
            "{injection} must remove the pointer this run created"
        );
        assert!(
            std::fs::symlink_metadata(harness.entrypoint()).is_err(),
            "{injection} must remove the entrypoint this run created"
        );

        let recovered = harness.install(&source);
        assert!(recovered.status.success(), "{}", output_text(&recovered));
        assert_eq!(
            harness.receipt().expect("receipt")["binary_version"],
            release.version
        );
    }
}

/// Abrupt termination during bootstrap leaves at worst binaries plus a dangling
/// or absent pointer and no receipt — honest either way — and a re-run converges.
#[test]
fn abrupt_bootstrap_termination_never_produces_a_falsely_managed_state() {
    for injection in [
        "abort:after_current",
        "abort:after_bin",
        "abort:receipt_write",
    ] {
        let harness = Harness::new(&format!("abrupt-{}", injection.replace(':', "-")));
        let release = Release::new("0.2.0", 'a', 'b');
        let source = harness.publish(&release);

        let crashed = harness.install_with_injection(&source, Some(injection));
        assert!(!crashed.status.success(), "{}", output_text(&crashed));
        assert!(
            !harness.receipt_path().exists(),
            "{injection} must never leave a receipt"
        );

        let recovered = harness.install(&source);
        assert!(recovered.status.success(), "{}", output_text(&recovered));
        assert_eq!(
            harness.receipt().expect("receipt")["binary_version"],
            release.version
        );
        assert_eq!(
            harness.live_pair(),
            Some((release.version.clone(), release.version.clone()))
        );
    }
}

/// A crash mid-download can only ever leave a partial *staging* directory. The
/// final generation name is complete by construction, because it only ever
/// appears as the result of a directory rename.
#[test]
fn a_crash_during_artifact_writes_leaves_only_a_staging_directory() {
    let harness = Harness::new("staging");
    let release = Release::new("0.2.0", 'a', 'b');
    let source = harness.publish(&release);

    let crashed = harness.install_with_injection(&source, Some("abort:artifact_write"));
    assert!(!crashed.status.success(), "{}", output_text(&crashed));

    let generations = harness.generations();
    assert!(
        generations.iter().all(|name| name.starts_with(".staging-")),
        "the final generation name never appears partial: {generations:?}"
    );
    assert!(
        !generations.is_empty(),
        "the crash is expected to leave its unreferenced staging directory behind"
    );
    assert!(harness.current_target().is_none(), "nothing points at it");

    let recovered = harness.install(&source);
    assert!(recovered.status.success(), "{}", output_text(&recovered));
    assert_eq!(
        harness.generations(),
        vec![release.generation()],
        "the stale staging directory is swept on the next run"
    );
}

/// The stale-staging sweep is fail-safe: it removes only installer-owned
/// directories matching its own pattern and leaves anything else alone.
#[test]
fn the_stale_staging_sweep_leaves_foreign_entries_in_place() {
    let harness = Harness::new("staging-sweep");
    let release = Release::new("0.2.0", 'a', 'b');
    install_ok(&harness, &release);

    let generations = harness.prefix.join("generations");
    let foreign = generations.join("not-a-staging-directory");
    std::fs::create_dir(&foreign).expect("create foreign directory");
    let symlinked = generations.join(".staging-symlink");
    symlink(&foreign, &symlinked).expect("symlink a staging name");

    let second = Release::new("0.3.0", 'c', 'd');
    install_ok(&harness, &second);

    assert!(foreign.exists(), "a non-matching name is never considered");
    assert!(
        std::fs::symlink_metadata(&symlinked).is_ok(),
        "the sweep never follows or removes a symlink at its own name pattern"
    );
}

/// An existing final generation is reused only on an exact match, and never
/// deleted or overwritten on a mismatch.
#[test]
fn an_existing_generation_is_reused_on_an_exact_match_and_aborts_otherwise() {
    let harness = Harness::new("reuse");
    let release = Release::new("0.2.0", 'a', 'b');
    let source = harness.publish(&release);
    install_ok(&harness, &release);

    // Exact match: a re-run reuses it rather than restaging.
    let reused = harness.install(&source);
    assert!(reused.status.success(), "{}", output_text(&reused));
    assert!(
        String::from_utf8_lossy(&reused.stdout).contains("reused_generation=true"),
        "{}",
        output_text(&reused)
    );

    // Mismatch: abort, and leave the divergent artifact byte-unchanged.
    let worker = harness
        .prefix
        .join("generations")
        .join(release.generation())
        .join("botster-session-worker");
    std::fs::write(&worker, b"tampered worker bytes\n").expect("tamper with the worker");
    let aborted = harness.install(&source);
    assert!(!aborted.status.success(), "{}", output_text(&aborted));
    assert_eq!(
        std::fs::read(&worker).expect("read tampered worker"),
        b"tampered worker bytes\n",
        "the installer neither deletes nor overwrites a generation it cannot prove it produced"
    );
}

/// `bin/botster-hub` has three cases, and the crash-left symlink is the
/// *expected* one.
#[test]
fn the_entrypoint_is_reused_when_canonical_and_fails_closed_otherwise() {
    // A regular file is never clobbered.
    let harness = Harness::new("entrypoint-regular");
    let release = Release::new("0.2.0", 'a', 'b');
    let source = harness.publish(&release);
    std::fs::create_dir_all(harness.prefix.join("bin")).expect("create bin");
    std::fs::write(harness.entrypoint(), b"operator's own script\n").expect("write regular file");
    let aborted = harness.install(&source);
    assert!(!aborted.status.success(), "{}", output_text(&aborted));
    assert_eq!(
        std::fs::read(harness.entrypoint()).expect("read entrypoint"),
        b"operator's own script\n"
    );
    assert!(!harness.receipt_path().exists());

    // A crash-left canonical symlink is reused, so a re-run converges.
    let harness = Harness::new("entrypoint-canonical");
    let source = harness.publish(&release);
    std::fs::create_dir_all(harness.prefix.join("bin")).expect("create bin");
    symlink("../current/botster-hub", harness.entrypoint()).expect("pre-place canonical symlink");
    let converged = harness.install(&source);
    assert!(converged.status.success(), "{}", output_text(&converged));
    assert_eq!(
        harness.receipt().expect("receipt")["binary_version"],
        release.version
    );

    // A symlink pointing outside the managed layout is neither followed nor
    // replaced: post-switch verification can never be made to execute an
    // attacker-chosen binary.
    let harness = Harness::new("entrypoint-foreign");
    let source = harness.publish(&release);
    std::fs::create_dir_all(harness.prefix.join("bin")).expect("create bin");
    let attacker = harness.root.join("attacker-binary");
    std::fs::write(&attacker, b"#!/bin/sh\necho product_id=botster-hub\n").expect("write attacker");
    std::fs::set_permissions(&attacker, std::fs::Permissions::from_mode(0o755))
        .expect("make attacker executable");
    symlink(&attacker, harness.entrypoint()).expect("pre-place foreign symlink");
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("entrypoint_foreign_symlink"),
        "{}",
        output_text(&refused)
    );
    assert_eq!(
        std::fs::read_link(harness.entrypoint()).expect("foreign symlink"),
        attacker,
        "the foreign symlink is left exactly as it was"
    );
    assert!(!harness.receipt_path().exists());
}

/// Four negative tests, one per field duplicated across the signed/unsigned
/// boundary. The `version` case is the attack the equality rule exists to stop:
/// a validly signed *old* manifest advertised as a *new* release.
#[test]
fn every_envelope_field_disagreeing_with_the_verified_manifest_aborts_the_install() {
    for field in ["product_id", "release_channel", "version", "build_revision"] {
        let harness = Harness::new(&format!("envelope-{field}"));
        let release = Release::new("0.2.0", 'a', 'b');
        let manifest = release.manifest(&harness.server);
        let mut document = harness.key.document(&manifest);
        document[field] = serde_json::json!(match field {
            "product_id" => "botster-core",
            "release_channel" => "beta",
            "version" => "99.0.0",
            _ => "9999999999999999999999999999999999999999",
        });

        let source = harness.publish_document(&release, &document);
        let refused = harness.install(&source);
        assert!(!refused.status.success(), "field={field}");
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("release_envelope_disagreement")
                || String::from_utf8_lossy(&refused.stderr).contains("invalid_release_metadata"),
            "field={field}: {}",
            output_text(&refused)
        );
        assert!(harness.prefix_fingerprint().is_empty(), "field={field}");
    }
}

#[test]
fn a_tampered_manifest_a_wrong_key_and_an_absent_signature_each_abort() {
    // Tampered payload: the signature no longer covers the bytes.
    let harness = Harness::new("signature-tampered");
    let release = Release::new("0.2.0", 'a', 'b');
    let manifest = release.manifest(&harness.server);
    let signed_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
    let mut document = harness.key.document_from_bytes(&signed_bytes, &manifest);
    let mut tampered = manifest.clone();
    tampered["source_revisions"]["botster_core"] = serde_json::json!("f".repeat(40));
    document["install_manifest"] = serde_json::json!(base64_encode(
        &serde_json::to_vec(&tampered).expect("serialize tampered manifest")
    ));
    let source = harness.publish_document(&release, &document);
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("release_signature_rejected"),
        "{}",
        output_text(&refused)
    );

    // Wrong key: a valid signature by material the anchor does not name.
    let harness = Harness::new("signature-wrong-key");
    let other = support::SigningKey::generate();
    let manifest = release.manifest(&harness.server);
    let document = other.document(&manifest);
    let source = harness.publish_document(&release, &document);
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("release_signature_rejected"),
        "{}",
        output_text(&refused)
    );

    // Absent signature.
    let harness = Harness::new("signature-absent");
    let manifest = release.manifest(&harness.server);
    let mut document = harness.key.document(&manifest);
    document["signature"]["value"] = serde_json::json!("");
    let source = harness.publish_document(&release, &document);
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("missing_release_signature"),
        "{}",
        output_text(&refused)
    );
}

/// A valid signature does not exempt a value from path-component validation, and
/// rejection happens before *any* filesystem mutation.
#[test]
fn a_validly_signed_but_hostile_source_revision_mutates_nothing() {
    for hostile in ["../escape", "a/b", "", "A".repeat(40).as_str()] {
        let harness = Harness::new("hostile-revision");
        let release = Release::new("0.2.0", 'a', 'b');
        let mut manifest = release.manifest(&harness.server);
        manifest["source_revisions"]["botster_hub"] = serde_json::json!(hostile);
        let document = harness.key.document(&manifest);
        let source = harness.publish_document(&release, &document);

        let refused = harness.install(&source);
        assert!(!refused.status.success(), "hostile={hostile}");
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("malformed_manifest_revision")
                || String::from_utf8_lossy(&refused.stderr)
                    .contains("release_envelope_disagreement"),
            "hostile={hostile}: {}",
            output_text(&refused)
        );
        assert!(
            harness.prefix_fingerprint().is_empty(),
            "hostile={hostile}: the prefix must be byte-identical afterwards"
        );
    }
}

/// Redirects are never followed, for the metadata document or for an artifact:
/// a followed redirect could downgrade to plaintext or cross origins after
/// validation has already passed.
#[test]
fn a_redirect_for_metadata_or_for_an_artifact_is_refused_rather_than_followed() {
    let harness = Harness::new("redirect-metadata");
    let release = Release::new("0.2.0", 'a', 'b');
    let real = harness.publish(&release);
    harness
        .server
        .serve("/redirected.json", Route::redirect(&real));
    let refused = harness.install(&harness.server.url("/redirected.json"));
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("release_redirect_refused"),
        "{}",
        output_text(&refused)
    );

    let harness = Harness::new("redirect-artifact");
    let release = Release::new("0.2.0", 'a', 'b');
    let source = harness.publish(&release);
    harness.server.serve(
        &format!("/{}/botster-session-worker", release.version),
        Route::redirect(&harness.server.url("/elsewhere")),
    );
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("release_redirect_refused"),
        "{}",
        output_text(&refused)
    );
}

/// A non-loopback plaintext coordinate is refused for the artifact as well as
/// for the document, and nothing is installed.
#[test]
fn a_plaintext_non_loopback_artifact_url_is_refused() {
    let harness = Harness::new("insecure-artifact");
    let release = Release::new("0.2.0", 'a', 'b');
    let mut manifest = release.manifest(&harness.server);
    manifest["artifacts"][1]["url"] = serde_json::json!("http://192.0.2.10/botster-session-worker");
    let document = harness.key.document(&manifest);
    let source = harness.publish_document(&release, &document);

    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("insecure_release_source"),
        "{}",
        output_text(&refused)
    );
    assert!(harness.prefix_fingerprint().is_empty());
}

/// A byte-flipped artifact fails checksum verification, installs nothing, and
/// never moves the pointer.
#[test]
fn a_byte_flipped_artifact_fails_verification_and_the_pointer_never_moves() {
    let harness = Harness::new("checksum");
    let first = Release::new("0.2.0", 'a', 'b');
    install_ok(&harness, &first);
    let before = harness.current_target();

    let second = Release::new("0.3.0", 'c', 'd');
    let source = harness.publish(&second);
    let mut flipped = second.worker_bytes.clone();
    flipped[0] ^= 0xff;
    harness.server.serve(
        &format!("/{}/botster-session-worker", second.version),
        Route::ok(flipped),
    );

    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("artifact_checksum_mismatch"),
        "{}",
        output_text(&refused)
    );
    assert_eq!(harness.current_target(), before, "the pointer never moved");
    assert!(
        !harness.generations().contains(&second.generation()),
        "no generation is published for a release that failed verification"
    );
    assert_eq!(
        harness.receipt().expect("receipt")["binary_version"],
        first.version
    );
}

/// The staged Hub binary's self-reported identity must agree with the signed
/// manifest before the pointer moves.
#[test]
fn a_staged_binary_whose_identity_disagrees_with_the_manifest_is_refused() {
    let harness = Harness::new("identity");
    let mut release = Release::new("0.2.0", 'a', 'b');
    release.hub_bytes =
        b"#!/bin/sh\necho product_id=botster-hub\necho version=0.9.9\necho build_revision=deadbeef\n"
            .to_vec();
    let source = harness.publish(&release);

    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("identity_verification_failed"),
        "{}",
        output_text(&refused)
    );
    assert!(
        harness.current_target().is_none(),
        "the pointer never moved"
    );
    assert!(!harness.receipt_path().exists());
}

/// A staged binary that does not answer `version` in `key=value` form is
/// refused rather than guessed at.
#[test]
fn a_staged_binary_with_malformed_identity_output_is_refused() {
    let harness = Harness::new("identity-malformed");
    let mut release = Release::new("0.2.0", 'a', 'b');
    release.hub_bytes = b"#!/bin/sh\necho this is not key value output\n".to_vec();
    let source = harness.publish(&release);

    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("staged_binary_output_malformed"),
        "{}",
        output_text(&refused)
    );
}

/// The installer refuses while the installation lease is held, and proceeds once
/// it is free. Holding the lease from another process is exactly what a managed
/// Hub daemon does at startup.
#[test]
fn the_installer_is_refused_while_the_installation_lease_is_held() {
    use botster_hub_installation::{LeaseMode, LeaseOutcome, lease};

    let harness = Harness::new("lease");
    let release = Release::new("0.2.0", 'a', 'b');
    install_ok(&harness, &release);

    let second = Release::new("0.3.0", 'c', 'd');
    let source = harness.publish(&second);

    let LeaseOutcome::Acquired(held) = lease::acquire(&harness.prefix, LeaseMode::Shared)
        .expect("take a daemon-shaped shared lease")
    else {
        panic!("an uncontended shared lease must be acquired");
    };
    let refused = harness.install(&source);
    assert!(!refused.status.success(), "{}", output_text(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("installation_busy"),
        "{}",
        output_text(&refused)
    );
    assert_eq!(
        harness.current_target().as_deref(),
        Some(format!("generations/{}", release.generation()).as_str()),
        "a refused install never switches generations"
    );

    drop(held);
    let allowed = harness.install(&source);
    assert!(allowed.status.success(), "{}", output_text(&allowed));
}

/// The lease is a **transaction guard**, not a precondition check.
///
/// The natural wrong implementation — acquire, verify no daemon, release, then
/// mutate — reads as correct and reintroduces the whole race. These two
/// boundaries are where that difference shows: a daemon startup and a second
/// installer must both fail closed *inside* the mutation window, and both must
/// succeed once the installation reaches a final state.
#[test]
fn the_lease_is_held_continuously_across_the_mutation_transaction() {
    use botster_hub_installation::{LeaseMode, LeaseOutcome, lease};

    for point in ["before_switch", "after_switch"] {
        let harness = Harness::new(&format!("continuous-{point}"));
        let first = Release::new("0.2.0", 'a', 'b');
        install_ok(&harness, &first);
        let second = Release::new("0.3.0", 'c', 'd');
        let source = harness.publish(&second);

        let (observed, output) = harness.install_while_held(&source, point, || {
            // A managed daemon starting mid-transaction must fail closed.
            let daemon = lease::acquire(&harness.prefix, LeaseMode::Shared)
                .expect("a daemon startup attempt is not an error");
            // A second installer must fail closed rather than interleaving its
            // switch, verification, rollback, or receipt write with the first.
            let second_installer = harness.install(&source);
            (matches!(daemon, LeaseOutcome::Contended), second_installer)
        });

        let (daemon_refused, second_installer) = observed;
        assert!(
            daemon_refused,
            "a daemon must not be able to start while the installer holds the lease at {point}"
        );
        assert!(
            !second_installer.status.success(),
            "a second installer must fail closed at {point}: {}",
            output_text(&second_installer)
        );
        assert!(
            String::from_utf8_lossy(&second_installer.stderr).contains("installation_busy"),
            "{}",
            output_text(&second_installer)
        );
        assert!(output.status.success(), "{}", output_text(&output));

        // The lease is released only once the installation reaches a final
        // state, and a daemon can then start normally.
        assert!(
            matches!(
                lease::acquire(&harness.prefix, LeaseMode::Shared).expect("post-install daemon"),
                LeaseOutcome::Acquired(_)
            ),
            "the lease must be free once the install completes"
        );
        assert_eq!(
            harness.receipt().expect("receipt")["binary_version"],
            second.version
        );
    }
}

/// The same continuity must hold on the failure path: the lease is released
/// only after rollback and cleanup, not at the moment the error is raised.
#[test]
fn the_lease_survives_until_rollback_and_cleanup_have_finished() {
    use botster_hub_installation::{LeaseMode, LeaseOutcome, lease};

    let harness = Harness::new("continuous-failure");
    let first = Release::new("0.2.0", 'a', 'b');
    install_ok(&harness, &first);
    let second = Release::new("0.3.0", 'c', 'd');
    let source = harness.publish(&second);

    let failed = harness.install_with_injection(&source, Some("fail:after_switch"));
    assert!(!failed.status.success(), "{}", output_text(&failed));
    assert!(
        matches!(
            lease::acquire(&harness.prefix, LeaseMode::Shared).expect("post-rollback daemon"),
            LeaseOutcome::Acquired(_)
        ),
        "a daemon can start normally once rollback and cleanup have finished"
    );
    assert_eq!(
        harness.current_target().as_deref(),
        Some(format!("generations/{}", first.generation()).as_str())
    );
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
