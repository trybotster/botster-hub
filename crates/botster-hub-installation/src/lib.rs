//! Shared managed-installation contract for `botster-hub`.
//!
//! Two components touch the same on-disk installation state: the installer that
//! writes it and the Hub that reads it. This crate is the single definition of
//! that state — the receipt shape, the prefix layout, the offline-upgrade lease,
//! the release-source policy, and the descriptor-relative filesystem discipline
//! all of them use — so writer and reader cannot disagree about a file they both
//! touch.
//!
//! It deliberately carries **no cryptography**. The installer is the trust
//! boundary because it is the component that writes executables to disk; the Hub
//! is a read-only reporter and gains no crypto trust anchor, even
//! architecturally. Signature verification lives in `botster-hub-installer`
//! alone, and the Hub records signature *facts* only.

pub mod layout;
pub mod lease;
pub mod receipt;
pub mod release;
pub mod safety;
pub mod source;

pub use lease::{InstallationLease, LeaseMode, LeaseOutcome};
pub use receipt::{
    InstallationReceipt, InstallationsDirectory, KNOWN_ARTIFACT_NAMES, KNOWN_SIGNATURE_ALGORITHMS,
    MAX_RECEIPT_BYTES, PRODUCT_ID, RECEIPT_FILE_NAME, RECEIPT_RELATIVE_PATH,
    RECEIPT_SCHEMA_VERSION, ReceiptArtifact, ReceiptInstaller, ReceiptSignature,
    ReceiptSourceRevisions, is_canonical_object_id, is_sanitized_label, is_sanitized_revision,
    is_sha256_hex, is_supported_channel,
};
pub use release::{
    MAX_RELEASE_BYTES, MINIMUM_RELEASE_SCHEMA_VERSION, ManifestArtifact, ManifestSourceRevisions,
    RELEASE_SCHEMA_VERSION, ReleaseDocument, ReleaseManifest, ReleaseSignature,
};
pub use safety::{DirectoryHandle, FileFacts, FileHandle, InstallationProblem};
pub use source::validate_release_source;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQUENCE: AtomicU64 = AtomicU64::new(1);

    struct Home {
        root: PathBuf,
    }

    impl Home {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "botster-hub-installation-{label}-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).expect("create home fixture");
            Self { root }
        }

        fn installations(&self) -> PathBuf {
            self.root
                .join(receipt::RECEIPT_RELATIVE_PATH.trim_end_matches("/botster-hub.json"))
        }
    }

    impl Drop for Home {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn receipt() -> InstallationReceipt {
        InstallationReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            product_id: PRODUCT_ID.to_string(),
            binary_version: "0.1.0".to_string(),
            installation_mode: "managed".to_string(),
            release_channel: "stable".to_string(),
            provider: "http_json".to_string(),
            source_url: "https://releases.example.invalid/botster-hub.json".to_string(),
            build_revision: "abcdef1".to_string(),
            artifacts: vec![
                ReceiptArtifact {
                    name: "botster-hub".to_string(),
                    sha256: "a".repeat(64),
                    size: 12,
                },
                ReceiptArtifact {
                    name: "botster-session-worker".to_string(),
                    sha256: "b".repeat(64),
                    size: 13,
                },
            ],
            source_revisions: ReceiptSourceRevisions {
                botster_hub: "0".repeat(40),
                botster_core: "1".repeat(40),
            },
            signature: ReceiptSignature {
                algorithm: "ed25519".to_string(),
                key_id: "test-only-do-not-trust".to_string(),
                signed_manifest_sha256: "c".repeat(64),
            },
            installer: ReceiptInstaller {
                id: "botster-hub-installer".to_string(),
                version: "0.1.0".to_string(),
            },
        }
    }

    #[test]
    fn receipt_round_trips_through_an_atomic_descriptor_relative_write() {
        let home = Home::new("round-trip");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        assert!(
            directory.read_receipt().expect("read empty").is_none(),
            "a fresh installations directory has no receipt"
        );
        directory.write_receipt(&receipt()).expect("write receipt");
        assert_eq!(
            directory.read_receipt().expect("read receipt"),
            Some(receipt())
        );

        let file = home.installations().join(receipt::RECEIPT_FILE_NAME);
        assert_eq!(
            fs::metadata(&file)
                .expect("receipt metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(home.installations())
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert!(
            fs::read_dir(home.installations())
                .expect("list installations")
                .flatten()
                .all(|entry| entry.file_name()
                    == receipt::RECEIPT_FILE_NAME.as_ref() as &std::ffi::OsStr),
            "no temporary file survives a successful write"
        );
    }

    #[test]
    fn a_stale_temporary_from_a_crashed_write_never_blocks_a_rerun() {
        let home = Home::new("stale-temp");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        let stale = home.installations().join("botster-hub.json.deadbeef.tmp");
        fs::write(&stale, b"partial").expect("write stale temporary");

        directory
            .write_receipt(&receipt())
            .expect("write receipt over a stale temporary");
        assert!(!stale.exists(), "the sweep removes its own stale temporary");
        assert_eq!(
            directory.read_receipt().expect("read receipt"),
            Some(receipt())
        );
    }

    #[test]
    fn the_sweep_leaves_anything_it_cannot_prove_is_its_own() {
        let home = Home::new("sweep-safety");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        let installations = home.installations();

        let foreign = installations.join("unrelated.json");
        fs::write(&foreign, b"keep").expect("write unrelated file");
        let symlinked = installations.join("botster-hub.json.aaaaaaaa.tmp");
        symlink(&foreign, &symlinked).expect("symlink a temporary name");
        let world_writable = installations.join("botster-hub.json.bbbbbbbb.tmp");
        fs::write(&world_writable, b"loose").expect("write world-writable temporary");
        fs::set_permissions(&world_writable, fs::Permissions::from_mode(0o666))
            .expect("make temporary world-writable");

        directory.sweep_stale_temporaries();

        assert!(foreign.exists(), "a non-matching name is never considered");
        assert!(
            fs::symlink_metadata(&symlinked).is_ok(),
            "the sweep never follows or unlinks a symlink at its own name pattern"
        );
        assert!(
            world_writable.exists(),
            "the sweep leaves a world-writable file it cannot prove is safely its own"
        );
        assert_eq!(fs::read(&foreign).expect("read unrelated"), b"keep");
    }

    #[test]
    fn an_exclusive_create_refuses_a_pre_placed_name_rather_than_overwriting_it() {
        let home = Home::new("exclusive-create");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        let taken = "botster-hub.json.0123456789abcdef.tmp";
        fs::write(home.installations().join(taken), b"pre-placed").expect("pre-place the name");

        let problem = directory
            .handle()
            .create_exclusive_file(taken, 0o600, "receipt temporary")
            .expect_err("O_EXCL refuses a name that is already taken");
        assert_eq!(problem.kind(), "installation_entry_exists");
        assert_eq!(
            fs::read(home.installations().join(taken)).expect("read pre-placed file"),
            b"pre-placed",
            "the pre-placed file is never overwritten"
        );
    }

    /// Race resistance here is proven **structurally**, not by injecting an
    /// adversarial mid-write symlink substitution — deterministic injection of
    /// that race is impractical, and a flaky probabilistic test would be worse
    /// evidence than a stated limit.
    ///
    /// What is proven instead: the only way to reach receipt bytes is through an
    /// [`InstallationsDirectory`], which is reachable only through a validated
    /// directory descriptor, and every component below the home root is opened
    /// `O_NOFOLLOW`. There is no path-taking read or write on the public API for
    /// a caller to reach for, so `fs::write(receipt_path, …)` cannot be
    /// reintroduced by convenience.
    #[test]
    fn receipt_access_is_reachable_only_through_a_validated_directory_descriptor() {
        let home = Home::new("structural");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        directory.write_receipt(&receipt()).expect("write receipt");

        // Substituting a symlink for the receipt is refused by the *operation*,
        // not by a preceding check: the open itself fails.
        let file = home.installations().join(receipt::RECEIPT_FILE_NAME);
        let elsewhere = home.root.join("attacker.json");
        fs::write(&elsewhere, b"attacker").expect("write attacker file");
        fs::remove_file(&file).expect("remove receipt");
        symlink(&elsewhere, &file).expect("substitute a symlink");

        assert_eq!(
            directory
                .read_receipt()
                .expect_err("substituted symlink")
                .kind(),
            "receipt_symlink"
        );
        assert_eq!(
            directory
                .write_receipt(&receipt())
                .map(|()| String::new())
                .unwrap_or_else(|problem| problem.kind().to_string()),
            String::new(),
            "an atomic rename over a symlink replaces the link, never its target"
        );
        assert_eq!(
            fs::read(&elsewhere).expect("read attacker file"),
            b"attacker",
            "the substituted target is byte-unchanged"
        );
    }

    #[test]
    fn a_symlinked_receipt_is_refused_and_its_target_is_untouched() {
        let home = Home::new("symlink-receipt");
        let directory =
            InstallationsDirectory::open_or_create(&home.root).expect("open installations");
        let target = home.root.join("target.json");
        fs::write(&target, b"original").expect("write symlink target");
        symlink(
            &target,
            home.installations().join(receipt::RECEIPT_FILE_NAME),
        )
        .expect("symlink the receipt path");

        let problem = directory
            .read_receipt()
            .expect_err("a symlinked receipt is refused");
        assert_eq!(problem.kind(), "receipt_symlink");
        assert_eq!(
            fs::read(&target).expect("read symlink target"),
            b"original",
            "refusal leaves the target byte-unchanged"
        );
    }

    #[test]
    fn unsafe_receipt_directories_are_refused_for_both_components() {
        for component in [
            receipt::RECEIPT_ROOT_DIRECTORY,
            receipt::RECEIPT_INSTALLATIONS_DIRECTORY,
        ] {
            for shape in ["symlink", "regular_file", "world_writable"] {
                let home = Home::new(&format!("unsafe-{component}-{shape}"));
                let path = if component == receipt::RECEIPT_ROOT_DIRECTORY {
                    home.root.join(component)
                } else {
                    let parent = home.root.join(receipt::RECEIPT_ROOT_DIRECTORY);
                    fs::create_dir_all(&parent).expect("create .botster");
                    parent.join(component)
                };
                let elsewhere = home.root.join("elsewhere");
                fs::create_dir_all(&elsewhere).expect("create symlink target");
                match shape {
                    "symlink" => symlink(&elsewhere, &path).expect("symlink component"),
                    "regular_file" => {
                        fs::write(&path, b"not a directory").expect("write component")
                    }
                    _ => {
                        fs::create_dir_all(&path).expect("create component");
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o777))
                            .expect("make component world-writable");
                    }
                }

                let problem = InstallationsDirectory::open(&home.root)
                    .expect_err("an unsafe receipt directory is refused");
                let expected = if shape == "world_writable" {
                    "receipt_world_writable"
                } else {
                    "unsafe_receipt_directory"
                };
                assert_eq!(
                    problem.kind(),
                    expected,
                    "component={component} shape={shape}"
                );
            }
        }
    }

    #[test]
    fn receipt_validation_covers_schema_shape_and_build_revision_agreement() {
        assert_eq!(receipt().validate("0.1.0", None), Ok(()));
        assert_eq!(receipt().validate("0.1.0", Some("abcdef1")), Ok(()));

        let mismatch = receipt()
            .validate("0.1.0", Some("9999999"))
            .expect_err("a disagreeing embedded revision diagnoses");
        assert_eq!(mismatch.kind(), "receipt_build_revision_mismatch");

        let mut schema_one = receipt();
        schema_one.schema_version = 1;
        assert_eq!(
            schema_one
                .validate("0.1.0", None)
                .expect_err("schema 1 is refused")
                .kind(),
            "unsupported_receipt_schema"
        );

        let mut bad_checksum = receipt();
        bad_checksum.artifacts[0].sha256 = "NOTHEX".to_string();
        assert_eq!(
            bad_checksum
                .validate("0.1.0", None)
                .expect_err("bad checksum")
                .kind(),
            "malformed_receipt_checksum"
        );

        let mut bad_algorithm = receipt();
        bad_algorithm.signature.algorithm = "rsa".to_string();
        assert_eq!(
            bad_algorithm
                .validate("0.1.0", None)
                .expect_err("bad algorithm")
                .kind(),
            "unsupported_signature_algorithm"
        );

        let mut bad_artifact = receipt();
        bad_artifact.artifacts[1].name = "botster-something-else".to_string();
        assert_eq!(
            bad_artifact
                .validate("0.1.0", None)
                .expect_err("unknown artifact")
                .kind(),
            "unknown_receipt_artifact"
        );

        let mut bad_revision = receipt();
        bad_revision.source_revisions.botster_hub = "../escape".to_string();
        assert_eq!(
            bad_revision
                .validate("0.1.0", None)
                .expect_err("non-canonical revision")
                .kind(),
            "malformed_receipt_revision"
        );
    }

    #[test]
    fn generation_names_are_built_only_from_canonical_object_ids() {
        let hub = "0".repeat(40);
        let core = "1".repeat(40);
        assert_eq!(
            layout::generation_name(&hub, &core),
            Some(format!("{hub}-{core}"))
        );
        for hostile in [
            "../escape",
            "a/b",
            "",
            &"a".repeat(65),
            "ABCDEF0123456789abcdef0123456789abcdef01",
            "g".repeat(40).as_str(),
        ] {
            assert_eq!(
                layout::generation_name(hostile, &core),
                None,
                "hostile hub revision accepted: {hostile}"
            );
            assert_eq!(
                layout::generation_name(&hub, hostile),
                None,
                "hostile core revision accepted: {hostile}"
            );
        }
    }

    #[test]
    fn prefix_derivation_matches_layout_shape_from_both_launch_paths() {
        let home = Home::new("prefix");
        let prefix = home.root.join("prefix");
        let generation = prefix.join(layout::GENERATIONS_DIRECTORY).join("gen");
        fs::create_dir_all(&generation).expect("create generation");
        fs::create_dir_all(prefix.join(layout::BIN_DIRECTORY)).expect("create bin");
        symlink("generations/gen", prefix.join(layout::CURRENT_POINTER)).expect("create pointer");

        assert_eq!(
            layout::derive_managed_prefix(&generation.join(layout::HUB_BINARY_NAME)),
            Some(prefix.clone()),
            "a Hub launched by its generation path derives the prefix"
        );
        assert_eq!(
            layout::derive_managed_prefix(
                &prefix
                    .join(layout::BIN_DIRECTORY)
                    .join(layout::HUB_BINARY_NAME)
            ),
            Some(prefix),
            "a Hub launched through bin/ derives the same prefix"
        );

        let development = home.root.join("target").join("debug");
        fs::create_dir_all(&development).expect("create development layout");
        assert_eq!(
            layout::derive_managed_prefix(&development.join(layout::HUB_BINARY_NAME)),
            None,
            "a development build derives no prefix and therefore takes no lease"
        );
    }

    #[test]
    fn the_lease_excludes_an_installer_while_a_daemon_holds_it_and_is_freed_on_release() {
        let home = Home::new("lease");
        let prefix = home.root.join("prefix");
        fs::create_dir_all(&prefix).expect("create prefix");

        let LeaseOutcome::Acquired(daemon) =
            lease::acquire(&prefix, LeaseMode::Shared).expect("daemon takes the shared lease")
        else {
            panic!("an uncontended shared lease must be acquired");
        };
        let LeaseOutcome::Acquired(second_daemon) = lease::acquire(&prefix, LeaseMode::Shared)
            .expect("a second daemon takes the shared lease")
        else {
            panic!("shared leases must not exclude each other");
        };
        assert!(matches!(
            lease::acquire(&prefix, LeaseMode::Exclusive).expect("installer attempt"),
            LeaseOutcome::Contended
        ));

        drop(daemon);
        assert!(
            matches!(
                lease::acquire(&prefix, LeaseMode::Exclusive).expect("installer attempt"),
                LeaseOutcome::Contended
            ),
            "the installer stays refused until every daemon exits"
        );
        drop(second_daemon);

        let LeaseOutcome::Acquired(installer) = lease::acquire(&prefix, LeaseMode::Exclusive)
            .expect("installer takes the exclusive lease")
        else {
            panic!("the exclusive lease must be acquired once every daemon has exited");
        };
        assert_eq!(installer.mode(), LeaseMode::Exclusive);
        assert!(matches!(
            lease::acquire(&prefix, LeaseMode::Shared).expect("daemon attempt"),
            LeaseOutcome::Contended
        ));
        assert!(
            matches!(
                lease::acquire(&prefix, LeaseMode::Exclusive).expect("second installer attempt"),
                LeaseOutcome::Contended
            ),
            "a second installer fails closed rather than interleaving"
        );
    }

    #[test]
    fn a_symlink_at_the_lease_path_is_refused_rather_than_followed() {
        let home = Home::new("lease-symlink");
        let prefix = home.root.join("prefix");
        fs::create_dir_all(&prefix).expect("create prefix");
        let target = home.root.join("elsewhere.lock");
        fs::write(&target, b"").expect("write lock symlink target");
        symlink(&target, prefix.join(layout::DAEMON_LOCK_FILE)).expect("symlink the lock path");

        let problem = lease::acquire(&prefix, LeaseMode::Shared)
            .expect_err("a symlinked lease path is refused");
        assert_eq!(problem.kind(), "unsafe_installation_lock");
    }

    #[test]
    fn the_lease_file_is_user_owned_and_not_world_writable() {
        let home = Home::new("lease-mode");
        let prefix = home.root.join("prefix");
        fs::create_dir_all(&prefix).expect("create prefix");
        let LeaseOutcome::Acquired(_lease) =
            lease::acquire(&prefix, LeaseMode::Shared).expect("take the lease")
        else {
            panic!("an uncontended lease must be acquired");
        };
        let metadata =
            fs::metadata(prefix.join(layout::DAEMON_LOCK_FILE)).expect("lease file metadata");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            std::os::unix::fs::MetadataExt::uid(&metadata),
            safety::effective_uid()
        );
    }

    #[test]
    fn release_sources_require_https_outside_loopback() {
        for accepted in [
            "https://releases.example.invalid/botster-hub.json",
            "http://127.0.0.1:8123/botster-hub.json",
            "http://[::1]:8123/botster-hub.json",
            "http://localhost:8123/botster-hub.json",
        ] {
            assert_eq!(
                validate_release_source(accepted),
                Ok(()),
                "source={accepted}"
            );
        }
        assert_eq!(
            validate_release_source("http://192.0.2.10/botster-hub.json")
                .expect_err("plaintext")
                .kind(),
            "insecure_release_source"
        );
        assert_eq!(
            validate_release_source("file:///tmp/botster-hub.json")
                .expect_err("non-http scheme")
                .kind(),
            "invalid_release_source"
        );
    }
}
