//! Release-side tooling: key custody and provenance-honest builds.
//!
//! These cover the two places where a mistake produces *signed* material that
//! is wrong rather than material that is obviously broken — which is why both
//! need to fail closed rather than warn.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn scratch(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "botster-hub-release-tooling-{label}-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("create scratch directory");
    path
}

fn text(output: &Output) -> String {
    format!(
        "status={} stdout={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn generate_key(out_dir: &Path, name: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_botster-hub-release-tool"))
        .args(["generate-key", "--out-dir"])
        .arg(out_dir)
        .args(["--name", name])
        .output()
        .expect("run the release tool")
}

fn digest(path: &Path) -> Vec<u8> {
    std::fs::read(path).expect("read key material")
}

/// A private signing key must never be readable by other local users, and the
/// mode has to come from the open itself rather than a follow-up chmod that
/// leaves a window.
#[test]
fn a_generated_private_key_is_owner_only_and_its_public_half_is_not() {
    let out_dir = scratch("key-mode");
    let generated = generate_key(&out_dir, "review-key");
    assert!(generated.status.success(), "{}", text(&generated));

    let private = out_dir.join("review-key.pkcs8");
    let public = out_dir.join("review-key.pub");
    assert_eq!(
        std::fs::metadata(&private)
            .expect("private key metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "private signing material must not be readable by other local users"
    );
    assert_eq!(
        std::fs::metadata(&public)
            .expect("public key metadata")
            .permissions()
            .mode()
            & 0o777,
        0o644,
        "the public half is meant to be distributed"
    );

    let _ = std::fs::remove_dir_all(&out_dir);
}

/// Re-running key generation must fail closed. Silently replacing a keypair
/// destroys the trust relationship every already-published release depends on,
/// and doing it with a zero exit code makes the loss invisible.
#[test]
fn key_generation_refuses_to_replace_existing_material_and_preserves_it() {
    let out_dir = scratch("key-replace");
    let first = generate_key(&out_dir, "review-key");
    assert!(first.status.success(), "{}", text(&first));

    let private = out_dir.join("review-key.pkcs8");
    let public = out_dir.join("review-key.pub");
    let private_before = digest(&private);
    let public_before = digest(&public);

    let second = generate_key(&out_dir, "review-key");
    assert!(
        !second.status.success(),
        "a second generation must refuse rather than rotate silently: {}",
        text(&second)
    );
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("already exists"),
        "{}",
        text(&second)
    );
    assert_eq!(
        digest(&private),
        private_before,
        "the private key is preserved"
    );
    assert_eq!(
        digest(&public),
        public_before,
        "the public key is preserved"
    );

    // A pre-placed public half alone also blocks generation, so a partial
    // directory can never be completed into a mismatched pair.
    let other = scratch("key-partial");
    std::fs::write(other.join("review-key.pub"), b"pre-placed\n").expect("pre-place public half");
    let partial = generate_key(&other, "review-key");
    assert!(!partial.status.success(), "{}", text(&partial));
    assert!(
        !other.join("review-key.pkcs8").exists(),
        "no private key is left behind when the pair cannot be completed"
    );
    assert_eq!(
        std::fs::read(other.join("review-key.pub")).expect("read pre-placed public half"),
        b"pre-placed\n"
    );

    let _ = std::fs::remove_dir_all(&out_dir);
    let _ = std::fs::remove_dir_all(&other);
}

/// The release script must refuse a dirty checkout **before** building or
/// signing.
///
/// The manifest records `source_revisions.botster_hub` as HEAD while cargo
/// compiles the working tree, so a modified tracked file would be compiled,
/// checksummed, and signed under provenance that claims something else. The
/// refusal is what keeps the signed fact auditable.
///
/// Run against a throwaway git repository rather than this checkout: dirtying
/// the real worktree mid-suite would race every other test.
#[test]
fn a_release_build_refuses_a_dirty_checkout_before_producing_signed_metadata() {
    let root = scratch("dirty-checkout");
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&root)
            .env("GIT_AUTHOR_NAME", "Release Fixture")
            .env("GIT_AUTHOR_EMAIL", "release@example.invalid")
            .env("GIT_COMMITTER_NAME", "Release Fixture")
            .env("GIT_COMMITTER_EMAIL", "release@example.invalid")
            .output()
            .expect("run git in the release fixture");
        assert!(output.status.success(), "git {args:?}: {}", text(&output));
    };

    run_git(&["init", "--quiet"]);
    std::fs::write(root.join("tracked-source.rs"), b"// original\n").expect("write tracked source");
    run_git(&["add", "."]);
    run_git(&["commit", "--quiet", "-m", "release fixture"]);

    let script_source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../script/build-release-artifacts")
        .canonicalize()
        .expect("resolve the release script");
    std::fs::create_dir_all(root.join("script")).expect("create fixture script directory");
    let script = root.join("script/build-release-artifacts");
    std::fs::copy(&script_source, &script).expect("copy the release script");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("make the release script executable");
    // Committing the copied script keeps the checkout clean, so the only thing
    // dirtying it below is the deliberate source edit.
    run_git(&["add", "."]);
    run_git(&["commit", "--quiet", "-m", "add release script"]);

    let out_dir = root.join("release-out");
    let invoke = || {
        Command::new(&script)
            .args(["--out-dir"])
            .arg(&out_dir)
            .args(["--key", "/nonexistent.pkcs8", "--key-id", "fixture"])
            .args(["--base-url", "https://releases.example.invalid"])
            .current_dir(&root)
            .output()
            .expect("run the release script")
    };

    // Dirty a tracked, build-affecting file.
    std::fs::write(root.join("tracked-source.rs"), b"// smuggled change\n")
        .expect("dirty a tracked source file");
    let refused = invoke();
    assert!(
        !refused.status.success(),
        "a dirty checkout must refuse: {}",
        text(&refused)
    );
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("dirty checkout"), "{}", text(&refused));
    assert!(stderr.contains("tracked-source.rs"), "{}", text(&refused));
    assert!(
        !out_dir.exists(),
        "no artifacts or signed metadata are produced from a dirty checkout"
    );

    // An untracked build-affecting file is dirty too: it can be compiled and
    // would be invisible in the recorded revision.
    run_git(&["checkout", "--", "tracked-source.rs"]);
    std::fs::write(root.join("smuggled.rs"), b"// untracked\n").expect("add an untracked source");
    let refused_untracked = invoke();
    assert!(
        !refused_untracked.status.success(),
        "an untracked build-affecting file must also refuse: {}",
        text(&refused_untracked)
    );
    assert!(
        String::from_utf8_lossy(&refused_untracked.stderr).contains("smuggled.rs"),
        "{}",
        text(&refused_untracked)
    );
    assert!(!out_dir.exists());

    // Clean again: the cleanliness gate now passes and the script proceeds far
    // enough to fail on the fixture's absent Cargo.lock, proving the refusal
    // above came from the dirty checkout and not from the fixture's shape.
    std::fs::remove_file(root.join("smuggled.rs")).expect("remove the untracked source");
    let clean = invoke();
    assert!(!clean.status.success(), "the fixture has no cargo project");
    let clean_stderr = String::from_utf8_lossy(&clean.stderr);
    assert!(
        !clean_stderr.contains("dirty checkout"),
        "a clean checkout must clear the provenance gate: {}",
        text(&clean)
    );

    let _ = std::fs::remove_dir_all(&root);
}
