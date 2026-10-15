//! `cargo xtask unsafe-code`: the workspace forbids `unsafe_code`, with the named-exception rule of Core
//! (`botster_gate_kit::unsafe_exception`). The Hub has one exception, `take_ready_fd` (HC HS-0 `--ready-fd`; lead ruling
//! 2026-10-12), and no FFI crate: any `unsafe_code` lint entry but the workspace's and the exception crate's, and any
//! `unsafe_code` identifier in a Rust source but the exception's, fails.

use anyhow::{bail, Context, Result};
use botster_gate_kit::files::tracked_files;
use botster_gate_kit::unsafe_exception::{checked, verdict, Exception, Policy};
use std::path::Path;

/// The exceptions: the one item that turns a ready-fd number into an owned descriptor.
pub const EXCEPTIONS: [Exception; 1] = [Exception {
    manifest: "crates/botster-hub/Cargo.toml",
    file: "crates/botster-hub/src/main.rs",
    item: &["fn", "take_ready_fd"],
}];

/// The Hub's policy: its exception, no FFI crate.
pub const POLICY: Policy = Policy {
    exceptions: &EXCEPTIONS,
    ffi_manifests: &[],
};

/// The check over the repository's tracked files.
///
/// # Errors
/// A file could not be read, or the check found a problem.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        bail!("unknown argument '{arg}'");
    }
    let read = |file: &str| {
        std::fs::read_to_string(root.join(file)).with_context(|| format!("read {file}"))
    };
    let mut files = Vec::new();
    for file in tracked_files(root)?
        .into_iter()
        .filter(|file| checked(file))
    {
        let text = read(&file)?;
        files.push((file, text));
    }
    let mut krates = Vec::new();
    for exception in EXCEPTIONS {
        krates.push((exception.manifest, read(exception.manifest)?));
    }
    let summary =
        verdict(&POLICY, &read("Cargo.toml")?, &krates, &files).map_err(anyhow::Error::msg)?;
    println!("{summary}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_gate_kit::unsafe_exception::{lint_drift, other_attributes, other_manifests};

    /// The exception crate's lint table is the workspace's with `deny` in place of `forbid`, and nothing else.
    #[test]
    fn the_exception_crate_differs_from_the_workspace_by_deny_only() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let read = |file: &str| std::fs::read_to_string(root.join(file)).unwrap();
        for exception in EXCEPTIONS {
            assert_eq!(
                lint_drift(
                    &read("Cargo.toml"),
                    exception.manifest,
                    &read(exception.manifest)
                ),
                Ok(())
            );
        }
    }

    /// The Hub's own manifests pass, and with no exception any `#[allow(unsafe_code)]` fails.
    #[test]
    fn the_workspace_forbids_unsafe_code_with_no_exception() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let workspace = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
        assert!(workspace.contains("[workspace.lints.rust]\nunsafe_code = \"forbid\"\n"));
        assert_eq!(
            other_manifests(&POLICY, &[("Cargo.toml".to_string(), workspace)]),
            Vec::<String>::new()
        );
        let allowed = "#[allow(unsafe_code)]\nfn close_inherited() {}\n";
        assert_eq!(
            other_attributes(
                &POLICY,
                &[(
                    "crates/botster-hub/src/lib.rs".to_string(),
                    allowed.to_string()
                )]
            ),
            ["crates/botster-hub/src/lib.rs:1: #[allow(unsafe_code)]"]
        );
        let ffi = "[lints.rust]\nunsafe_code = \"allow\"\n";
        assert_eq!(
            other_manifests(
                &POLICY,
                &[("crates/x/Cargo.toml".to_string(), ffi.to_string())]
            ),
            ["crates/x/Cargo.toml: lints.rust.unsafe_code"]
        );
    }
}
