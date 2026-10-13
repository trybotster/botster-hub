//! `cargo xtask unsafe-code`: the workspace forbids `unsafe_code`, with the named-exception rule of Core
//! (`botster_gate_kit::unsafe_exception`). The Hub has no exception and no FFI crate: any `unsafe_code` lint entry but the
//! workspace's, and any `unsafe_code` identifier in a Rust source, fails.

use anyhow::{bail, Context, Result};
use botster_gate_kit::files::tracked_files;
use botster_gate_kit::unsafe_exception::{checked, verdict, Policy};
use std::path::Path;

/// The Hub's policy: no exception, no FFI crate.
pub const POLICY: Policy = Policy {
    exceptions: &[],
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
    let summary =
        verdict(&POLICY, &read("Cargo.toml")?, &[], &files).map_err(anyhow::Error::msg)?;
    println!("{summary}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_gate_kit::unsafe_exception::{other_attributes, other_manifests};

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
