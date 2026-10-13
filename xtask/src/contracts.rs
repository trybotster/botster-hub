//! The pinned botster-contracts checkout: the root of the git checkout that cargo made for the Hub's contract crates.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The package whose manifest locates the checkout: `<root>/crates/botster-hub-contract/Cargo.toml`.
const LOCATOR: &str = "botster-hub-contract";

/// The checkout root, from the JSON of `cargo metadata`: the directory two levels above the locator crate's directory.
///
/// # Errors
/// The text is not cargo metadata, the locator is missing or listed twice, or its manifest path is too short.
pub fn root_from_metadata(json: &str) -> Result<PathBuf, String> {
    let doc: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("cargo metadata is not JSON: {e}"))?;
    let packages = doc["packages"]
        .as_array()
        .ok_or("cargo metadata has no packages")?;
    let manifests: Vec<&str> = packages
        .iter()
        .filter(|package| package["name"] == LOCATOR)
        .filter_map(|package| package["manifest_path"].as_str())
        .collect();
    let [manifest] = manifests.as_slice() else {
        return Err(format!(
            "cargo metadata lists {} {LOCATOR} packages, expected one",
            manifests.len()
        ));
    };
    Path::new(manifest)
        .ancestors()
        .nth(3)
        .map(Path::to_path_buf)
        .ok_or(format!("{LOCATOR}: manifest path {manifest} is too short"))
}

/// The checkout root of the workspace at `root`.
///
/// # Errors
/// cargo metadata fails, or its output does not locate the checkout.
pub fn root(root: &Path) -> Result<PathBuf> {
    let out = Command::new("cargo")
        .current_dir(root)
        .args(["metadata", "--format-version", "1", "--locked"])
        .output()
        .context("run cargo metadata")?;
    if !out.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let json = String::from_utf8(out.stdout).context("cargo metadata is not UTF-8")?;
    root_from_metadata(&json).map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(packages: &[(&str, &str)]) -> String {
        let packages: Vec<serde_json::Value> = packages
            .iter()
            .map(|(name, path)| serde_json::json!({"name": name, "manifest_path": path}))
            .collect();
        serde_json::json!({ "packages": packages }).to_string()
    }

    #[test]
    fn the_root_is_two_levels_above_the_locator_crate() {
        let json = metadata(&[
            ("anyhow", "/c/registry/anyhow/Cargo.toml"),
            (LOCATOR, "/c/git/checkouts/botster-contracts-x/abc1234/crates/botster-hub-contract/Cargo.toml"),
        ]);
        assert_eq!(
            root_from_metadata(&json),
            Ok(PathBuf::from(
                "/c/git/checkouts/botster-contracts-x/abc1234"
            ))
        );
    }

    #[test]
    fn a_missing_twice_listed_or_short_locator_is_an_error() {
        assert_eq!(
            root_from_metadata(&metadata(&[("anyhow", "/a/Cargo.toml")])),
            Err(format!(
                "cargo metadata lists 0 {LOCATOR} packages, expected one"
            ))
        );
        assert_eq!(
            root_from_metadata(&metadata(&[
                (LOCATOR, "/a/b/c/Cargo.toml"),
                (LOCATOR, "/d/e/f/Cargo.toml")
            ])),
            Err(format!(
                "cargo metadata lists 2 {LOCATOR} packages, expected one"
            ))
        );
        assert_eq!(
            root_from_metadata(&metadata(&[(LOCATOR, "c/Cargo.toml")])),
            Err(format!(
                "{LOCATOR}: manifest path c/Cargo.toml is too short"
            ))
        );
        assert!(root_from_metadata("[")
            .unwrap_err()
            .starts_with("cargo metadata is not JSON"));
        assert_eq!(
            root_from_metadata("{}"),
            Err("cargo metadata has no packages".to_string())
        );
    }
}
