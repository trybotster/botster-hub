//! Consume Core-owned late-attach GHOSTSNP files from the pinned
//! `botster-terminal-protocol` crate source. Hub Git must not store those bytes.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const PROTOCOL_CRATE: &str = "botster-terminal-protocol";
const PROTOCOL_REV: &str = "a781556258789dea4a50ffcb17351e7294c8ff26";
const FIXTURE_FILES: &[&str] = &[
    "late-attach-history-ready-v2.ghostsnp",
    "late-attach-history-page-v2.ghostsnp",
    "late-attach-history-finish-v2.ghostsnp",
    "late-attach-blank-ready-v2.ghostsnp",
    "late-attach-blank-finish-v2.ghostsnp",
];

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let fixture_dir = protocol_crate_source().join("fixtures").join("ghostsnp");
    for name in FIXTURE_FILES {
        let src = fixture_dir.join(name);
        if !src.is_file() {
            panic!(
                "missing Core protocol fixture {name} in {PROTOCOL_CRATE} at rev {PROTOCOL_REV} ({})",
                src.display()
            );
        }
        println!("cargo:rerun-if-changed={}", src.display());
        fs::copy(&src, out_dir.join(name)).unwrap_or_else(|error| {
            panic!("copy {} into OUT_DIR: {error}", src.display());
        });
    }
}

fn protocol_crate_source() -> PathBuf {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let manifest_path =
        Path::new(&env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let output = Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(&manifest_path)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "cargo metadata failed while locating {PROTOCOL_CRATE} rev {PROTOCOL_REV}: {error}"
            )
        });
    if !output.status.success() {
        panic!(
            "cargo metadata failed while locating {PROTOCOL_CRATE} rev {PROTOCOL_REV}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse cargo metadata JSON");
    let packages = metadata["packages"]
        .as_array()
        .expect("cargo metadata packages");
    for package in packages {
        if package["name"].as_str() != Some(PROTOCOL_CRATE) {
            continue;
        }
        let source = package["source"].as_str().unwrap_or("");
        if !source.contains(PROTOCOL_REV) {
            continue;
        }
        let manifest = package["manifest_path"]
            .as_str()
            .expect("protocol crate manifest_path");
        return PathBuf::from(manifest)
            .parent()
            .expect("protocol crate source dir")
            .to_path_buf();
    }
    panic!("cargo metadata did not include {PROTOCOL_CRATE} at rev {PROTOCOL_REV}");
}
