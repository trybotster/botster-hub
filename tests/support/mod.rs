use std::process::Command;
use std::sync::Once;

pub fn ensure_session_worker_binary() {
    static BUILD_WORKER: Once = Once::new();
    BUILD_WORKER.call_once(|| {
        let metadata = Command::new("cargo")
            .args(["metadata", "--format-version", "1", "--locked"])
            .output()
            .expect("cargo metadata should run");
        assert!(
            metadata.status.success(),
            "cargo metadata should locate dependency packages: {}",
            String::from_utf8_lossy(&metadata.stderr)
        );
        let metadata: serde_json::Value =
            serde_json::from_slice(&metadata.stdout).expect("cargo metadata should be JSON");
        let core_manifest_path = metadata["packages"]
            .as_array()
            .and_then(|packages| {
                packages.iter().find_map(|package| {
                    (package["name"].as_str() == Some("botster-core"))
                        .then(|| package["manifest_path"].as_str())
                        .flatten()
                })
            })
            .expect("botster-core package should appear in cargo metadata");
        let core_workspace_manifest_path = std::path::Path::new(core_manifest_path)
            .parent()
            .and_then(std::path::Path::parent)
            .and_then(std::path::Path::parent)
            .map(|path| path.join("Cargo.toml"))
            .expect("botster-core package should live under the core workspace");
        let target_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
        let status = Command::new("cargo")
            .args([
                "build",
                "--manifest-path",
                core_workspace_manifest_path
                    .to_str()
                    .expect("core workspace manifest should be UTF-8"),
                "--locked",
                "-p",
                "botster-core",
                "--bin",
                "botster-session-worker",
                "--target-dir",
                target_dir
                    .to_str()
                    .expect("target dir should be UTF-8 for cargo"),
            ])
            .status()
            .expect("worker binary build command should run");
        assert!(status.success(), "botster-session-worker should build");
    });
}
