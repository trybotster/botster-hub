use std::path::Path;
use std::process::Command;
use std::sync::Once;

pub fn ensure_session_worker_binary() {
    static BUILD_WORKER: Once = Once::new();
    BUILD_WORKER.call_once(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let status = Command::new("cargo")
            .args([
                "build",
                "--locked",
                "-p",
                "botster-core",
                "--bin",
                "botster-session-worker",
            ])
            .current_dir(manifest_dir)
            .status()
            .expect("worker binary build command should run");
        assert!(status.success(), "botster-session-worker should build");
    });
}
