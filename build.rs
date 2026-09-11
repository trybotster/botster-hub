use std::env;

fn main() {
    emit_rustc_version();
    println!("cargo:rerun-if-env-changed=BOTSTER_BUILD_REVISION");

    let Some(revision) = env::var("BOTSTER_BUILD_REVISION")
        .ok()
        .filter(|revision| is_sanitized_revision(revision))
    else {
        return;
    };

    println!("cargo:rustc-env=BOTSTER_EMBEDDED_BUILD_REVISION={revision}");
}

fn emit_rustc_version() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let Ok(output) = std::process::Command::new(rustc).arg("-V").output() else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let version = String::from_utf8_lossy(&output.stdout);
    println!("cargo:rustc-env=BOTSTER_RUSTC_VERSION={}", version.trim());
}

fn is_sanitized_revision(revision: &str) -> bool {
    !revision.is_empty()
        && revision.len() <= 64
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
