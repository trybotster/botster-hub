use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=BOTSTER_BUILD_REVISION");

    let Some(revision) = env::var("BOTSTER_BUILD_REVISION")
        .ok()
        .filter(|revision| is_sanitized_revision(revision))
    else {
        return;
    };

    println!("cargo:rustc-env=BOTSTER_EMBEDDED_BUILD_REVISION={revision}");
}

fn is_sanitized_revision(revision: &str) -> bool {
    !revision.is_empty()
        && revision.len() <= 64
        && revision
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
