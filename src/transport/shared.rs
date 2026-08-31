pub(crate) mod adapter_slot;
pub(crate) mod close_progress;
pub(crate) mod close_reason;
pub(crate) mod ingress;
pub(crate) mod wake;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    fn shared_sources() -> Vec<(String, String)> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/transport/shared");
        let mut files = Vec::new();
        let entries = fs::read_dir(&root).expect("read shared transport");
        for entry in entries {
            let path = entry.expect("shared entry").path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let relative = format!(
                "src/transport/shared/{}",
                path.file_name().expect("name").to_string_lossy()
            );
            let source = fs::read_to_string(&path).expect("read shared source");
            files.push((relative, source));
        }
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    #[test]
    fn shared_transport_contains_no_admission_route_grant_or_product_policy() {
        const FORBIDDEN: &[&str] = &[
            "ClosedEventRoute",
            "ClosedHandle",
            "UnixConnectionMux",
            "WebRtcConnectionMux",
            "GrantRegistry",
            "UnixTerminalAdmission",
            "WebrtcTerminalAdmission",
            "suppress_generation",
            "suppress_session",
            "DaemonEvent",
            "TERMINAL_SUBSCRIPTION_CLOSED",
            "admit_close_events",
            "label",
        ];
        for (path, source) in shared_sources() {
            let production = source.split("mod tests").next().unwrap_or(&source);
            for forbidden in FORBIDDEN {
                if *forbidden == "label" && production.contains("after_route") {
                    let without_cursor = production.replace("after_route", "");
                    assert!(
                        !without_cursor.contains("label"),
                        "{path} production source must not contain {forbidden}"
                    );
                    continue;
                }
                assert!(
                    !production.contains(forbidden),
                    "{path} production source must not contain {forbidden}"
                );
            }
        }
    }

    #[test]
    fn shared_transport_declares_no_cross_transport_mux_or_route_record() {
        let mut combined = String::new();
        for (path, source) in shared_sources() {
            combined.push_str(&source);
            assert!(
                !source.contains("struct ClosedEventRoute")
                    && !source.contains("trait ClosedHandle")
                    && !source.contains("struct UnixConnectionMux")
                    && !source.contains("struct WebRtcConnectionMux"),
                "{path} must not declare a route record or shared mux"
            );
        }
        assert!(
            !combined.contains("fn register("),
            "shared transport must not own a route registry"
        );
        let unix = include_str!("unix/adapter.rs");
        let webrtc = include_str!("webrtc/adapter.rs");
        assert!(
            unix.contains("pub(crate) struct UnixConnectionMux"),
            "Unix mux stays in the Unix adapter"
        );
        assert!(
            webrtc.contains("pub(crate) struct WebRtcConnectionMux"),
            "WebRTC mux stays in the WebRTC adapter"
        );
    }
}
