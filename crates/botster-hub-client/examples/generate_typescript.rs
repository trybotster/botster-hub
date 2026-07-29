use std::fs;
use std::path::PathBuf;

fn main() {
    let artifact = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("generated/daemon-protocol.ts");
    let expected = botster_hub_client::daemon_protocol_typescript();

    if std::env::args().any(|argument| argument == "--check") {
        let actual =
            fs::read_to_string(&artifact).expect("read generated daemon protocol artifact");
        assert_eq!(actual, expected, "generated daemon protocol is stale");
        return;
    }

    fs::write(artifact, expected).expect("write generated daemon protocol artifact");
}
