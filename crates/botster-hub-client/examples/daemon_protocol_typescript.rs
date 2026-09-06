//! Emit the generated daemon protocol TypeScript to stdout.
//!
//! Regenerate the checked artifact with:
//! `cargo run -p botster-hub-client --example daemon_protocol_typescript > crates/botster-hub-client/generated/daemon-protocol.ts`

fn main() {
    print!("{}", botster_hub_client::daemon_protocol_typescript());
}
