//! Control adapter for the shared-session and resource harnesses.
//!
//! Build with `cargo build -p botster-hub-client --example harness_control`.
//! Set BOTSTER_HUB_CLIENT_ADAPTER_BIN to the resulting executable.
//! Each input line contains one DaemonRequest. Each output line contains its
//! DaemonResponse. This pipe format is not the daemon socket protocol.
//! The parent enforces a deadline and terminates this process on timeout.

use std::error::Error;
use std::io::{self, BufRead, Write};

use botster_hub_client::{DaemonConnection, DaemonEndpoint, DaemonRequest};

fn serve(
    endpoint: &DaemonEndpoint,
    persistent: bool,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<(), Box<dyn Error>> {
    let mut connection = DaemonConnection::connect(endpoint)?;
    if persistent {
        writeln!(output, "{{\"ready\":true}}")?;
        output.flush()?;
    }
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line)? == 0 {
            if persistent {
                return Ok(());
            }
            return Err("expected one request on stdin".into());
        }
        let request: DaemonRequest = serde_json::from_str(&line)?;
        let response = connection.request(&request)?;
        // These harnesses observe responses and subscription counters only.
        // The client parks unsolicited frames until the response arrives.
        drop(connection.take_skipped_entity_frames());
        drop(connection.take_skipped_events());
        drop(connection.take_skipped_terminal());
        serde_json::to_writer(&mut *output, &response)?;
        writeln!(output)?;
        output.flush()?;
        if !persistent {
            return Ok(());
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 3 || args[1] != "--socket" {
        return Err("usage: harness_control request|connection --socket PATH".into());
    }
    let persistent = match args[0].to_str() {
        Some("request") => false,
        Some("connection") => true,
        _ => return Err("mode must be request or connection".into()),
    };
    let endpoint = DaemonEndpoint::new(&args[2]);
    serve(
        &endpoint,
        persistent,
        &mut io::stdin().lock(),
        &mut io::stdout().lock(),
    )
}

fn main() {
    if let Err(error) = run() {
        eprintln!("hub-client-adapter failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use botster_hub_client::{
        ClientFrame, DaemonCompatibility, DaemonEntityFrame, DaemonHelloAck, DaemonResponse,
        DaemonUnixFrame, PROTOCOL, PROTOCOL_VERSION, ServerFrame, decode_unix_frame,
        write_server_frame,
    };

    struct SocketFixture(PathBuf);

    impl SocketFixture {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            Self(std::env::temp_dir().join(format!(
                "harness-{}-{}.sock",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for SocketFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn read_client(stream: &mut UnixStream) -> ClientFrame {
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let length = u32::from_le_bytes(prefix) as usize;
        assert!(length <= 4096);
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).unwrap();
        let DaemonUnixFrame::Control(frame) = decode_unix_frame(&payload).unwrap() else {
            panic!("expected a control frame");
        };
        frame
    }

    fn hello(listener: &UnixListener, compatible: bool) -> UnixStream {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let ClientFrame::Hello { hello } = read_client(&mut stream) else {
            panic!("expected Hello");
        };
        assert_eq!(hello.protocol, PROTOCOL);
        assert_eq!(hello.compatibility.protocol_version, PROTOCOL_VERSION);
        let mut compatibility = DaemonCompatibility::current();
        if !compatible {
            compatibility.protocol_version -= 1;
        }
        write_server_frame(
            &mut stream,
            &ServerFrame::HelloAck {
                ack: DaemonHelloAck {
                    protocol: PROTOCOL.into(),
                    compatibility,
                    terminal_compatibility: None,
                    diagnostics: vec![],
                },
            },
        )
        .unwrap();
        stream
    }

    fn response(kind: &str, error: serde_json::Value) -> DaemonResponse {
        serde_json::from_value(serde_json::json!({
            "kind": kind, "sessions": [], "packages": [], "events": [], "lifecycle": [],
            "error": error
        }))
        .unwrap()
    }

    #[test]
    fn persistent_requests_keep_the_connection_and_skip_early_entities() {
        let fixture = SocketFixture::new();
        let listener = UnixListener::bind(&fixture.0).unwrap();
        let peer = std::thread::spawn(move || {
            let mut stream = hello(&listener, true);
            for (index, kind) in ["entity_subscribed", "entity_unsubscribed"]
                .iter()
                .enumerate()
            {
                let ClientFrame::Request {
                    request_id,
                    request,
                } = read_client(&mut stream)
                else {
                    panic!("expected a request on the original connection");
                };
                assert_eq!(request_id, (index + 1).to_string());
                match (index, request) {
                    (
                        0,
                        DaemonRequest::SubscribeEntities {
                            subscription_id, ..
                        },
                    )
                    | (1, DaemonRequest::UnsubscribeEntities { subscription_id }) => {
                        assert_eq!(subscription_id, "probe");
                    }
                    _ => panic!("unexpected subscription request"),
                }
                write_server_frame(
                    &mut stream,
                    &ServerFrame::Entity {
                        entity: DaemonEntityFrame::Snapshot {
                            subscription_id: "probe".into(),
                            entity_type: "session".into(),
                            snapshot_seq: 1,
                            items: vec![],
                            resync_reason: None,
                        },
                    },
                )
                .unwrap();
                write_server_frame(
                    &mut stream,
                    &ServerFrame::Response {
                        request_id,
                        response: response(kind, serde_json::Value::Null),
                    },
                )
                .unwrap();
            }
            assert_eq!(
                stream.read(&mut [0]).unwrap(),
                0,
                "EOF must close the daemon connection"
            );
        });
        let mut input = Cursor::new(concat!(
            "{\"type\":\"subscribe_entities\",\"entity_type\":\"session\",\"subscription_id\":\"probe\"}\n",
            "{\"type\":\"unsubscribe_entities\",\"subscription_id\":\"probe\"}\n"
        ));
        let mut output = Vec::new();
        serve(
            &DaemonEndpoint::new(&fixture.0),
            true,
            &mut input,
            &mut output,
        )
        .unwrap();
        peer.join().unwrap();
        let rows: Vec<serde_json::Value> = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], serde_json::json!({"ready": true}));
        assert_eq!(rows[1]["kind"], "entity_subscribed");
        assert_eq!(rows[2]["kind"], "entity_unsubscribed");
    }

    #[test]
    fn one_shot_preserves_daemon_errors_as_responses() {
        let fixture = SocketFixture::new();
        let listener = UnixListener::bind(&fixture.0).unwrap();
        let peer =
            std::thread::spawn(move || {
                let mut stream = hello(&listener, true);
                let ClientFrame::Request { request_id, .. } = read_client(&mut stream) else {
                    panic!("expected a request");
                };
                write_server_frame(&mut stream, &ServerFrame::Response {
                request_id, response: response("operator_error", serde_json::json!({
                    "code": "too_many_requests", "request_id": "1", "operation": "status",
                    "message": "fixture refusal"
                })),
            }).unwrap();
                assert_eq!(stream.read(&mut [0]).unwrap(), 0);
            });
        let mut output = Vec::new();
        serve(
            &DaemonEndpoint::new(&fixture.0),
            false,
            &mut Cursor::new("{\"type\":\"status\"}\n"),
            &mut output,
        )
        .unwrap();
        peer.join().unwrap();
        let row: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(row["kind"], "operator_error");
        assert_eq!(row["error"]["code"], "too_many_requests");
    }

    #[test]
    fn incompatible_hello_fails_without_a_request_or_ready_output() {
        let fixture = SocketFixture::new();
        let listener = UnixListener::bind(&fixture.0).unwrap();
        let peer = std::thread::spawn(move || {
            let mut stream = hello(&listener, false);
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        });
        let mut output = Vec::new();
        assert!(
            serve(
                &DaemonEndpoint::new(&fixture.0),
                true,
                &mut Cursor::new("{\"type\":\"status\"}\n"),
                &mut output
            )
            .is_err()
        );
        peer.join().unwrap();
        assert!(output.is_empty());
    }
}
