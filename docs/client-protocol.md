# Botster Hub Client Protocol

The authoritative reusable client-to-hub daemon protocol lives in:

- `crates/botster-hub-client/src/lib.rs`
- `src/daemon_transport.rs`

Implementation baseline before this split: `9b39f1607144319138151cdf776e8909f35a63d4`. The pipeline implementation commit should be treated as the final protocol revision once merged.

External same-device clients should depend on the `botster-hub-client` crate and use `DaemonEndpoint`, `DaemonConnection`, `request`, or `stream_attach` to talk to a running `botster-hub` daemon socket. The crate owns the client-facing handshake, request, response, event, and JSON frame helpers.

The production route is:

`botster_hub_client::DaemonConnection::request`
to the daemon socket, then `src/daemon_transport.rs` `serve_daemon`/`handle_connection`, then `handle_runtime_control_request`, then `HubClientApi::handle_request`, then `HubRuntime` and the core daemon `SessionIo`/`ClientWorker` terminal data plane.

Do not reuse `botster_core::contract` session-worker protocol, session frame magic, `DefaultEngineCommand`, `TransportIngress`, or `BoundaryJson` for external clients. Those are not the client-to-hub protocol. The client crate also intentionally excludes hub runtime, embedded TUI, Lua/plugin runtime, `ratatui`, `crossterm`, `mlua`, and core UI action/node types.

Plugin surface and action responses cross the daemon boundary as JSON values. Hub-owned code may deserialize them into local core UI types, but external clients are not required to compile those internal UI/runtime dependencies.

## Isolated Integration Tests For External Clients

External clients that need a true live-hub integration test should depend on the
client protocol crate plus the test-support crate, not on the full `botster-hub`
library:

```toml
[dev-dependencies]
botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-client", rev = "<hub-rev>" }
botster-hub-test-support = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-test-support", rev = "<hub-rev>" }
```

The harness starts the `botster-hub` binary as a subprocess and talks to it
through `botster-hub-client`. It does not compile or link hub runtime, TUI, Lua,
or plugin internals into the downstream client.

Build or otherwise provide both binaries before running the downstream test:

```bash
BOTSTER_ENV=test cargo build --locked --bin botster-hub
BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker
```

Then pass explicit paths into the harness. Environment variables are accepted as
a convenience, but the library never relies on `CARGO_BIN_EXE_botster-hub`
internally because Cargo only injects that variable for the package that owns the
binary.

```rust
use botster_hub_client::{DaemonConnection, DaemonRequest};
use botster_hub_test_support::IsolatedHubBuilder;

let hub = IsolatedHubBuilder::new()
    .hub_bin(std::env::var("BOTSTER_HUB_BIN").expect("BOTSTER_HUB_BIN"))
    .session_worker_bin(
        std::env::var("BOTSTER_SESSION_WORKER_BIN").expect("BOTSTER_SESSION_WORKER_BIN"),
    )
    .name("my-client-test")
    .start()
    .expect("isolated hub starts");

let status = botster_hub_client::request(hub.endpoint(), DaemonRequest::Status)
    .expect("status request");
assert_eq!(status.status.expect("status body").lifecycle_state, "running");

let mut connection = DaemonConnection::connect(hub.endpoint()).expect("connect");
connection
    .request(&DaemonRequest::Spawn {
        session_id: "my-client-session".to_string(),
        command: "printf 'ready\\n'; while IFS= read -r line; do printf 'echo:%s\\n' \"$line\"; done".to_string(),
    })
    .expect("spawn session");
connection
    .request(&DaemonRequest::Attach {
        session_id: "my-client-session".to_string(),
        subscription_id: "my-client-subscription".to_string(),
    })
    .expect("attach session");
connection
    .request(&DaemonRequest::Resize {
        session_id: "my-client-session".to_string(),
        rows: 31,
        cols: 101,
    })
    .expect("resize session");
connection
    .request(&DaemonRequest::SendInput {
        session_id: "my-client-session".to_string(),
        data: "hello\\n".to_string(),
    })
    .expect("send input");
connection
    .request(&DaemonRequest::Drain {
        session_id: "my-client-session".to_string(),
    })
    .expect("drain output");
connection
    .request(&DaemonRequest::Detach {
        session_id: "my-client-session".to_string(),
        subscription_id: "my-client-subscription".to_string(),
    })
    .expect("detach session");
botster_hub_client::request(
    hub.endpoint(),
    DaemonRequest::ShutdownSession {
        session_id: "my-client-session".to_string(),
    },
)
.expect("shutdown session");
hub.shutdown().expect("shutdown isolated hub");
```

Each harness instance creates a disposable data directory and socket path under
the configured test root, uses synthetic default hub identity, and attempts a
daemon shutdown on drop with a kill fallback for failed tests. Tests should still
call `shutdown()` explicitly when they need teardown failures to be visible.
