# Botster Hub Client Protocol

The authoritative reusable client-to-hub daemon protocol lives in:

- `crates/botster-hub-client/src/lib.rs`
- `src/daemon_transport.rs`

Implementation baseline before this split: `9b39f1607144319138151cdf776e8909f35a63d4`. The pipeline implementation commit should be treated as the final protocol revision once merged.

External same-device clients should depend on the `botster-hub-client` crate and use `DaemonEndpoint`, `DaemonConnection`, `request`, or `stream_attach` to talk to a running `botster-hub` daemon socket. The crate owns the client-facing handshake, request, response, event, and JSON frame helpers.

The control-plane production route is:

`botster_hub_client::DaemonConnection::request`
to the daemon socket, then `src/daemon_transport.rs` `serve_daemon`/`handle_connection`, then `handle_runtime_control_request`, then `HubClientApi::handle_request`, then `HubRuntime` and the core daemon `SessionIo`/`ClientWorker` terminal data plane.

Terminal attach and drain conformance uses `botster_hub_client::stream_attach`.
That helper still connects through the daemon socket, but terminal bytes are
delivered by the hub-owned client/session actor data plane rather than by a
private session-worker frame contract.

Do not reuse `botster_core::contract` session-worker protocol, session frame magic, `DefaultEngineCommand`, `TransportIngress`, or `BoundaryJson` for external clients. Those are not the client-to-hub protocol. The client crate also intentionally excludes hub runtime, embedded TUI, Lua/plugin runtime, `ratatui`, `crossterm`, `mlua`, and core UI action/node types.

Plugin surface and action responses cross the daemon boundary as JSON values. Hub-owned code may deserialize them into local core UI types, but external clients are not required to compile those internal UI/runtime dependencies.

## Isolated Integration Tests For External Clients

External clients that need a true live-hub integration test should depend on the
client protocol crate plus the test-support crate, not on the full `botster-hub`
library. Until these crates are published to crates.io, the supported
out-of-repo dependency shape is a git dependency pinned to the same repository
revision for both crates:

```toml
[dev-dependencies]
botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-client", rev = "<hub-rev>" }
botster-hub-test-support = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-test-support", rev = "<hub-rev>" }
```

The harness starts the `botster-hub` binary as a subprocess and talks to it
through `botster-hub-client`. It does not compile or link hub runtime, TUI, Lua,
or plugin internals into the downstream client.

Build or otherwise provide both binaries before running the downstream test. The
fixture does not provision binaries itself; third-party CI should either build
them from the same checkout/revision or download a release artifact that matches
the crate revision under test.

```bash
BOTSTER_ENV=test cargo build --locked --bin botster-hub
BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker
```

Then pass explicit paths into the harness. Environment variables are accepted as
a convenience, but the library never relies on `CARGO_BIN_EXE_botster-hub`
internally because Cargo only injects that variable for the package that owns the
binary.

```rust
use botster_hub_test_support::{run_client_conformance, IsolatedHubBuilder};

let hub = IsolatedHubBuilder::new()
    .hub_bin(std::env::var("BOTSTER_HUB_BIN").expect("BOTSTER_HUB_BIN"))
    .session_worker_bin(
        std::env::var("BOTSTER_SESSION_WORKER_BIN").expect("BOTSTER_SESSION_WORKER_BIN"),
    )
    .name("my-client-test")
    .start()
    .expect("isolated hub starts");

let report = run_client_conformance(&hub).expect("client conformance");
assert_eq!(report.lifecycle_state, "running");
assert_eq!(report.initial_session_count, 0);
assert!(report.stream_contains_ready);
assert!(report.stream_contains_echo);
assert!(report.stream_contains_resize);
assert_eq!(report.validation_error_operation, "drain_runtime");
hub.shutdown().expect("shutdown isolated hub");
```

Each harness instance creates a disposable data directory and socket path under
the configured test root, uses synthetic default hub identity, and attempts a
daemon shutdown on drop with a kill fallback for failed tests. Tests should still
call `shutdown()` explicitly when they need teardown failures to be visible.

`run_client_conformance` returns a stable report instead of raw event streams.
It covers status, empty session list, spawn, terminal attach/drain through
`stream_attach`, input echo, resize observation through `stty size`, a missing
session validation error, and teardown. Downstream CI can run it twice against
two fresh isolated hubs and compare the reports to prove deterministic fixture
output.

If a downstream client also wants to prove plugin surface/action dispatch
against the first-party Project Pipelines example, provide a checkout path to
the example package and call the optional helper:

```rust
let plugin_report = botster_hub_test_support::run_project_pipelines_conformance(
    &hub,
    "examples/project-pipelines",
)
.expect("project pipelines conformance");
assert_eq!(plugin_report.package_state, "enabled");
assert_eq!(plugin_report.surface_id, "project-pipelines-create-panel");
assert_eq!(plugin_report.invalid_action_status, "failure");
assert_eq!(plugin_report.invalid_title_error, "Title is required");
```
