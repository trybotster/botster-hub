# Botster Hub Client Protocol

The authoritative reusable client-to-hub daemon protocol lives in:

- `crates/botster-hub-client/src/lib.rs`
- `src/daemon_transport.rs`

Implementation baseline before this split: `9b39f1607144319138151cdf776e8909f35a63d4`. The pipeline implementation commit should be treated as the final protocol revision once merged.

External same-device clients should depend on the `botster-hub-client` crate and use `DaemonEndpoint`, `DaemonConnection`, `request`, or `stream_attach` to talk to a running `botster-hub` daemon socket. The crate owns the client-facing handshake, request, response, event, and JSON frame helpers.

## Compatibility Handshake

Clients should check hub compatibility before depending on request-specific
behavior. `DaemonConnection::connect`, `request`, and `stream_attach` perform
the current first-party compatibility check during the socket hello handshake.
The running hub also returns the same descriptor on `DaemonRequest::Status` so
operator UIs can show protocol diagnostics without opening a special endpoint.

The current descriptor includes:

- protocol name and version;
- supported features: sessions, terminal streaming, resize, plugin surface
  render, and plugin surface action dispatch;
- conformance fixture revision.

The hub-owned first-party support matrix lives in
`botster_hub_test_support::first_party_client_support_matrix`. It is a
serde-serializable test/docs contract that expands the compatibility descriptor
into the exact first-party client surface covered today: diagnostic kinds,
session actions, held-open terminal streaming, resize, Project Pipelines
surface/action dispatch, and known limitations. It is not a daemon runtime
endpoint.

Downstream clients with the same requirements as the current crate can rely on
the default connection helper. The checked example for this path lives on
`botster_hub_client::DaemonConnection`.

```rust
let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
    .map_err(|error| error.to_string())?;
```

Clients that need to declare stricter requirements should use the explicit
handshake helper and display the returned diagnostic as a connection/status
error. The checked examples for this path live on
`botster_hub_client::DaemonCompatibilityRequirement::current` and
`botster_hub_client::connect_and_hello_with_requirement`.

```rust
let mut requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
requirement.client_name = "botster-tui".to_string();
requirement
    .required_features
    .push("future_feature".to_string());

let stream = botster_hub_client::connect_and_hello_with_requirement(
    &endpoint,
    &requirement,
)
.map_err(|error| error.to_string())?;
```

`botster-tui` should run this check as part of its daemon connect/reconnect
path and render `DaemonTransportError::Compatibility` as the status panel
connection error instead of continuing into session or terminal operations.
`botster-web` should perform the same check in its local hub bridge/status path
before relying on sessions, terminal streaming, resize, or plugin surface/action
dispatch, and show the diagnostic in the hub connection state.

## Connection Diagnostics

The daemon protocol exposes policy-free diagnostics through stable
`DaemonDiagnostic` values. Clients should branch on `kind`, `operation`, and
`feature`, and treat `message` as optional operator detail rather than a parsing
contract.

Diagnostics are additive fields on `DaemonHelloAck`, `DaemonStatus`,
`DaemonResponse`, and `DaemonOperatorError`. Older responses that do not include
diagnostics still deserialize with empty diagnostic lists.

Current diagnostic kinds are:

- `connected` for successful hello/status/shutdown lifecycle checks;
- `compatibility_mismatch` for protocol, protocol-version, or conformance
  descriptor mismatch;
- `unsupported_feature` for missing handshake features or unsupported daemon
  operations;
- `terminal_stream_unavailable` when a terminal stream request has a distinct
  runtime signal such as missing session on attach/drain;
- `action_failure` when a plugin surface action returns a rejected or error
  result;
- `daemon_startup_failure` for startup failures reported by client/test-support
  helpers before a daemon socket protocol response can exist;
- `disconnected` for client-side transport disconnect classification.

Downstream clients should prefer the structured fields over private string
parsing:

```rust
let response = connection.request(&botster_hub_client::DaemonRequest::Status)?;
if response.diagnostics.iter().any(|diagnostic| {
    diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
}) {
    // Render connected state.
}
```

Compatibility errors also carry diagnostics:

```rust
match botster_hub_client::connect_and_hello_with_requirement(&endpoint, &requirement) {
    Err(botster_hub_client::DaemonTransportError::Compatibility(error)) => {
        for diagnostic in error.diagnostics {
            // Render compatibility_mismatch or unsupported_feature.
        }
    }
    other => other.map(drop)?,
}
```

Diagnostic messages intentionally avoid local data directories, socket paths,
raw worktree paths, and mutable Botster identity. First-party clients may add UI
severity or remediation copy, but that policy belongs in the client renderer,
not in the daemon protocol.

## Package Runnable Entrypoints

`DaemonPackage` rows include `runnable_entrypoints` for hub-owned local/dev
process contracts declared by installed packages. Entrypoints marked
`may_supervise` can be started, stopped, restarted, and inspected with
`StartPackageEntrypoint`, `StopPackageEntrypoint`,
`RestartPackageEntrypoint`, and `PackageEntrypointStatus` daemon requests.
Runtime process state is owned by the running hub daemon and is not persisted
into package registry state.

Each entrypoint exposes sanitized manifest declarations: `id`, `kind`,
`command`, `args`, `working_directory`, declarative `environment`
requirements, `mode`, capability needs, `may_supervise`, and process
diagnostics. Runtime process fields are additive: `pid`, `started_at`,
`exited_at`, and `exit_status` may be omitted when no supervised process state
exists. The daemon response must not expose the local package root, provenance
path, socket path, or host-resolved environment values. Environment defaults
are manifest-provided literals, not snapshots from the operator's machine.

Supervised entrypoints are local development processes, not a production
installer or sandbox. The daemon stops them on explicit stop/restart, package
disable/remove, `DaemonShutdown`, and daemon SIGINT/SIGTERM cleanup.

The runnable contract is intentionally adjacent to core package `entrypoints`.
Core `entrypoints` remain the plugin/provider code-load ABI, while
`runnable_entrypoints` is the package discovery shape for clients and future
launchers.

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
revision for both crates. Use one exact commit SHA for every Botster crate in
the downstream test so the client protocol crate, test harness crate, hub
binary, and session-worker binary all come from the same protocol revision:

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
the crate revision under test. Use `--locked` when building from source so the
hub's committed lockfile preserves the `botster-core` revision paired with that
hub checkout.

```bash
BOTSTER_ENV=test cargo build --locked --bin botster-hub
BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker
```

Then pass explicit paths into the harness. Environment variables are accepted as
a convenience, but the library never relies on `CARGO_BIN_EXE_botster-hub`
internally because Cargo only injects that variable for the package that owns the
binary. The compile-checked usage examples live on
`botster_hub_test_support::IsolatedHubBuilder`,
`botster_hub_test_support::run_client_conformance`, and
`botster_hub_test_support::run_project_pipelines_conformance`.

Each harness instance creates a disposable data directory and socket path under
the configured test root, uses synthetic default hub identity, and attempts a
daemon shutdown on drop with a kill fallback for failed tests. Tests should still
call `shutdown()` explicitly when they need teardown failures to be visible.

`run_client_conformance` returns a stable report instead of raw event streams.
It covers status, empty session list, spawn, terminal attach/drain through
`stream_attach`, input echo, resize observation through `stty size`, a missing
session validation error, connected diagnostics, terminal-unavailable
diagnostics, and teardown. Downstream CI can run it twice against two fresh
isolated hubs and compare the reports to prove deterministic fixture output.

Downstream `botster-tui` tests should import
`botster_hub_test_support::first_party_client_support_matrix` directly and
compare it to `run_client_conformance` for the local client paths they exercise.
Downstream `botster-web` tests should consume the matrix as serialized JSON,
for example with `serde_json::to_value(first_party_client_support_matrix())`
from a Rust fixture or repo sync step, rather than mirroring the matrix by hand
in TypeScript.

If a downstream client also wants to prove plugin surface/action dispatch
against the first-party Project Pipelines example, provide a checkout path to
the example package and call the optional `run_project_pipelines_conformance`
helper. Its report includes the rejected-action diagnostic for the invalid form
submission path.

The matrix currently marks JSON plugin surface render/action dispatch as
supported and full plugin entity-frame hydration as intentionally unsupported by
this conformance fixture. Clients that render plugin entity stores should prove
that path with their own entity-frame tests until the hub publishes a dedicated
entity conformance fixture.
