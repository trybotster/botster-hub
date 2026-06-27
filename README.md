# botster-hub

`botster-hub` is Botster's trusted first-party host profile over
`botster-core`.

The profile owns trusted startup composition, policy, and host integration. It
does not fork or replace core runtime mechanics, and it does not implement every
provider itself; cloud federation, signaling relays, browser shells, and API
integrations belong in installable providers that declare capabilities through
`botster-core` package contracts.

The accepted boundary model is documented in
[`docs/adr/hub-as-host-profile-over-core.md`](docs/adr/hub-as-host-profile-over-core.md):
the hub is a first-party host profile/plugin bundle over `botster-core`, not a
thick wrapper.

## Responsibility split

| Layer | Owns |
| --- | --- |
| `botster-core` | Policy-free reusable local engine mechanics and transport-neutral primitives: session spawning, PTY/process mechanics, lifecycle, activity, fanout, `TransportIngress`/`TransportEgress`, `SessionIo`, client stream contracts, notifications, plugin worker primitives, reusable crypto/identity mechanisms, package manifests, `Capability`, `CapabilitySurface`, host-profile admission contracts, and capability runtime primitives. |
| `botster-hub` | Trusted first-party host profile policy over core contracts: config locations, persistence policy, auth hooks, startup composition, admission/enforcement, package install/enable/pin/update policy, lifecycle ordering, timeout/failure policy, and audit hooks. |
| CLI | Thin operator entrypoints that start, discover, or attach to a hub without owning profile policy. |
| Clients | Browser, TUI, socket, and custom renderers that consume hub contracts. Clients do not own provider behavior. |
| Plugins/providers | Installable behavior packages that declare capabilities, compatibility, entrypoints, provenance, checksums, enabled state, and update policy. |
| External provider implementations | Cloud federation, signaling relay, browser shell, API, and other privileged integrations implemented outside the hub crate. |

The host profile consumes `botster-core` through the typed core daemon API for
the production local session path. `botster-core-daemon` owns the durable
session registry metadata, worker-backed session supervision, PTY/process
mechanics, subscription fanout, readiness-gated writes, adoption primitives, and
delivery-state transitions. `HubRuntime` is the hub-owned host-profile facade
over that daemon plus hub package, lifecycle, and capability policy; it is not a
replacement runtime engine and it does not own live production PTY handles.

## HubRuntime facade audit

The hub exposes explicit methods where the operation is host-policy,
admission, scheduling, or visibility adjacent. It hides generic core routers and
keeps byte routing, PTY/session mechanics, fanout, plugin workers, capability
surfaces, and transport contracts in `botster-core`.
Client admission is host-profile policy over core admission and transport
contracts, not a hub replacement for `TransportIngress`, `TransportEgress`,
`SessionIo`, or client stream contracts.

`HubClientApi` is the stable local client API boundary over this facade. It is
transport-neutral and currently exercised in-process; socket, CLI, TUI, or local
browser bridge adapters should frame the same request/response/event contract
instead of bypassing hub admission or calling core routers directly. Attach is a
subscription handshake only, so clients still explicitly pull status, packages,
lifecycle status, or sessions when they need them. Hub code may start or embed
the typed core daemon API; it must not shell out to the core daemon CLI or parse
CLI output for session routing. Screen and snapshot requests return a typed
unsupported response until the daemon-backed core API exposes those operations.

| Core operation | HubRuntime decision | Reason |
| --- | --- | --- |
| `execute_command(DefaultEngineCommand)` | Hidden | A generic command router would obscure hub admission and policy boundaries. |
| `list_sessions` | Exposed | Host visibility over daemon-recorded sessions. |
| `spawn_session` | Exposed | Host-admitted local session creation through the core daemon. |
| `attach_client` | Exposed | Explicit client subscription handshake without global state hydration. |
| `detach_client` | Exposed | Explicit client subscription teardown through the core daemon. |
| `write_bytes` | Exposed | Explicit client terminal input path through the core daemon. |
| `resize` | Exposed | Explicit client terminal resize path through the core daemon. |
| `guarded_write` | Exposed | Hub admits the package/provider request, then core daemon owns readiness and delivery states. |
| `release_sessions_for_restart` / `adoption_scan` / `adopt_session` | Exposed | Explicit daemon restart/adoption controls over worker-backed core sessions. |
| `read_screen` / `capture_snapshot` / `report_delivery_*` | Deferred | Daemon-backed core API does not expose these embedded-engine-only helpers yet. |
| `PluginCapabilityRuntime::submit` | Exposed | Hub owns concrete local capability policy and submits through core request contracts. |
| `PluginCapabilityRuntime::drain_events` | Exposed | Plugin capability completions and timer events are drained through a hub-owned path. |
| `PluginCapabilityRuntime::cleanup_plugin` | Exposed | Capability resources are released during hub plugin reload and unload. |

## Local capability runtimes

`HubRuntime` owns the local concrete capability adapter for dogfood plugins. It
accepts `botster-core` `CapabilityRuntimeRequest` values through
`submit_capability_request`, returns core `CapabilityRuntimeHandle` values, and
drains core `CapabilityRuntimeEvent` values through `drain_capability_events`.
The hub adapter implements scoped filesystem operations, plugin JSON store
operations, logical timers, policy-gated HTTP execution, and core's in-memory
WebSocket runtime. It does not add product cloud, public WebRTC, webhook, OAuth,
Rails, or provider-specific API behavior.

Filesystem access is rooted under the explicit hub data directory at
`capability-scopes/workspace`. Plugin store data is rooted under
`plugin-data/<plugin>/`, with `project-pipelines` as the first dogfood namespace
grant. Runtime data must not be written under plugin source directories.
Capability grants are scoped to match core request requirements exactly:
`Network:http`, `Network:websocket`, `Filesystem:workspace`,
`PluginDb:project-pipelines`, and `Timers:callbacks`.

HTTP requests are admitted through the core capability runtime and then executed
by the hub transport only when the URL scheme, host, method, headers, body size,
response size, header limits, and timeout policy pass. The default policy allows
loopback HTTP/HTTPS hosts for local dogfood plugins, `GET` and `POST`, and a
small safe request-header allowlist. Sensitive request headers such as
authorization and cookie headers are denied without echoing their values in
capability failure events. Deterministic fake HTTP should be injected only by
tests that explicitly configure a fake transport; the default `HubRuntime` path
performs real admitted HTTP I/O.

Filesystem, plugin-store, and HTTP work is accepted through the hub capability
path and completed on runtime-owned worker threads. Plugin unload and reload call
capability cleanup in addition to core plugin worker cleanup so timer and network
resources do not survive replacement.

## Crate layout

```text
src/lib.rs                 public facade over runtime and profile metadata
src/client_api.rs          transport-neutral local client request/response/event API
src/profile.rs             first-party host profile manifest and policy metadata
src/main.rs                thin binary smoke path through the profile facade
src/config.rs              hub-owned config policy seam
src/daemon.rs              deterministic local daemon lifecycle over runtime/state
src/persistence.rs         hub-owned persistence policy seam
src/auth.rs                hub-owned auth hook seam
src/packages.rs            hub package policy over core package contracts
src/lifecycle.rs           hub package lifecycle adapter over core plugin workers
src/capabilities.rs        hub-owned local capability runtime policy
src/runtime.rs             hub runtime facade over botster-core-daemon
examples/project-pipelines/plugin.lua
                          first Project Pipelines Lua workflow plugin source
```

This scaffold is intentionally shallow. The module tree makes the intended
ownership boundaries compile-checked, but it is not a final API freeze and does
not add a physical multi-crate split.

## Scaffold-only exclusions

This repo does not yet implement Rails, TryBotster Cloud, ActionCable, WebRTC,
signaling servers, browser shells, API clients, OAuth/device-code flows,
provider processes, persistence databases, marketplace fetches, package
installers, or client transports. The hub does include local file-backed
durable state for dogfood; database-backed persistence and cloud sync remain
excluded.

The exception in this scaffold is the constrained `examples/project-pipelines`
local plugin package. The daemon loads that package through the real Lua plugin
runtime; its entrypoint registers Project Pipelines MCP descriptors and workflow
handlers with the Lua ABI. MCP tools are registered through the shared
`mcp-serve` registry, dispatched over daemon transport to the owner thread,
invoked through `PluginWorkerEngine`, and persisted through the PluginDb
capability under `plugin-data/project-pipelines/`. The plugin README names
unsupported monolith features and the no-in-flight-monolith-ticket cutover
posture.

## Durable hub state

`FileHubStateStore` persists versioned local state at
`<HubConfig.data_directory>/hub-state.json`. The v1 state model records host
identity, config/schema metadata, package/provider registry snapshots,
capability grants, package admission decisions, enabled/disabled/pinned state,
provenance/checksum/update policy fields, local runtime settings, and audit
history.

The durable local startup path is explicit:

```sh
cargo run -- start --data-dir target/botster-hub-daemon-smoke-data
```

`start --data-dir` constructs `HubDaemon`, loads or initializes
`hub-state.json`, restores package/provider policy records through
`PackageRegistrySnapshot` admission, initializes `HubRuntime` through the
worker-backed core daemon facade, binds the configured local Unix socket, and
stays running until `shutdown --data-dir` asks it to stop. Later operator CLI
invocations connect to that socket with a `hello` / `hello_ack` protocol
handshake before sending daemon requests. Future transports, provider runtimes,
sockets, and supervisors should attach after this lifecycle object has started;
they should not recreate config or durable state ownership.

The no-arg binary path is a side-effect-light host-profile summary. It builds
resolved config and an in-memory `HubRuntime::new` summary only; it does not
load or save `hub-state.json` through HOME/XDG fallback paths. `run-one` remains
an explicit-data-dir runtime smoke path through `HubRuntime::load`. Registry,
grant, and admission mutation saves are now exercised by the local operator CLI
package commands through `HubStateStore::update`.

## Local dogfood operator CLI

The `botster-hub` binary includes a deliberately thin local operator surface for
dogfood. `start --data-dir` owns the daemon lifecycle and one `HubRuntime`;
`status`, `sessions list`, `sessions spawn`, `sessions attach`,
`sessions send-input`, `sessions resize`, `sessions detach`, and `shutdown`
connect to that daemon over the resolved local socket. The CLI remains a thin
adapter: daemon requests still route through `HubClientApi` instead of raw core
routers, and the daemon stamps runtime clocks for separate stateless client
invocations. Package state persists through `hub-state.json`, core registry
metadata persists under the hub data directory, and live worker-backed sessions
can be adopted after an intentional daemon restart.

The end-to-end local dogfood proof is the Unix integration flow below:

```sh
./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down
./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown
./test.sh --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport
```

The first test proves the production `botster-hub dogfood` entrypoint: it starts
an isolated daemon/session-worker subprocess, enables the checked-in
`examples/project-pipelines` package plus a supplied local `botster-web` package
through the daemon-owned package registry, supervises the `botster-web`
`web-client` entrypoint in existing-hub attach mode, prints the bridge URL plus
TUI/MCP/status/shutdown next steps, and shuts down through the daemon socket. The library-level test is
the documented proof path for the lower scaffold. It starts an
explicit `HubDaemon` with durable state, installs and enables the checked-in
`examples/synthetic-plugin` fixture, persists and reloads `hub-state.json`,
pulls status/package/lifecycle state through `HubClientApi`, resolves the
package's Lua entrypoint path, loads the package, invokes a synthetic in-process
plugin runtime through `HubRuntime`, spawns a local PTY session, attaches a
client, sends input, drains the observed marker, and shuts down through the same
local client API. Separate Lua runtime tests cover real Lua entrypoint
execution. The PTY portion is Unix-only.

For daily first-party plugin dogfood from fresh main checkouts, keep local
`botster-web` and `botster-tui` checkouts beside this repo or pass their paths
explicitly. Use a stable data directory when you want the package installs and
app registry to survive across reruns:

```sh
cargo run -- dogfood \
  --data-dir target/botster-hub-client-dogfood-data \
  --web-package-path ../botster-web \
  --tui-package-path ../botster-tui
```

The launcher uses an isolated data directory under `target/` by default. Pass
`--data-dir <path>` when you want state to survive across runs. It locates a
co-located `botster-session-worker` next to the current `botster-hub` binary, or
you can pass `--session-worker-bin <path>` explicitly. Pass
`--web-package-path <path>` to the first-party `botster-web` checkout; pass
`--tui-package-path <path>` when you also want to enable a local `botster-tui`
terminal app package. The launcher enables those packages, starts `web-client`
through daemon supervision, passes the dogfood hub socket as
`BOTSTER_HUB_SOCKET`, waits for `/health` to report `existing_hub` from
`socket`, and then prints the exact commands for the current data directory:

```sh
web=http://127.0.0.1:41739/?dogfood=real-hub
tui=botster-hub apps open --data-dir <path> botster-tui
mcp=botster-hub mcp-serve --data-dir <path>
status=botster-hub status --data-dir <path>
shutdown=run botster-hub shutdown --data-dir <path>
```

The supervised `botster-web` process receives `BOTSTER_HUB_SOCKET` because the
launcher owns that child process. Foreground terminal apps run through
`botster-hub apps open`, which asks the daemon for the resolved launch contract
and then starts the child with inherited stdio.

From another terminal, the composed local client app path should be visible
through the same stable data directory:

```sh
botster-hub apps list --data-dir target/botster-hub-client-dogfood-data
botster-hub apps show --data-dir target/botster-hub-client-dogfood-data botster-web/web-client
botster-hub apps open --data-dir target/botster-hub-client-dogfood-data botster-web/web-client
botster-hub apps open --data-dir target/botster-hub-client-dogfood-data botster-tui
botster-hub tui --data-dir target/botster-hub-client-dogfood-data
```

`apps open botster-web/web-client` reports an `app_url=` matching the printed
`web=` URL. `apps open botster-tui` and the deprecated `botster-hub tui` alias
both use the daemon-resolved `terminal_app` foreground launch contract; there is
no standalone fallback when the package is missing or disabled.

Keep the launcher running in the foreground. For graceful shutdown, run the
printed shutdown command from another terminal; `Ctrl-C` hard-stops the
foreground launcher. Shutdown remains hub-owned: the existing-hub bridge does
not stop the daemon it attached to, but daemon supervision stops the
`botster-web` process.

The CLI commands below exercise the daemon-backed workflow across separate
processes:

```sh
# Terminal 1: leave the daemon running.
cargo run -- start --data-dir target/botster-hub-dogfood-data

# Other terminals:
cargo run -- status --data-dir target/botster-hub-dogfood-data

cargo run -- packages install --data-dir target/botster-hub-dogfood-data \
  --path examples/synthetic-plugin
cargo run -- packages list --data-dir target/botster-hub-dogfood-data
cargo run -- packages show --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin
cargo run -- packages enable --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin
cargo run -- packages check-update --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin
cargo run -- packages preview-update --data-dir target/botster-hub-dogfood-data \
  dogfood.synthetic-plugin --revision v1.0.1 --policy manual
cargo run -- packages apply-update --data-dir target/botster-hub-dogfood-data \
  dogfood.synthetic-plugin --revision v1.0.1 --checksum sha256:example --policy manual
cargo run -- packages disable --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin
cargo run -- packages remove --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin
cargo run -- providers list --data-dir target/botster-hub-dogfood-data

cargo run -- apps list --data-dir target/botster-hub-dogfood-data
cargo run -- apps show --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin/web
cargo run -- apps open --data-dir target/botster-hub-dogfood-data dogfood.synthetic-plugin/web

cargo run -- sessions spawn --data-dir target/botster-hub-dogfood-data \
  --session-id dogfood-session -- "printf 'dogfood-ok\n'; sleep 1"
cargo run -- sessions list --data-dir target/botster-hub-dogfood-data
cargo run -- sessions attach --data-dir target/botster-hub-dogfood-data dogfood-session
cargo run -- sessions send-input --data-dir target/botster-hub-dogfood-data \
  dogfood-session -- "ping\r"
cargo run -- sessions resize --data-dir target/botster-hub-dogfood-data \
  dogfood-session 30 100
cargo run -- sessions detach --data-dir target/botster-hub-dogfood-data dogfood-session
cargo run -- sessions shutdown --data-dir target/botster-hub-dogfood-data dogfood-session
cargo run -- inspect --data-dir target/botster-hub-dogfood-data dogfood-session
cargo run -- shutdown --data-dir target/botster-hub-dogfood-data
```

`packages install --path` connects to the running daemon, validates the local
package manifest through the existing hub package registry policy, persists it
as installed but disabled under `hub-state.json`, and then lists packages from
the daemon's refreshed in-memory registry. `packages show`, `packages enable`,
`packages disable`, `packages remove`, `packages list`, and `providers list` use
the same daemon-backed registry view. `packages enable --path` remains available
as a convenience that installs and enables in one daemon-owned mutation.
`packages check-update`, `packages preview-update`, and `packages apply-update`
also route through the daemon. They report structured unavailable diagnostics
for unsupported update/reload paths and `apply-update` records pin/checksum
metadata without fetching package code, starting entrypoints, or restarting the
hub. The
session commands also use the running daemon runtime, so a session created by
one CLI process is visible to later `sessions list`, `sessions attach`,
`sessions send-input`, `sessions resize`, `sessions detach`, and `sessions
shutdown` invocations.
`attach` streams terminal bytes and currently exits after an idle window if the
core runtime does not provide a process-exit frame. `inspect` is intentionally
scoped to sanitized session list data until the stable client API grows a
dedicated inspection request.

## Standalone local TUI

When a `botster-tui` terminal app package is installed and enabled,
`botster-hub apps open --data-dir <path> botster-tui` opens the local terminal
UI over the same daemon socket and `botster-hub-client` protocol path as the
operator CLI. The hub resolves the foreground command, working directory, and
allowlisted environment; the CLI only spawns that contract with inherited stdio.
`botster-hub tui --data-dir <path>` is a deprecated compatibility alias for that
apps command and reports clearly when `botster-tui` is not installed/enabled.

```sh
# Terminal 1: leave the daemon running.
cargo run -- start --data-dir target/botster-hub-tui-dogfood-data

# Terminal 2: create a deterministic echo-loop session for typed-input testing.
cargo run -- sessions spawn --data-dir target/botster-hub-tui-dogfood-data \
  --session-id dogfood-session -- "printf 'dogfood-ok\n'; while IFS= read -r line; do printf 'dogfood:%s\n' \"$line\"; done"

# Terminal 3: operate the session from the installed terminal app.
botster-hub apps open --data-dir target/botster-hub-tui-dogfood-data botster-tui
```

The echo-loop fixture is intentionally not a shell: typing `hello` should produce
`dogfood:hello`, while commands such as `ls` are just echoed back. For shell
commands, spawn a separate long-lived shell session and attach to that session
from the TUI:

```sh
cargo run -- sessions spawn --data-dir target/botster-hub-tui-dogfood-data \
  --session-id dogfood-shell -- "/bin/sh -i"
```

The standalone TUI lists daemon sessions, attaches with a persistent socket
subscription, sends ordinary typed input to the active PTY, forwards terminal
resize events, detaches and reattaches with fresh subscription ids, shuts down
sessions, and can request daemon shutdown. On daemon socket loss it shows a
reconnecting state, reconnects to the daemon, refreshes the session list, drops
the stale subscription id, and reattaches when the worker-backed session is
recovered.
When recovery is absent, it leaves the operator in the session/status view with
a visible session-lost error.

Visible key hints are shown in the TUI: `Enter` attaches, typed input sends only
after attach, `Esc`/`Ctrl-D` detaches, `Ctrl-Q` quits, `Ctrl-N` sends a doorbell
that may defer, `Ctrl-S` shuts down the selected session, and `Ctrl-X` requests
daemon shutdown.

Doorbell notifications use the daemon `NotifySession` request, the same native
coordination path exposed through MCP as `notify_session`. The current daemon
socket surface does not expose observed mode/screen readiness yet, so the
production TUI fails closed and reports the deferred guarded-write decision
rather than fabricating `SafeWriteIndicator::Safe`. A future delivered doorbell
must be driven by observed session readiness and should prove both a delivered
case and a deferred/rejected unsafe case. The v1 activity row is a client-rendered
guarded-write status row, not a separate routed notification event from core.

Package commands require the daemon socket for this local dogfood path. When
the daemon is not running they fail with `daemon not running` instead of
mutating `hub-state.json` out of band. That keeps package/provider state,
daemon-backed status, and plugin lifecycle reads on one control plane.

## Agent-facing MCP stdio

Local agents can launch the daemon-backed MCP surface with an explicit data
directory:

```sh
botster-hub mcp-serve --data-dir target/botster-hub-dogfood-data
```

`mcp-serve` speaks MCP over stdio as newline-delimited JSON-RPC: every stdout
line is one protocol message, and the command does not use `Content-Length`
framing. Process diagnostics belong on stderr so agent clients can treat stdout
as the protocol stream.

Native tools route through the running daemon, not directly into hub state:

- `hub.status` returns sanitized daemon status through
  `daemon_transport_request -> serve_daemon -> HubClientApi -> HubRuntime`.
- `hub.sessions.list` returns sanitized session ids and lifecycle labels through
  the same daemon/client path.
- `whoami` reports the local MCP identity available to native tools. When
  `BOTSTER_SESSION_UUID` is present it is reported as the caller session.
- `post_message` and `post_envelope` publish a text payload as a core routed
  envelope to one target session.
- `receive_messages` and `receive_envelopes` drain only the caller session route
  from `BOTSTER_SESSION_UUID`; they do not accept another session id or agent id.
- `ack_message` and `ack_envelope` acknowledge one delivered caller-scoped
  envelope.
- `notify_session` is a guarded-write doorbell attempt. The current native MCP
  surface does not yet gather terminal readiness evidence from attached clients,
  so it reports core's guarded-write decision and can defer instead of injecting
  bytes. That result is separate from routed-envelope inbox, cursor, and ack
  semantics.

The message/envelope tools use
`daemon_transport_request -> serve_daemon -> HubClientApi -> HubRuntime ->
CoreDaemon::{publish,drain,acknowledge}_routed_envelope`. Core assigns routed
envelope cursors in memory; the current hub surface reports the cursor returned
by core but does not claim restart-durable inbox state.

Tool listing and calling both route through `McpToolRegistry`. Native hub tools
are provided by `NativeHubToolProvider`; Lua plugin tools use
`PluginHubToolProvider` descriptors and owned daemon call messages on the same
registry path. Plugin execution is dispatched through the plugin
worker/supervisor boundary instead of creating a second MCP server or direct
in-process closure path.

The native local coordination path uses no Lua or plugin tool execution:
`whoami`, `post_message`, `receive_messages`, `ack_message`, and
`notify_session` are native hub tools even when the binary also has the Lua
plugin runtime available. Project Pipelines separately composes the same
routed-envelope primitives from `examples/project-pipelines/plugin.lua` through
the Lua ABI.

## Project Pipelines Local Readiness

The checked-in `examples/project-pipelines` package is ready for constrained
daily local coordination dogfood. Prefer the single-command launcher:

```sh
cargo run -- dogfood
```

For lower-level diagnostics, enable it through a running daemon and serve MCP
from the same data directory:

```sh
cargo run -- packages install --data-dir target/botster-hub-dogfood-data \
  --path examples/project-pipelines
cargo run -- packages enable --data-dir target/botster-hub-dogfood-data project-pipelines
cargo run -- mcp-serve --data-dir target/botster-hub-dogfood-data
```

`mcp-serve` lists and calls the plugin's Project Pipelines tools through
`PluginHubToolProvider -> daemon request -> HubRuntime -> HubPluginLifecycle ->
PluginWorkerEngine -> LuaPluginRuntime`. `project_pipelines.start` requires an
explicit `target_id` and assigned worktree and records primitive-backed
coordination evidence on the run: request id, agent name, owner plugin, routed
envelope id, publish delivery status, drain cursor, and acknowledge delivery
status. `session_uuid` is intentionally absent in this constrained local flow
because the plugin records coordination before spawning an agent session.

Project Pipelines state persists through PluginDb under
`plugin-data/project-pipelines/`, not a host-supplied runtime bundle or plugin
source directory. After an intentional daemon restart over the same data
directory, package state reloads, Project Pipelines MCP tools re-register, and
persisted tickets, runs, gates, and events remain visible through
`project_pipelines.current_context`.

Secrets are not imported or persisted by this readiness proof. Operators should
re-enter any provider credentials needed by deferred provider integrations when
those integrations land. Live monolith Project Pipelines data is not imported in
this milestone; cutover requires no in-flight monolith tickets or a future
explicit one-shot export/import before switching active work to the local
plugin.

Dogfood-ready today: explicit local daemon lifecycle, file-backed hub/package
state, local package admission from a manifest path, typed status/package reads,
plugin lifecycle observation/invocation through the hub facade, daemon-backed
PTY spawn/list/attach/input/resize/detach/session-shutdown through
`HubClientApi`, and cross-process daemon transport proof for hub restart
recovery.

The production-shaped restart proof lives in `hub_daemon_lifecycle_test`: it
starts the `botster-hub` binary, spawns a long-running worker-backed session
over daemon transport, stops the hub daemon process through `shutdown
--data-dir`, starts the binary again over the same explicit data directory,
observes startup `recovered_sessions`, lists the same session, reattaches,
sends input, drains post-restart output, shuts down the session, and stops the
daemon. The lower-level in-process restart test remains contract coverage for
`HubClientApi`.

Startup reconciliation is deterministic: registry records with missing protocol
evidence, missing workers, unhealthy workers, or duplicate candidates are marked
stale; terminal records remain terminal; and live worker-backed records absent
from hub-owned state are recovered from core daemon/session-worker evidence.
The readiness boundary is documented in
[`docs/adr/local-runtime-dogfood-readiness.md`](docs/adr/local-runtime-dogfood-readiness.md).

In the daemon-backed model, attach, detach, input, and resize requests are
control-plane acknowledgements. Terminal egress is delivered by explicit
`DrainRuntime` calls over the session-backed CoreDaemon path, not synchronously
from those control operations.

Ready for daily local use today: explicit daemon lifecycle, daemon-backed local
PTY session operations, minimal daemon-backed TUI attach/reconnect, native MCP
coordination tools, and constrained Project Pipelines MCP workflow tools over
the Lua plugin runtime.

Feature parity still pending: durable PTY recovery after daemon exit, provider
process supervision, GitHub/PR automation, install/update packaging,
cloud/Rails/WebRTC/browser/marketplace surfaces, broad migration compatibility
from the monolith, missing-public-socket self-heal after the socket path is
externally removed, long-running attach signal handling, and uncoordinated crash
PTY recovery.

Schema and consistency posture are documented in
[`docs/adr/durable-hub-state-v1.md`](docs/adr/durable-hub-state-v1.md).

## Dependency policy

During development, this scaffold tracks `botster-core` from the `main` branch
declared in `Cargo.toml`, with the resolved git revision committed in
`Cargo.lock` for reproducibility. Refresh the lockfile intentionally when hub
work needs current core behavior; stale lock drift is not a separate pinning
policy.

Release builds should move to a deliberate `botster-core` tag or revision pin
before shipping. Local `path` overrides are not the repo default and should stay
outside committed dependency policy unless the repo grows an explicit override
workflow.

The production local session path uses `botster-core-daemon` through typed Rust
APIs and configures the sibling `botster-session-worker` executable for
worker-backed sessions. Keep core default features enabled so
daemon/session-worker mechanics can use the local runtime contracts. Do not
route hub session control through the thin core daemon CLI.

## Runtime smoke proof

The production binary includes a deliberately thin smoke entrypoint:

```sh
cargo run -- run-one --data-dir target/botster-hub-smoke-data -- /bin/sh -c "printf 'botster-hub-smoke-ok\n'"
```

`run-one` requires an explicit `--data-dir`, builds hub config without falling
back to user paths, then crosses `HubRuntime -> CoreDaemon -> botster-session-worker`
through spawn, attach, resize, drain, marker observation, detach, and shutdown.
Its output is scrubbed to profile, host, session, marker, byte-count, and
daemon-path facts so pipeline artifacts do not need local paths, environment
dumps, keys, or fingerprints.

The in-process `HubClientApi` local dogfood workflow supports status, session
list, spawn, attach, input, resize, drain/output events, shutdown, guarded
notification write, package queries, and plugin lifecycle status. Browser, TUI,
socket, WebRTC, and cloud transports remain future adapters over this same local
API; they are not implemented by the smoke command.

## Package registry policy

`default_package_policy()` is the production-facing hub-owned package policy
surface. It builds a `PackageAdmissionPolicy` from `host_profile()` default
capability grants, then stores in-memory package records around
`botster_core::PackageManifest` values through `PackageRegistry`. The registry
keeps enabled/disabled state, records provenance/checksum and pin/update
metadata, classifies providers from `botster_core::ExtensionKind`, persists a
hub-owned trust marker, records admitted capabilities after enablement, records
the narrow Botster compatibility result/diagnostics, and validates enable
decisions against the profile-derived hub grant set.

The registry deliberately uses core package contracts instead of defining a hub
manifest or capability vocabulary. Provider packages must carry host-profile
metadata before they can be enabled; provider host-profile packages are admitted
through `botster_core::admit_host_profile`, so core still owns the narrow
manifest/admission preconditions while hub owns install, enable, disable, pin,
grant, provenance, update, and audit policy.

Local dogfood installs accept either an explicit JSON manifest path or a package
directory containing `botster-package.json`. The file is parsed as
`botster_core::PackageManifest` plus the hub-owned `runnable_entrypoints`
extension; the hub rewrites the manifest source to `PackageSource::Path` with
the canonical package root, records
`local:<canonical-package-root>` provenance, marks the record as
`local_development` trust, and rejects absolute, traversing, or symlink-escaped
entrypoints before registry mutation. Enabled local records can be prepared into
canonical entrypoint paths for the core lifecycle adapter.

`entrypoints` remains the core plugin/provider code-load contract. Runnable
local/dev process declarations live under `runnable_entrypoints` so clients,
launchers, and future marketplace tooling share one discovery shape without
changing plugin loading semantics. Each runnable entrypoint declares a stable
`id`, `kind` (`client`, `web`, `mcp`, `daemon`, or `provider`), `command`,
`args`, `working_directory`, declarative `environment` requirements, `mode`
(`dev` or `local`), capability needs, `may_supervise`, and a static process DTO.
Current process state is always `not_started`; this slice does not spawn,
supervise, restart, or health-check runnable entrypoints.

Local path manifest example:

```json
{
  "name": "dogfood.synthetic-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": { "type": "path", "path": "." },
  "capabilities": [
    { "surface": "mcp" },
    { "surface": "timers", "scope": "callbacks" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ],
  "runnable_entrypoints": [
    {
      "id": "web-client",
      "kind": "web",
      "command": "bin/botster-web",
      "args": ["--host", "127.0.0.1"],
      "working_directory": { "policy": "package_root" },
      "environment": [
        {
          "name": "BOTSTER_WEB_PORT",
          "required": false,
          "default": "5173",
          "description": "Local botster-web dev server port"
        }
      ],
      "mode": "dev",
      "capabilities": [
        { "surface": "network", "scope": "localhost" }
      ],
      "may_supervise": true
    }
  ]
}
```

Packages can also declare hub-owned `session_templates` for PTY session launches
with trusted context injection:

```json
{
  "session_templates": [
    {
      "id": "init",
      "command": "bin/init.sh",
      "working_directory": { "policy": "package_root" },
      "environment": { "BOTSTER_MODE": "default" },
      "allowed_environment_overrides": ["BOTSTER_MODE"],
      "context": ["prompt", "ticket_id"]
    }
  ]
}
```

The hub validates templates and materializes them into generic core spawn
requests. Core remains unaware of template ids, Project Pipelines, Codex,
Claude, agents, tickets, workspaces, and `botster context`. Explicit spawn
overrides are admitted only when the template and target policy allow them.
Spawned scripts receive `BOTSTER_SESSION_ID`, `BOTSTER_CONTEXT_ID`,
`BOTSTER_HUB_DATA_DIR`, `BOTSTER_HUB_SOCKET`, and `BOTSTER_HUB_BIN`, and can read
context with `"$BOTSTER_HUB_BIN" context --key prompt`.

Git-source manifests use the same core shape. The registry can persist the Git
URL/reference, provenance, pin revision/checksum, compatibility result, trust
classification, and enabled/admitted-capability state, but this ticket does not
clone, fetch, update, or resolve network Git sources:

```json
{
  "name": "example.workflow-plugin",
  "version": "1.0.0",
  "kind": "plugin",
  "botster": ">=0.1.0",
  "source": {
    "type": "git",
    "repo": "https://example.invalid/botster/workflow-plugin.git",
    "reference": "v1.0.0"
  },
  "capabilities": [
    { "surface": "surfaces" }
  ],
  "entrypoints": [
    { "runtime": "lua", "path": "plugin.lua", "bootstrap": false }
  ]
}
```

Accepted and denied package decisions carry package name, action,
classification when known, prior or resulting state when known, typed policy
reason for denials, admitted host-profile metadata when present, and the audit
reason passed by the hub caller. The binary boot summary constructs the default
policy from the host profile so this policy path is reachable outside tests.

This is the policy gate future package lifecycle loading should call before
starting plugin/provider execution through core APIs. The hub runtime now loads,
invokes, reloads, and unloads enabled in-memory package records through
`botster-core` plugin worker mechanics, with host-supplied deterministic runtime
bundles. Package records persist through the canonical `HubState.package_registry`
snapshot inside `hub-state.json`; there is no separate package-state file for the
registry. The snapshot is the local lock state: installed source, provenance,
pin revision/checksum, update policy, trust classification, enabled state,
admitted capabilities, compatibility result/diagnostics, runnable entrypoint
contracts, optional install/update timestamps, and the latest audit reason.

Deferred production concerns remain outside this contract slice: signing,
sandboxing, dependency solving, installer-managed binaries, hosted marketplace
resolution, and production WebRTC launch paths.

Compatibility remains deliberately narrow in this slice. The manifest `botster`
field accepts only exact `MAJOR.MINOR.PATCH` or lower-bound
`>=MAJOR.MINOR.PATCH` requirements for the current hub binary. Broader semver
ranges, separate hub/core/client protocol windows, compatibility channels,
hosted registry indexes, signing, dependency solving, auto-update daemons,
publishing portals, network Git clone/fetch/update behavior, and binary/CLI
package-install commands remain excluded.

Compatibility and enabled capability admission fail closed. An incompatible or
invalid `botster` requirement is rejected at install; if a persisted package no
longer satisfies the current hub version or grant/surface policy during
`PackageRegistry::from_snapshot`, the registry load returns a typed error rather
than silently quarantining that single record. The persisted
`PackageCompatibility` value on accepted records is therefore the last accepted
compatible result; incompatible and invalid results are operator diagnostics on
the rejected install/reload path until package quarantine behavior exists.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
