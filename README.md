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

## One production path

There is one local product topology. Docs, operators, and clients should start
here—not from parallel scaffold stories or in-process engine embeds:

**resolved data directory → `HubDaemon` / `HubRuntime` → `CoreDaemon` → `botster-session-worker`**

| Layer | Role on the product path |
| --- | --- |
| Hub host profile | Explicit data directory, package install/enable policy, capability grants, auth/admission, durable hub state, plugin lifecycle, CLI/MCP/socket adapters, and `HubClientApi`. |
| `botster_core_daemon::CoreDaemon` | Durable session registry, worker supervision, spawn/list/attach/detach/input/resize/drain, adoption/restart release, guarded-write readiness, and **routed-envelope coordination** (publish/drain/ack). |
| `botster-session-worker` | Owns live PTY handles so sessions can survive intentional hub daemon restart. |
| `HubRuntime` | Host-profile facade that composes CoreDaemon with hub package, lifecycle, and capability policy. Not a second runtime engine and not a second coordination bus. |
| `HubClientApi` | Stable local client request/response/event boundary over that facade (CLI, MCP, socket, TUI, and local browser bridge adapters frame this contract). |

`DefaultBotsterEngine` is a **core library embed** path for tests, toys, and
custom hosts that want in-process or worker-backed sessions without the hub
product stack. Hub production sessions do **not** re-embed
`DefaultBotsterEngine` as a parallel product runtime. Core documents that
library path separately; this hub documents only the product host path above.

### Start here

```sh
# Once per checkout: build the PTY worker that CoreDaemon supervises.
cargo build --locked -p botster-core --bin botster-session-worker

# One terminal: start or reuse Hub and attach its operator console.
cargo run
data_dir=resolved:$HOME/.botster/hub
daemon=started
botster-hub> packages install --path /path/to/botster-web
botster-hub> packages enable botster-web
botster-hub> packages install --path /path/to/botster-tui
botster-hub> packages enable botster-tui
botster-hub> up
botster-hub> status
botster-hub> apps list
botster-hub> sessions list
botster-hub> exit
```

`exit` and Ctrl-D detach without stopping Hub. Running bare `botster-hub` again
reuses the daemon; `down` deliberately stops it and exits the console. At an
idle prompt Ctrl-C cancels the current line and leaves the daemon and its
sessions running.

For a verified metadata-owned runtime, successful `shutdown` or `down` means
the recorded Hub PID is absent from the process table and the owned local socket
and runtime metadata file have been removed. Receiving the daemon's shutdown
response or observing an exited-but-unreaped zombie is not completion.
Completion combines the daemon's successful shutdown response with
independently observed PID absence; an owner may reap the child while reaching
that same process-table terminal state.

Bare invocation requires terminal stdin and stdout. Scripts and redirected
commands must use an explicit subcommand such as `botster-hub status`; they
fail clearly instead of entering a prompt.

Use `--data-dir <path>` on any stateful command when an isolated override is
needed. The selected directory owns `hub-state.json`, package registry state,
plugin data, and core daemon registry metadata.

### Coordination owner

After dual coordination kill, **CoreDaemon is the single product owner** of
hub-native routed envelopes and guarded notification writes:

- Native MCP/daemon tools: `HubClientApi` → `HubRuntime` →
  `CoreDaemon::{publish,drain,acknowledge}_routed_envelope` and
  `CoreDaemon::guarded_write`.
- Lua plugins: `botster.coordination.*` uses the same CoreDaemon instance through
  a narrow hub coordination bridge (no hub-local envelope inbox).
- There is no parallel hub-local `RoutedEnvelopeRouter` product path.
- Envelope queues are **process memory** in the hub/daemon process: they are not
  restart-durable. Worker-backed sessions and file-backed hub/package state are
  a different durability story.

### Session history, screen, and snapshot (product surfaces)

| Surface | Status on the product path |
| --- | --- |
| Attach + drain terminal egress | Product. Control ops ack through CoreDaemon; bytes arrive via drain/subscription. Late attach may replay prior output as Snapshot/Scrollback/TerminalOutput events when the worker path emits them. |
| Hub `ReadScreen` / `ReadModeFlags` / `CaptureSnapshot` | Product. Routes `HubClientApi` → `HubRuntime` → `CoreDaemon` readback. `ReadScreen` returns session text; targeted `ReadModeFlags` returns the authoritative session id and exact `mouse_mode: u8`; `CaptureSnapshot` returns metadata only (rows/cols/format/byte count). Errors stay errors rather than fabricated mouse-off results, and mode readback has no pushed event. Opaque snapshot bytes stay on the attach/drain data plane. Hub never decodes snapshot wire magic (`GHOSTSNP`) or asserts payload layout — cold cutover is enforced in core/restty, not hub. |
| Subscription history | Product. History and live terminal output flow through attach/drain events, not through readback responses. |
| `report_delivery_*` pressure helpers | Still unfinished. Not exposed on the hub client product surface yet. |

**Ghostty snapshot cutover:** locking hub to a core rev that emits
`ghostty-terminal-snapshot-v1` / `GHOSTSNP` does **not** prove end-to-end
restorability. Green hub CI only proves hub builds, opaque pass-through, and
metadata labeling. Restorability proof lives in core + restty (and updated
clients/WASM). Pre-cutover snapshot bytes are invalid against the new wire;
downstream clients must consume the post-cutover restty/WASM stack.

### Product today vs still unfinished

**Product on this path:** explicit data-dir daemon lifecycle; worker-backed
local PTY sessions (spawn/list/attach/input/resize/detach/session-shutdown);
attach/drain history; hub client screen, mode-flags, and snapshot readback through CoreDaemon;
package install/enable/disable/reload and entrypoint supervision for local
packages; `HubClientApi` + daemon socket protocol; native MCP coordination
tools; Lua plugin runtime including constrained Project Pipelines; local
capability runtimes; OS credential store for production secrets; durable
`hub-state.json` package/provider policy; intentional restart adoption of
worker-backed sessions.

**Still unfinished / out of product claims:** delivery-pressure reporting
(`report_delivery_*`); observed terminal readiness for always-delivered
doorbells; restart-durable routed-envelope inboxes; uncoordinated full
daemon/worker crash PTY recovery; marketplace fetch/update packaging UX;
provider process supervision; cloud/Rails/public WebRTC/browser shell as hub
builtins; broad monolith migration import.

## Responsibility split

| Layer | Owns |
| --- | --- |
| `botster-core` / `botster-core-daemon` | Policy-free reusable local engine mechanics and transport-neutral primitives: session spawning, PTY/process mechanics (via worker path), lifecycle, activity, fanout, `TransportIngress`/`TransportEgress`, `SessionIo`, client stream contracts, notifications, **routed-envelope coordination**, plugin worker primitives, reusable crypto/identity mechanisms, package manifests, `Capability`, `CapabilitySurface`, host-profile admission contracts, and capability runtime primitives. Production supervisor: `CoreDaemon` + `botster-session-worker`. |
| `botster-hub` | Trusted first-party host profile policy over core contracts: config locations, persistence policy, auth hooks, startup composition, admission/enforcement, package install/enable/pin/update policy, lifecycle ordering, timeout/failure policy, and audit hooks. |
| CLI | Thin operator entrypoints that start, discover, or attach to a hub without owning profile policy. |
| Clients | Browser, TUI, socket, and custom renderers that consume hub contracts. Clients do not own provider behavior. |
| Plugins/providers | Installable behavior packages that declare capabilities, compatibility, entrypoints, provenance, checksums, enabled state, and update policy. |
| External provider implementations | Cloud federation, signaling relay, browser shell, API, and other privileged integrations implemented outside the hub crate. |

`HubRuntime` is the hub-owned host-profile facade over CoreDaemon plus hub
package, lifecycle, and capability policy. It is not a replacement runtime
engine and it does not own live production PTY handles.

## HubRuntime facade audit

The hub exposes explicit methods where the operation is host-policy,
admission, scheduling, or visibility adjacent. It hides generic core routers and
keeps byte routing, PTY/session mechanics, fanout, plugin workers, capability
surfaces, and transport contracts in `botster-core`.
Client admission is host-profile policy over core admission and transport
contracts, not a hub replacement for `TransportIngress`, `TransportEgress`,
`SessionIo`, or client stream contracts.

`HubClientApi` is the stable local client API boundary over this facade. Socket,
CLI, TUI, and local browser bridge adapters frame the same request/response/event
contract instead of bypassing hub admission or calling raw core routers.
Terminal attach is a terminal-stream handshake only. Session-list reads remain
an operator/query API; stateful clients use the explicit held-open `session`
entity subscription for an authoritative snapshot followed by ordered deltas.
Enabled Lua packages may also declare exact package-namespaced
`entity_provider` families. Their UiNode bindings are admitted only for the
declaring package, and each subscribe or reconnect queries the isolated worker
for a freshly validated whole-family snapshot before bounded daemon/WebRTC
delivery.
The reusable contract is prepared in `@trybotster/hub-test-support@0.1.23` as
source-derived JSON fixtures and a Rust
`run_session_lifecycle_subscription_conformance` runner over the real isolated
Hub/Core/session-worker topology.
Every delivered session row includes required `lifecycle_class`:
`starting|running|stopping` map to `current`,
`exited|failed` map to `ended`, and missing lifecycle maps to
`indeterminate`; `registry_state: stale` always takes precedence and maps to
`indeterminate`. Plugin-authored trees may bind only the Hub-admitted
`/session` family, keyed by canonical session UUID. Inside a BindList item
template, authored `UiNode.id` may bind directly to a row field such as
`@/session_uuid`. Descendants may use contract-owned keyed identity such as
`bind_list_descendant_id/remove`, realized from that row through the canonical
UTF-8 byte-length helper. Realized ids and action request/result `node_id` remain
literal strings. A missing row alone means unknown/unavailable.
That subscription hydrates no status, package, worktree, target, or plugin state.
The local socket adapter admits at most 64 live connections and runs them as
joined Tokio tasks on a fixed transport runtime; it does not create an OS
thread per client. Complete healthy attach and entity streams may remain idle
indefinitely. Handshakes, incomplete frames, stalled writes, peer loss, and
daemon shutdown are bounded failure paths. `DaemonStatus.lifecycle_counters`
reports sanitized accepted/rejected/live/high-water connection and
subscription counts, cleanup outcomes, journal/baseline reconciliation work,
and entity delivery pressure without exposing session or subscription ids.
Steady-state entity reconciliation consumes the Core lifecycle journal on one
shared 500 ms backstop and performs a filesystem-backed baseline only when the
journal explicitly requires resynchronization.
Hub code
embeds the typed CoreDaemon API; it must not shell out to the thin core daemon
CLI or parse CLI output for session routing. Screen and snapshot requests route
through `HubRuntime -> CoreDaemon` and return typed readback response DTOs.
Snapshot readback returns metadata only; opaque snapshot bytes stay on the
attach/drain data plane. Subscription history still flows through attach/drain
events rather than through readback responses.

The renderer-neutral plugin UI wire contract is owned by the standalone
`botster-ui-contract` workspace crate and its generated
`@trybotster/ui-contract` package. Hub runtime code validates and routes those
types but does not own renderer presentation policy. `botster-hub-client`
re-exports the same typed daemon bodies; clients must not maintain local
`UiNode` or action-result mirrors.
Hub validates plugin surfaces and accepted replacement trees as authored
UiNodes. Required bindable fields are the explicit seven-field contract:
Button/IconButton/MenuItem `label`, Form `submit_label`, Iframe `src` and
`title`, and Text `text`. Renderer clients materialize these bindings; Rust
clients use the contract crate's strict realized validator, while non-Rust
clients enforce the equivalent sentinel-free boundary in their own runtime.
The npm contract package does not export a JavaScript runtime validator. Hub
intentionally does not materialize UI trees.
The packaged plugin-contract-matrix proves the user-shaped path through a real
isolated Hub and plugin worker: rendered action metadata dispatches an accepted
presentation `set`, scoped client state opens the delivered Dialog and satisfies
the selected-workspace equality binding, rejected form values retain both tree
and state, and valid submit plus toggle/clear effects remain typed. Static
`@trybotster/ui-contract` fixtures remain the deterministic contract complement.
The fixture's `contract.sessions` surface accepts a bounded UUID set and
authors `/session` bindings without receiving session values or raw Hub state.
The held-open client entity subscription supplies authoritative snapshots and
ordered patches. The Spawn Button label binds `@/lifecycle_class`; strict Rust
and Node reference materializers resolve it to the row's literal class before
realized validation. Reconnect requires a fresh snapshot and explicit removal is
the only operation that makes a retained reference unavailable.

| Core / daemon operation | HubRuntime decision | Reason |
| --- | --- | --- |
| `execute_command(DefaultEngineCommand)` | Hidden | Generic core router would obscure hub admission/policy. Not the product session path. |
| `list_sessions` | Exposed | Host visibility over daemon-recorded sessions. |
| `lifecycle_baseline` / `lifecycle_changes` | Exposed through session entity subscriptions | CoreDaemon remains lifecycle authority; Hub owns the sanitized projection, bounded delivery, and reconnect baseline. |
| `remove_session` | Exposed for terminal sessions | Explicit host retention policy produces an ordered `entity_remove`; shutdown does not imply forgetting. |
| `spawn_session` | Exposed | Host-admitted local session creation through CoreDaemon. |
| `attach_client` | Exposed | Explicit client subscription handshake without global state hydration. |
| `detach_client` | Exposed | Explicit client subscription teardown through CoreDaemon. |
| `write_bytes` | Exposed | Explicit client terminal input path through CoreDaemon. |
| `resize` | Exposed | Explicit client terminal resize path through CoreDaemon. |
| `guarded_write` | Exposed | Hub admits the request; CoreDaemon owns readiness and delivery states. |
| `publish` / `drain` / `acknowledge` routed envelope | Exposed | Single CoreDaemon coordination bus for native MCP and Lua. |
| `release_sessions_for_restart` / `adoption_scan` / `adopt_session` | Exposed | Explicit daemon restart/adoption over worker-backed sessions. |
| `read_screen` / `read_mode_flags` / `capture_snapshot` | Exposed | Daemon-backed terminal readback through `HubRuntime` and `CoreDaemon`; `read_mode_flags` returns only authoritative `session_id` plus exact `mouse_mode: u8` and preserves errors; `capture_snapshot` returns metadata only, keeping opaque bytes on the attach/drain data plane. |
| `report_delivery_*` | Deferred | Delivery-pressure reporting is not exposed on the production hub path yet. |
| `PluginCapabilityRuntime::submit` | Exposed | Hub owns concrete local capability policy and submits through core request contracts. |
| `PluginCapabilityRuntime::drain_events` | Exposed | Plugin capability completions and timer events are drained through a hub-owned path. |
| `PluginCapabilityRuntime::cleanup_plugin` | Exposed | Capability resources are released during hub plugin reload and unload. |

## Local capability runtimes

`HubRuntime` owns the local concrete capability adapter for production runtime plugins. It
accepts `botster-core` `CapabilityRuntimeRequest` values through
`submit_capability_request`, returns core `CapabilityRuntimeHandle` values, and
drains core `CapabilityRuntimeEvent` values through `drain_capability_events`.
The hub adapter implements scoped filesystem operations, plugin JSON store
operations, logical timers, policy-gated HTTP execution, core's in-memory
WebSocket runtime, and the local hub-side WebRTC adapter for installed
`botster-web`. It does not add product cloud, public WebRTC, webhook, OAuth,
Rails, or provider-specific API behavior.

Filesystem access is rooted under the explicit hub data directory at
`capability-scopes/workspace`. Plugin store data is rooted under
`plugin-data/<plugin>/`, with `project-pipelines` as the first production runtime namespace
grant. Runtime data must not be written under plugin source directories.
Capability grants are scoped to match core request requirements exactly:
`Network:http`, `Network:websocket`, `Filesystem:workspace`,
`PluginDb:project-pipelines`, and `Timers:callbacks`.

HTTP requests are admitted through the core capability runtime and then executed
by the hub transport only when the URL scheme, host, method, headers, body size,
response size, header limits, and timeout policy pass. The default policy allows
loopback HTTP/HTTPS hosts for local production runtime plugins, `GET` and `POST`, and a
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
src/main.rs                operator CLI and daemon binary entrypoints
src/config.rs              hub-owned config resolution and defaults
src/daemon.rs              local daemon lifecycle over runtime and durable state
src/persistence.rs         hub-owned durable hub-state store
src/auth.rs                hub-owned auth and admission hooks
src/packages.rs            hub package policy over core package contracts
src/lifecycle.rs           hub package lifecycle adapter over core plugin workers
src/capabilities.rs        hub-owned local capability runtime policy
src/runtime.rs             HubRuntime facade over botster-core-daemon (CoreDaemon)
src/mcp.rs                 daemon-backed MCP stdio surface
src/lua_runtime.rs         Lua plugin runtime and CoreDaemon coordination bridge
examples/project-pipelines/plugin.lua
                          first Project Pipelines Lua workflow plugin source
```

These modules are real product host-profile surfaces for the local production
path. The tree makes ownership boundaries compile-checked; it is not a frozen
multi-crate split and not a placeholder “shallow scaffold.”

## Out of product scope (not unfinished dual paths)

The following are intentionally **not** hub product surface today—they are not
alternate local runtimes, and they do not compete with the CoreDaemon path:

Rails, TryBotster Cloud, ActionCable, public WebRTC signaling servers, browser
shells as hub builtins, OAuth/device-code flows, provider process supervision,
database-backed cloud sync, marketplace fetch/install packaging UX, and package
installers that clone remote Git sources at enable time.

What **is** product on this repo: local file-backed durable hub state, OS
credential store for production secrets, local installed-package WebRTC
signaling/DataChannel adapter for production runtime clients, local package path install
and entrypoint supervision, and the constrained
`examples/project-pipelines` local plugin package. That package loads through
the real Lua plugin runtime; MCP tools register through `mcp-serve`, dispatch
over daemon transport to the owner thread, invoke through `PluginWorkerEngine`,
and persist through PluginDb under `plugin-data/project-pipelines/`.

## Durable hub state

`FileHubStateStore` persists versioned local state at
`<HubConfig.data_directory>/hub-state.json`. The v1 state model records host
identity, config/schema metadata, package/provider registry snapshots,
credential key references, trusted browser public identities/fingerprints,
bootstrap grant metadata, capability grants, package admission decisions,
enabled/disabled/pinned state, provenance/checksum/update policy fields, local
runtime settings, and audit history.

Durable credential material is not stored in `hub-state.json`. Production
startup uses the OS credential store through the hub-owned `keyring` adapter:
macOS Keychain Services, Windows Credential Manager, or the platform Secret
Service on Unix-like systems. Hub-state records only stable key ids such as
`hub/<host-id>/<purpose>/<local-id>`, the expected provider kind, public browser
identity bytes, derived public fingerprints, trust timestamps, expiry,
revocation, and audit-safe reasons. Raw private keys, browser secrets, bootstrap
grant tokens, file-encryption keys, local paths, hostnames, and emails do not
belong in this JSON file.

An available-but-empty credential store is a valid first boot state. Once
hub-state references credential keys or trusted browser identities,
`HubRuntime::load_from_store` validates those references before session
reconciliation and fails closed if the OS credential store rejects lookup, a
referenced key is missing, the provider kind does not match, or a browser public
key no longer matches its stored fingerprint. The file-backed credential store
is intentionally named and constructed as `TestFileCredentialStore`; it is for
deterministic tests and local fixtures only, not a production fallback.

The durable local startup path is explicit:

```sh
cargo run -- start
```

`start` constructs `HubDaemon`, loads or initializes
`hub-state.json`, restores package/provider policy records through
`PackageRegistrySnapshot` admission, initializes `HubRuntime` through the
worker-backed core daemon facade, binds the configured local Unix socket, and
stays running until `shutdown` asks it to stop. Later operator CLI
invocations connect to that socket with a `hello` / `hello_ack` protocol
handshake before sending daemon requests. Future transports, provider runtimes,
sockets, and supervisors should attach after this lifecycle object has started;
they should not recreate config or durable state ownership.

The no-argument binary path is the interactive operator entrypoint. With
terminal stdin and stdout it resolves the normal data directory, starts or
reuses the daemon, reports package prerequisites, and opens a prompt over the
same command parsers and daemon APIs as explicit CLI invocation. Without both
terminals it rejects before resolving or creating durable runtime state.
`run-one` remains an explicit-data-dir runtime smoke path through
`HubRuntime::load`.

## Local production runtime operator CLI

The `botster-hub` binary includes a deliberately thin local operator surface for
production runtime. Bare interactive `botster-hub` starts or reuses the daemon
and attaches the console. `start` remains the low-level foreground daemon host;
`up` remains the noninteractive package-refresh and daily-app orchestrator.
`status`, `sessions list`, `sessions spawn`, `sessions attach`,
`sessions send-input`, `sessions resize`, `sessions detach`, and `shutdown`
connect to that daemon over the resolved local socket. The CLI remains a thin
adapter: daemon requests still route through `HubClientApi` instead of raw core
routers, and the daemon stamps runtime clocks for separate stateless client
invocations. Package state persists through `hub-state.json`, core registry
metadata persists under the hub data directory, and live worker-backed sessions
can be adopted after an intentional daemon restart.

The end-to-end local production runtime proofs are:

```sh
./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime
./test.sh --test hub_local_runtime_test
./test.sh --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport
script/test-production-package-runtime \
  --hub-repo /path/to/botster-hub --hub-revision <sha> \
  --core-repo /path/to/botster-core --core-revision <sha> \
  --web-repo /path/to/botster-web --web-revision <sha> \
  --tui-repo /path/to/botster-tui --tui-revision <sha> \
  --tui-kit-repo /path/to/botster-tui-kit --tui-kit-revision <sha> \
  --workspaces-repo /path/to/botster-workspaces --workspaces-revision <sha> \
  --project-pipelines-repo /path/to/botster-project-pipelines \
  --project-pipelines-revision <sha> \
  --pre-cutover-hub-revision <sha> \
  --pre-cutover-web-revision <sha> \
  --pre-cutover-tui-revision <sha> \
  --pre-cutover-workspaces-revision <sha> \
  --pre-cutover-project-pipelines-revision <sha> \
  --evidence-dir /path/to/new-evidence-directory
```

The cross-repository acceptance script requires Ruby 2.7 or newer and uses only
Ruby's standard library. It checks the interpreter before building artifacts and
prints an installation/version remediation when the prerequisite is missing.
The committed counter, thread, idle, reload, disable, and post-down invariants
and their macOS/Linux diagnostic recipes are documented in
[`docs/hub-resource-proof.md`](docs/hub-resource-proof.md).

The first test proves the persisted-package CLI path. The explicit-coordinate
script rejects dirty or revision-mismatched repositories before starting a
process. It first installs Web's declared Hub test-support release in a clean
external consumer and requires its metadata, generated protocol bytes,
conformance revision, and packaged fixture checksums to match both the exact Hub
source and Web's vendored protocol.

The default `--mode all` then runs fresh and upgrade acceptance. The fresh leg
installs and enables Web, TUI, Workspaces, and Project Pipelines through the
public package commands. It proves structured dynamic Web readiness while the
retired fixed port is occupied, an explicit Web port override and invalid-port
rejection, browser/WebRTC session and reconnect behavior, the package-launched
headless TUI path, registered plugin tools, status, doctor, smoke, and
owned-process cleanup. The upgrade leg creates its fixture with exact
pre-cutover revisions in temporary Git worktrees, advances only those package
worktrees, and lets current `up` refresh the untouched durable state without a
manual reload. It also proves daemon restart and worker-backed session adoption.

The new evidence directory contains path-neutral revisions, commands, artifact
hashes, operator-state before/after manifests, fresh and upgrade endpoints,
runtime output, and the seven-repository product-surface audit. Retained
`docs/plans/**`, `docs/reports/**`, and Git history are excluded from that audit;
current source, tests, executable scripts, manifests, README files, supported
examples, and current architecture/operator documentation remain in scope.

For daily first-party local development, install and enable `botster-web` and
`botster-tui` once through the ordinary package commands, then use
`botster-hub up` and `botster-hub down`. `up` never discovers sibling
checkouts or accepts package-path flags.

Build `botster-hub` normally with Cargo before treating this as a daily stack.
The session path also needs a built `botster-session-worker` next to the
`botster-hub` binary, or an explicit `--session-worker-bin <path>`. First-party
package app entrypoints must already exist in their package roots before `apps
open` can launch them.

`botster-hub up` starts or reuses a daemon over a stable data directory,
transactionally re-reads every directly installed local path package, requires
the exact enabled package identities `botster-web` and `botster-tui`, starts the
`botster-web` app entrypoint through daemon supervision, and prints the
app/operator commands for the same data directory. The refresh completes before
any enabled entrypoint launches; one invalid local package rejects the complete
refresh without mixing old and new registrations. Registry-installed packages
remain pinned and are not implicitly refreshed:

```sh
cargo run -- up

botster-hub status
botster-hub doctor
botster-hub open web
botster-hub open tui
botster-hub smoke
botster-hub down
```

Every stateful command defaults to `$HOME/.botster/hub`, independent of the
working directory. The precedence is `--data-dir <path>`, then
`BOTSTER_HUB_DATA_DIR`, then `$HOME/.botster/hub`. `XDG_DATA_HOME` is not part of
Hub runtime selection. Pass `--data-dir <path>` for tests, isolated runtimes, or
multiple simultaneous runtimes. The selected directory persists `hub-state.json`,
package registry state, plugin data, and Project Pipelines state. Existing
`$HOME/.botster/{plugins,agents,lua,profiles,shared,workspaces}` directories are
not loaded, changed, or migrated.

The command is idempotent: rerunning it against a live daemon reuses that daemon,
and rerunning after shutdown reloads the persisted package registry from
`hub-state.json`. Normal output does not print local package source paths:

```sh
runtime=ready
data_dir=resolved:$HOME/.botster/hub
daemon=started
protocol=botster-hub-daemon-v1
protocol_version=6
conformance_fixture_revision=30
package_count=2
enabled_package_count=2
app_count=2
app package=botster-web app_id=web-client kind=web_app lifecycle_state=running local_url=http://127.0.0.1:49152/
web=http://127.0.0.1:49152/
tui=botster-hub apps open --data-dir $HOME/.botster/hub botster-tui
mcp=botster-hub mcp-serve --data-dir $HOME/.botster/hub
status=botster-hub status --data-dir $HOME/.botster/hub
apps=botster-hub apps list --data-dir $HOME/.botster/hub
down=botster-hub down --data-dir $HOME/.botster/hub
```

These emitted copy/paste commands pin the exact resolved runtime. Ordinary
operator examples below omit the optional flag and use the canonical default.

The console accepts request/response commands without a repeated
`botster-hub` prefix. Foreground terminal apps temporarily own the terminal and
return to the prompt afterward. Commands that own stdin or host another runtime
(`start`, `mcp-serve`, `sessions attach`, `inspect`, and `run-one`) remain
external-only and the console prints the exact explicit invocation to use.

`botster-hub doctor [--data-dir <path>]` is the non-mutating diagnostic path for
the daily runtime or an explicitly selected runtime. It reports stable check
rows such as daemon running, daemon compatibility, core initialization, package
registry counts, first-party package state, and the `botster-web` app URL state.
Stopped runtimes and stale/incompatible daemons exit nonzero with a remediation
command instead of leaking raw protocol errors.

`botster-hub smoke [--data-dir <path>]` is the end-to-end local runtime proof.
It uses the same daily default unless an override is provided, may start or reuse
the daemon, enables first-party local packages, starts package entrypoints, and
creates a disposable smoke session. When the first-party package prerequisites
are not available, it exits nonzero with a named `missing_prerequisite=...`
diagnostic.

The daily aliases are only shortcuts over the lower-level daemon-backed
commands:

```sh
botster-hub open web
botster-hub open tui
botster-hub reload botster-web
```

`open web` resolves the first-party `botster-web/web-client` app through the
same `apps open` path. `open tui` resolves `botster-tui` through the daemon's
terminal launch contract. `reload <package>` remains an explicit package reload
alias backed by the same local-package refresh implementation as `up`; it
restarts that package's already-running entrypoints after the refreshed
registration is durably committed.

Command layers:

- Runtime commands: `up`, `down`, `status`, `mcp-serve`, plus the daily
  `open web`, `open tui`, and `reload <package>` aliases.
- App entrypoints: `apps list`, `apps show`, and `apps open` operate on
  installed package runnable entrypoints projected by the daemon.
- Local packages: `packages install --path`, `packages enable`, `disable`,
  `remove`, `reload`, and update commands mutate the running daemon owner.
- Registry packages: `packages available`, `inspect`, `preview-install`, and
  `install --registry` operate on a registry directory or file.
- Plugin configuration: `packages config` reads configuration and
  `packages config set` writes configuration JSON for an installed package.

The daemon constructs Core's typed Hub connection descriptor from its absolute
Unix socket path and serializes it into each runnable entrypoint's
manifest-declared `hub_connection` target. Package data paths are likewise
injected only through declared `data_dir` targets. Foreground terminal apps run
through `botster-hub apps open`, which asks the daemon for the resolved launch
contract and then starts the child with inherited stdio. Host-owned injections
remain authoritative after environment overrides and package working-directory
changes.

For lower-level package diagnostics, the daily flow maps to the daemon-owned
package commands:

```sh
botster-hub packages install \
  --path /path/to/botster-web
botster-hub packages enable botster-web
botster-hub packages check-update botster-web
botster-hub packages preview-update \
  botster-web --revision local-dev --policy manual
botster-hub packages apply-update \
  botster-web --revision local-dev --policy manual
botster-hub packages reload botster-web
```

The ordinary package commands perform the one-time install/enable step.
`packages check-update`, `preview-update`, and
`apply-update` exercise the hub's update metadata path; they do not fetch local
package code or rebuild a sibling repo for you. After editing an installed local
package, rebuild that package's own output when needed, then run bare
`botster-hub up`; it re-reads all direct local manifests before launch and
restarts entrypoints that were already running. Use `packages reload` when an
operator needs to refresh one package without running the daily `up` flow.

From another terminal, the composed local client app path should be visible
through the same stable data directory:

```sh
botster-hub apps list
botster-hub apps show botster-web/web-client
botster-hub apps open botster-web/web-client
botster-hub apps open botster-tui
```

`apps open botster-web/web-client` reports an `app_url=` matching the printed
`web=` URL. `apps open botster-tui` uses the daemon-resolved `terminal_app`
foreground launch contract; there is no standalone fallback when the package is
missing or disabled.

The CLI commands below exercise the daemon-backed workflow across separate
processes:

```sh
# Terminal 1: leave the daemon running.
cargo run -- start

# Other terminals:
cargo run -- status

cargo run -- packages install \
  --path examples/synthetic-plugin
cargo run -- packages available \
  --registry path/to/package-registry.json
cargo run -- packages inspect \
  --registry path/to/package-registry.json runtime.synthetic-plugin
cargo run -- packages preview-install \
  --registry path/to/package-registry.json runtime.synthetic-plugin
cargo run -- packages install \
  --registry path/to/package-registry.json runtime.synthetic-plugin
cargo run -- packages list
cargo run -- packages show runtime.synthetic-plugin
cargo run -- packages config runtime.synthetic-plugin
cargo run -- packages config set \
  runtime.synthetic-plugin '{"enabled":true}'
cargo run -- packages enable runtime.synthetic-plugin
cargo run -- packages check-update runtime.synthetic-plugin
cargo run -- packages preview-update \
  runtime.synthetic-plugin --revision v1.0.1 --policy manual
cargo run -- packages apply-update \
  runtime.synthetic-plugin --revision v1.0.1 --checksum sha256:example --policy manual
cargo run -- packages reload runtime.synthetic-plugin
cargo run -- packages start-entrypoint \
  runtime.synthetic-plugin web
cargo run -- packages entrypoint-status \
  runtime.synthetic-plugin web
cargo run -- packages restart-entrypoint \
  runtime.synthetic-plugin web
cargo run -- packages stop-entrypoint \
  runtime.synthetic-plugin web
cargo run -- packages disable runtime.synthetic-plugin
cargo run -- packages remove runtime.synthetic-plugin
cargo run -- providers list

cargo run -- apps list
cargo run -- apps show runtime.synthetic-plugin/web
cargo run -- apps open runtime.synthetic-plugin/web

cargo run -- sessions spawn \
  --session-id runtime-session -- "printf 'production runtime-ok\n'; sleep 1"
cargo run -- sessions list
cargo run -- sessions attach runtime-session
cargo run -- sessions send-input \
  runtime-session -- "ping\r"
cargo run -- sessions resize \
  runtime-session 30 100
cargo run -- sessions detach runtime-session
cargo run -- sessions shutdown runtime-session
cargo run -- inspect runtime-session
cargo run -- shutdown
```

`packages install --path` connects to the running daemon, validates the local
package manifest through the existing hub package registry policy, persists it
as installed but disabled under `hub-state.json`, and then lists packages from
the daemon's refreshed in-memory registry. `packages show`, `packages enable`,
`packages disable`, `packages remove`, `packages list`, and `providers list` use
the same daemon-backed registry view. `packages enable --path` remains available
as a convenience that installs and enables in one daemon-owned mutation.
Registry package discovery uses `packages available`; there is no separate
`search` command. `packages inspect`, `preview-install`, and
`install --registry` operate on entries from that registry view. Plugin
configuration is separate package state and uses `packages config` plus
`packages config set`. `packages check-update`, `packages preview-update`, and
`packages apply-update` also route through the daemon. They report structured
unavailable diagnostics for unsupported update/reload paths and `apply-update`
records pin/checksum metadata without fetching package code, starting
entrypoints, or restarting the hub. `packages reload` re-reads an installed
package manifest and restarts any running entrypoints for that package.
`packages start-entrypoint`, `stop-entrypoint`, `restart-entrypoint`, and
`entrypoint-status` control app entrypoint processes directly. The
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
The package manifest chooses the environment or argument targets for the typed
Hub connection descriptor and package data directory. The descriptor's Unix
socket path and the data directory are absolute and must not be resolved
relative to the package working directory.

```sh
# Terminal 1: leave an explicitly isolated daemon running.
cargo run -- start --data-dir /tmp/botster-hub-tui-production-runtime-data

# Terminal 2: create a deterministic echo-loop session for typed-input testing.
cargo run -- sessions spawn --data-dir /tmp/botster-hub-tui-production-runtime-data \
  --session-id runtime-session -- "printf 'production runtime-ok\n'; while IFS= read -r line; do printf 'runtime:%s\n' \"$line\"; done"

# Terminal 3: operate the session from the installed terminal app.
botster-hub apps open --data-dir /tmp/botster-hub-tui-production-runtime-data botster-tui
```

The echo-loop fixture is intentionally not a shell: typing `hello` should produce
`runtime:hello`, while commands such as `ls` are just echoed back. For shell
commands, spawn a separate long-lived shell session and attach to that session
from the TUI:

```sh
cargo run -- sessions spawn --data-dir /tmp/botster-hub-tui-production-runtime-data \
  --session-id runtime-shell -- "/bin/sh -i"
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

Package commands require the daemon socket for this local production runtime path. When
the daemon is not running they fail with `daemon not running` instead of
mutating `hub-state.json` out of band. That keeps package/provider state,
daemon-backed status, and plugin lifecycle reads on one control plane.

## Agent-facing MCP stdio

Local agents can launch the daemon-backed MCP surface against the canonical
default data directory:

```sh
botster-hub mcp-serve
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
CoreDaemon::{publish,drain,acknowledge}_routed_envelope`. That is the only
product coordination path: CoreDaemon owns the router, cursors, and delivery
state. Cursors are process memory; the hub surface reports the cursor returned
by CoreDaemon and does not claim restart-durable inbox state.

Tool listing and calling both route through `McpToolRegistry`. Native hub tools
are provided by `NativeHubToolProvider`; Lua plugin tools use
`PluginHubToolProvider` descriptors and owned daemon call messages on the same
registry path. Plugin execution is dispatched through the plugin
worker/supervisor boundary instead of creating a second MCP server or direct
in-process closure path.

The native local coordination path uses no Lua or plugin tool execution:
`whoami`, `post_message`, `receive_messages`, `ack_message`, and
`notify_session` are native hub tools even when the binary also has the Lua
plugin runtime available. Project Pipelines uses the same CoreDaemon-owned
routed-envelope bus from `examples/project-pipelines/plugin.lua` through
`botster.coordination.*` (Lua bridge into the same CoreDaemon instance—not a
second inbox).

## Project Pipelines Local Readiness

The checked-in `examples/project-pipelines` package is ready for constrained
local coordination through the ordinary persisted package registry. Install
and enable it against the running daemon, then run MCP from that same directory:

```sh
botster-hub packages install \
  --path examples/project-pipelines
botster-hub packages enable project-pipelines
botster-hub mcp-serve
```

For lower-level diagnostics, enable the plugin package through a running daemon
and serve MCP from the same data directory:

```sh
cargo run -- packages install \
  --path examples/project-pipelines
cargo run -- packages enable project-pipelines
cargo run -- mcp-serve
```

`mcp-serve` lists and calls the plugin's Project Pipelines tools through
`PluginHubToolProvider -> daemon request -> HubRuntime -> HubPluginLifecycle ->
PluginWorkerEngine -> LuaPluginRuntime`. `project_pipelines.start` requires an
explicit `target_id` and assigned worktree and records primitive-backed
coordination evidence on the run: request id, agent name, owner plugin, routed
envelope id, publish delivery status, drain cursor, and acknowledge delivery
status. It then spawns the hub-owned `project-pipelines/agent-step` session
template through the plugin worker `session_types.spawn` path and records
`session_uuid`, `session_type_id`, `session_context_id`, and
`session_lifecycle` on the run coordination record.

Session types are hub-owned PTY launch contracts, not Project Pipelines
entrypoints or legacy monolith agent runners. The plugin supplies product
workflow policy and context, while the hub validates and materializes the
template into a generic session spawn. Project Pipelines prompts and tool calls
must carry an explicit target id and worktree; they should not depend on the
agent's ambient current directory.

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

Project Pipelines uses the same persistent daemon, ordinary installed packages,
and `mcp-serve` over one data directory as every other production package.

Production runtime-ready today: explicit local daemon lifecycle, file-backed hub/package
state, local package admission from a manifest path, typed status/package reads,
plugin lifecycle observation/invocation through the hub facade, daemon-backed
PTY spawn/list/attach/input/resize/detach/session-shutdown through
`HubClientApi`, attach/drain history plus screen/snapshot readback through
CoreDaemon, and cross-process daemon transport proof for hub restart recovery.

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
[`docs/adr/local-runtime-production-readiness.md`](docs/adr/local-runtime-production-readiness.md).

In the daemon-backed model, attach, detach, input, and resize requests are
control-plane acknowledgements. Terminal egress is delivered by explicit
`DrainRuntime` calls over the session-backed CoreDaemon path, not synchronously
from those control operations.

Ready for daily local use today: explicit daemon lifecycle, daemon-backed local
PTY session operations including attach/drain history and screen/snapshot
readback, minimal daemon-backed TUI attach/reconnect, native MCP coordination
tools, and constrained Project Pipelines MCP workflow tools over the Lua plugin
runtime.

Still unfinished (not alternate production paths): delivery-pressure reporting
(`report_delivery_*`), durable PTY recovery after uncoordinated daemon/worker
crash, provider process supervision, GitHub/PR automation, marketplace
fetch/update packaging, cloud/Rails/public WebRTC/browser-as-hub-builtin
surfaces, broad migration compatibility from the monolith, missing-public-socket
self-heal after the socket path is externally removed, long-running attach
signal handling, restart-durable coordination inboxes, and observed readiness
for always-delivered doorbells.

## Daily Dev Troubleshooting

Stale package build output: rebuild the edited sibling package with its own
repo's build command, then rerun `botster-hub up`. The daily command re-reads
direct local manifests but does not build sibling artifacts; a missing declared
package-relative command fails before launch with the package name, local path,
and rebuild remediation. `botster-hub packages reload <package-name>` remains
available for an explicit one-package refresh.

Missing app or Lua entrypoints: run
`botster-hub packages show <package-name>` and `botster-hub apps list`. Local packages need a valid
`botster-package.json`; runnable client apps need `runnable_entrypoints`; Lua
plugins such as Project Pipelines also need their `entrypoints` path to exist.
`apps open` has no fallback when the package is missing, disabled, or lacks the
requested app selector.

Missing provider config or auth: the local local runtime does not import cloud,
GitHub, or monolith credentials. Re-enter provider credentials when a provider
integration asks for them, and treat unavailable provider/GitHub automation as
deferred unless the relevant package and config are installed.

Session-template spawn failure: confirm the Project Pipelines package is
enabled, the same data directory is used for `mcp-serve`, and the package still
declares the `project-pipelines/agent-step` template. `project_pipelines.start`
also requires explicit `target_id` and `worktree` arguments; missing either one
is a tool-call error, not a template fallback.

Terminal attach or scrollback issues: use `botster-hub sessions list`, attach
only to a running session, and expect
terminal output to arrive through the session-backed drain/subscription path.
Late attach may replay existing terminal output as ordinary terminal data rather
than a distinct scrollback frame, and current long-running attach signal
handling is still listed as pending readiness work.

Schema and consistency posture are documented in
[`docs/adr/durable-hub-state-v1.md`](docs/adr/durable-hub-state-v1.md).

## Dependency policy

During development, this repo tracks `botster-core` from the `main` branch
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

The in-process `HubClientApi` product client workflow supports status, session
list, spawn, attach, input, resize, drain/output events, screen and snapshot
readback, shutdown, guarded notification write, routed-envelope publish/drain/ack,
package queries, and plugin lifecycle status. The daemon socket and local WebRTC
adapter route through this same API. Browser parity and cloud transports remain
outside the smoke command; local TUI attaches through the same daemon socket.

## Package registry policy

`default_package_policy()` is the production-facing hub-owned package policy
surface. It builds a `PackageAdmissionPolicy` from `host_profile()` default
capability grants, then stores in-memory package records around the Hub-owned
`HubPackageManifest` through `PackageRegistry`. The registry
keeps enabled/disabled state, records provenance/checksum and pin/update
metadata, classifies providers from `botster_core::ExtensionKind`, persists a
hub-owned trust marker, records admitted capabilities after enablement, records
the narrow Botster compatibility result/diagnostics, and validates enable
decisions against the profile-derived hub grant set.

The Hub manifest reuses Core's policy-free capability, dependency,
configuration, entrypoint, and host-profile field types, while
`botster-ui-contract` owns renderer-neutral surface and navigation semantics.
Provider packages must carry host-profile
metadata before they can be enabled; provider host-profile packages are admitted
through `botster_core::admit_host_profile`, so core still owns the narrow
manifest/admission preconditions while hub owns install, enable, disable, pin,
grant, provenance, update, and audit policy.

Local production runtime installs accept either an explicit JSON manifest path or a package
directory containing `botster-package.json`. The file is parsed once as
`HubPackageManifest`; the hub rewrites the manifest source to
`PackageSource::Path` with
the canonical package root, records
`local:<canonical-package-root>` provenance, marks the record as
`local_development` trust, and rejects absolute, traversing, or symlink-escaped
entrypoints before registry mutation. Enabled local records can be prepared into
canonical entrypoint paths for the core lifecycle adapter.

`entrypoints` remains the core plugin/provider code-load contract. Runnable
process declarations live under `runnable_entrypoints` so clients, launchers,
and future marketplace tooling share one discovery shape without changing
plugin loading semantics. Each runnable entrypoint declares a stable `id`,
`kind` (`web_app` or `terminal_app`), `command`, `args`, `working_directory`,
structured `injections`, declarative `environment` requirements, `launch_mode`
(`background` or `foreground_stdio`), structured `readiness`, capability needs,
and `may_supervise`. The Hub admits, launches, supervises, health-checks,
restarts, and projects these entrypoints through the installed-app API.

Local path manifest example:

```json
{
  "name": "runtime.synthetic-plugin",
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
      "kind": "web_app",
      "launch_mode": "background",
      "command": "node",
      "args": ["scripts/local-package-server.mjs"],
      "working_directory": { "policy": "package_root" },
      "injections": [
        {
          "kind": "hub_connection",
          "target": {
            "type": "environment",
            "name": "BOTSTER_HUB_CONNECTION"
          },
          "required": true
        }
      ],
      "environment": [],
      "readiness": { "result_fields": ["local_url"] },
      "capabilities": [
        { "surface": "network", "scope": "localhost" }
      ],
      "may_supervise": true
    }
  ]
}
```

Packages can also declare hub-owned `session_types` for PTY session launches
with trusted context injection:

```json
{
  "session_types": [
    {
      "id": "init",
      "label": "Interactive agent",
      "description": "A task-scoped interactive coding session.",
      "icon": "terminal",
      "role": "botster.agent",
      "interaction": "interactive",
      "traits": ["coding", "worktree-aware"],
      "lifecycle": "task",
      "command": "bin/init.sh",
      "working_directory": { "policy": "package_root" },
      "environment": { "BOTSTER_MODE": "default" },
      "allowed_environment_overrides": ["BOTSTER_MODE"],
      "context": ["prompt", "ticket_id"]
    }
  ]
}
```

The Hub validates session types and materializes them into generic Core spawn
requests. Role, interaction, traits, and lifecycle are independent strings:
`persistent` does not imply `botster.agent`, and `interactive` does not imply
either an agent or accessory role. Core remains unaware of session type ids, Project Pipelines, Codex,
Claude, agents, tickets, workspaces, and `botster context`. Explicit spawn
overrides are admitted only when the template and target policy allow them.
Spawned scripts receive `BOTSTER_SESSION_ID`, `BOTSTER_CONTEXT_ID`,
`BOTSTER_HUB_DATA_DIR`, `BOTSTER_HUB_SOCKET`, and `BOTSTER_HUB_BIN`, and can read
context with `"$BOTSTER_HUB_BIN" context --key prompt`.

Session type sources are layered by Hub policy: package < device < repo < explicit
request values. Effective rows expose the winning `source`/`source_name`,
`editable`, `overridden_sources`, and diagnostics. Device definitions and the
monotonic session-type generation are durable in `hub-state.json`; schema 3 is a
cold cut and rejects older state before decoding the new shape. Repo-local
definitions are read and written only through enabled admitted target roots at
`.botster/session-types.json`:

```json
{
  "session_types": [
    {
      "id": "init",
      "label": "Repository agent",
      "role": "botster.agent",
      "interaction": "interactive",
      "traits": ["coding"],
      "lifecycle": "task",
      "command": "bin/repo-init.sh",
      "environment": { "BOTSTER_MODE": "repo" },
      "allowed_environment_overrides": ["BOTSTER_MODE"]
    }
  ]
}
```

The repo file uses the same definition shape as package manifests. Device rows
support full CRUD, repo rows support CRUD scoped by `target_id`, and package rows
return `read_only_session_type_source` for mutations. The daemon and
`session-types create|update|delete` CLI are the mutation boundary; callers do
not receive filesystem paths or raw state access.

Update replaces a definition wholesale, and the ordinary list/show row is
sanitized — it derives a `working_directory_policy` string and omits the
authored environment — so an editor cannot rebuild a complete definition from
it. `session-types definition <session-type-id>` is the editor-scoped read that
closes that gap: it returns the authored definition exactly as update consumes
it, plus the mutation source, so a read-modify-write edit preserves the authored
working-directory path and environment. It covers device and repo definitions
only; a package-owned id returns `read_only_session_type_source`, and it is
admitted under the same editor authority as create/update/delete rather than the
broader sanitized-read category. Ordinary list/show rows and `session_type`
entity frames are unchanged and still carry no authored environment or path.

Each observable mutation
advances the durable generation and publishes `session_type` entity upsert or
remove frames, with bounded overflow recovering through a full snapshot.
Disabled or unadmitted targets contribute no definitions, and final command,
cwd, and environment remain checked against the selected source root.

Spawn materialization writes only bounded classification facts into opaque Core
host metadata: session type id/source, role, traits, interaction, and lifecycle.
The canonical `session` entity projection reads those facts back from Core
lifecycle records, including reconnect and Hub restart/adoption. Direct sessions
with no session-type metadata expose explicit absence; Hub never infers a type
from command, name, owner, or duration.

Hub-owned spawn targets are local directory admissions with stable `target_id`,
label, root, enabled state, kind, optional `base_ref`, and small sanitized metadata.
They are hub policy state, not `botster-core` state. Plugins reference target
ids and may list or validate them through Lua capabilities, while create,
update, and delete stay on the daemon/CLI operator path. `kind = "directory"`
keeps the generic behavior and does not imply Git even when the directory is a
repository. `kind = "git"` explicitly opts into managed worktrees and requires
a stored `base_ref`. Create, or an explicit directory-to-Git update, may capture
the current symbolic branch once when `base_ref` is omitted. Later managed
spawns resolve the stored value and never guess `main`/`master` or reread live
`HEAD` as policy.

Hub-owned worktrees are generic working-directory records scoped to a spawn
target. They persist a stable `worktree_id`, `target_id`, label, canonical path,
reconciled status, optional git metadata, and small sanitized metadata. A
worktree does not require git; `.git` is inspected only when present. Existing
rows deserialize with `management = "registered"` and retain target-root
containment. The atomic managed-Git path records
`management = "hub_managed_git"` and owns its deterministic path beneath
`<data-dir>/managed-worktrees`. Those rows reconcile repository identity and
branch ownership instead of requiring the path to be beneath the target root.
Plugins
that need workflow associations should store those associations in plugin state
and reference the returned `worktree_id` rather than adding workflow fields to
hub records. Worktree delete removes registered records only and does not
delete filesystem contents. Hub-managed Git rows reject record-only deletion,
and their target cannot be deleted or reclassified while they remain recorded.

All loaded plugins receive the same target-filtered template list/show
projections as the existing target and worktree reads. Packages granted the
exact `session_type_managed_git_spawn` session-action scope additionally
receive one atomic Lua mutation:

```lua
local templates = botster.capabilities.session_types.list({
  target_id = "tgt_repo",
})

local result =
  botster.capabilities.session_types.ensure_worktree_and_spawn({
    target_id = "tgt_repo",
    branch = "feature/example",
    session_type_id = templates[1].session_type_id,
    context = { ticket_id = "ticket_example" },
  })
```

The Hub validates the Git target and template, serializes Git mutation on a
bounded lane, reuses the exact matching worktree or creates it from the stored
base ref, derives cwd and trusted context, and spawns through Core as one
operation. Dirty matching worktrees are reused without reset or cleanup.
Conflicting branch/path ownership is a typed failure. Spawn failure rolls back
only the worktree and branch created by that call; pre-existing branches,
worktrees, and dirty content are never deleted. The older
`session_type_spawn` scope does not grant this operation, and generic
registered-worktree CRUD remains a record-only, non-destructive API.

Worktree CRUD emits client-visible `worktree_lifecycle` daemon events and
worker-isolated Lua plugin events:

```lua
events.on("worktree_created", function(event)
  -- Store workflow-specific associations in plugin state keyed by worktree id.
  return { worktree_id = event.worktree_id, target_id = event.target_id }
end)

return botster.register({})
```

The stable event names are `worktree_created`, `worktree_create_failed`,
`worktree_deleted`, and `worktree_delete_failed`. Lifecycle payloads include ids
and sanitized metadata such as status, label, relative display path, and failure
kind/message, but they do not include raw absolute worktree paths by default.

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

The Hub now owns one read-only product maintenance path. `status` reports
immutable embedded product identity plus installation provenance from exactly
`$HOME/.botster/installations/botster-hub.json`; `check-update` lets a valid
managed receipt query its configured authoritative HTTP JSON source without
mutating packages, Git, the running binary, or durable Hub state. `version`
prints the binary's own `product_id`/`version`/`build_revision` with no data
directory and no running daemon, which is how the installer verifies a staged
binary that has never been started. Development builds, manual/unmanaged
installs, missing receipts, and invalid or unsafe receipts return an honest
unavailable/manual result.

### Managed distribution

`botster-hub-installer` writes the half the maintenance path only reads.

The Hub and its locked-Core worker are **one revision-coupled generation**,
never two independently replaceable files:

```
<prefix>/
  daemon.lock                                   # the installation lease
  generations/
    <hub-sha>-<core-sha>/
      botster-hub
      botster-session-worker
  current -> generations/<hub-sha>-<core-sha>   # the pointer
  bin/
    botster-hub -> ../current/botster-hub       # the stable entrypoint
```

Both binaries are reachable only through one pointer that flips with a single
atomic `renameat`, so a mixed Hub-at-N+1-beside-worker-at-N pair is unreachable
by construction rather than merely unlikely. Rollback is that same operation
pointed back at the retained previous generation. Staging happens in a unique
`.staging-<random>/` directory that is renamed into place, so the final
generation name is complete by construction.

```sh
botster-hub-installer install \
  --prefix ~/.local/share/botster \
  --source https://releases.example/botster-hub.json \
  --trust-anchor /path/to/release-signing.pub
```

- **Upgrades are offline.** Every managed Hub daemon takes `LOCK_SH|LOCK_NB` on
  `<prefix>/daemon.lock` at startup and holds it for its lifetime; the installer
  takes `LOCK_EX|LOCK_NB` and holds the same descriptor across switch,
  verification, and receipt commit or rollback. This is authoritative across any
  number of daemons and any data directories, which a socket probe could never
  be. Both sides are non-blocking and fail fast with a diagnostic.
- **Signature verification is installer-only.** The installer is the trust
  boundary because it is the component that writes executables. The Hub holds no
  trust anchor and verifies nothing; it records signature *facts*. The trust
  anchor is not embedded — `--trust-anchor` is required.
- **The verified manifest is the sole authority.** `product_id`,
  `release_channel`, `version`, and `build_revision` appear in both the unsigned
  envelope and the signed manifest, and the installer requires exact equality on
  all four. Without that rule a validly signed *old* manifest could be wrapped in
  an envelope advertising a *new* version.
- **The receipt is written last**, so no reachable state places a schema-2
  receipt beside an old generation. Intermediate crash states degrade honestly:
  the Hub reports unmanaged rather than falsely claiming managed, and re-running
  the idempotent installer converges.
- **Durability has two strengths of claim.** `SIGKILL` safety is demonstrated by
  crash injection at every boundary. Power-loss durability is *argued from the
  fsync ordering* and is **not** demonstrated — this repository has no
  fault-injection harness, and building one is out of scope.

Release metadata is built and signed by `script/build-release-artifacts`, which
reads the `Cargo.lock`-pinned `botster-core` revision rather than introducing a
second pinning mechanism, so the Hub SHA and locked-Core SHA stay distinct
identities in both the manifest and the receipt.

Deferred production concerns remain outside this slice: publishing to a real
origin, production key custody, release CI, online upgrade, generation pruning
beyond retaining the previous one, auto-update daemons, sandboxing, notarization,
multi-platform artifact matrices, delta updates, downgrade support, Hub-side
signature verification or startup re-hashing, dependency solving, hosted
marketplace resolution, and remote/cloud WebRTC launch paths.

Compatibility remains deliberately narrow in this slice. The manifest `botster`
field accepts only exact `MAJOR.MINOR.PATCH` or lower-bound
`>=MAJOR.MINOR.PATCH` requirements for the current hub binary. Broader semver
ranges, separate hub/core/client protocol windows, compatibility channels,
hosted registry indexes, signing, dependency solving, auto-update daemons,
publishing portals, network Git clone/fetch/update behavior, and binary/CLI
package-install commands remain excluded.

Compatibility and enabled capability admission fail closed. An incompatible or
invalid `botster` requirement or package presentation declaration is rejected
at install; if a persisted package no longer satisfies the current hub version,
grant/surface policy, or presentation validation during
`PackageRegistry::from_snapshot`, the registry load returns a typed error rather
than silently quarantining that single record. For presentation errors admitted
before this rule existed, the startup error names the package and validation
reason. Back up `hub-state.json`, then remove or correct that package record
before restarting the Hub; the original file is the recovery source if the
manual correction is wrong. The persisted
`PackageCompatibility` value on accepted records is therefore the last accepted
compatible result; incompatible and invalid results are operator diagnostics on
the rejected install/reload path until package quarantine behavior exists.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
