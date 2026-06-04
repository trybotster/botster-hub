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
operations, logical timers, bounded HTTP stubs, and core's in-memory WebSocket
runtime. It does not add product cloud, public WebRTC, webhook, OAuth, Rails, or
external API behavior.

Filesystem access is rooted under the explicit hub data directory at
`capability-scopes/workspace`. Plugin store data is rooted under
`plugin-data/<plugin>/`, with `project-pipelines` as the first dogfood namespace
grant. Runtime data must not be written under plugin source directories.
Capability grants are scoped to match core request requirements exactly:
`Network:http`, `Network:websocket`, `Filesystem:workspace`,
`PluginDb:project-pipelines`, and `Timers:callbacks`.

Filesystem and plugin-store work is accepted through the hub capability path and
completed on runtime-owned worker threads. Plugin unload and reload call
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
```

This scaffold is intentionally shallow. The module tree makes the intended
ownership boundaries compile-checked, but it is not a final API freeze and does
not add a physical multi-crate split.

## Scaffold-only exclusions

This repo does not yet implement Rails, TryBotster Cloud, ActionCable, WebRTC,
signaling servers, browser shells, API clients, OAuth/device-code flows,
provider processes, persistence databases, plugin runtimes, marketplace fetches,
package installers, or client transports. The hub does include local file-backed
durable state for dogfood; database-backed persistence and cloud sync remain
excluded.

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
./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown
./test.sh --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport
```

That test is the documented proof path for the current scaffold. It starts an
explicit `HubDaemon` with durable state, installs and enables the checked-in
`examples/synthetic-plugin` fixture, persists and reloads `hub-state.json`,
pulls status/package/lifecycle state through `HubClientApi`, resolves the
package's Lua entrypoint path, loads the package, invokes a synthetic in-process
plugin runtime through `HubRuntime`, spawns a local PTY session, attaches a
client, sends input, drains the observed marker, and shuts down through the same
local client API. The Lua fixture is not executed by a real Lua runtime in this
proof. The PTY portion is Unix-only.

The CLI commands below exercise the daemon-backed workflow across separate
processes:

```sh
# Terminal 1: leave the daemon running.
cargo run -- start --data-dir target/botster-hub-dogfood-data

# Other terminals:
cargo run -- status --data-dir target/botster-hub-dogfood-data

cargo run -- packages enable --data-dir target/botster-hub-dogfood-data \
  --path examples/synthetic-plugin
cargo run -- packages list --data-dir target/botster-hub-dogfood-data
cargo run -- providers list --data-dir target/botster-hub-dogfood-data

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

`packages enable --path` installs and enables a local package manifest through
the existing hub package registry policy, persists the registry snapshot under
`hub-state.json`, and then lists packages through `HubClientApi::ListPackages`.
The session commands use the running daemon runtime, so a session created by
one CLI process is visible to later `sessions list`, `sessions attach`,
`sessions send-input`, `sessions resize`, `sessions detach`, and
`sessions shutdown` invocations.
`attach` streams terminal bytes and currently exits after an idle window if the
core runtime does not provide a process-exit frame; persistent TUI-grade attach
with explicit signal handling remains outside this scaffold. `inspect` is
intentionally scoped to sanitized session list data until the stable client API
grows a dedicated inspection request.

Package commands are a separate short-lived hub-policy path over the same
durable state. They start a `HubDaemon`, mutate or read `hub-state.json`, route
package/provider reads through `HubClientApi`, and stop; they do not attach to
the long-running session daemon today.

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

Feature parity still pending: durable PTY recovery after daemon exit, provider
process supervision, cloud/Rails/WebRTC/browser/TUI adapters,
marketplace/package fetching, missing-public-socket self-heal after the socket
path is externally removed, and long-running attach signal handling.

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
surface. It builds a `PackageAdmissionPolicy` from `host_profile()` default capability
grants, then stores in-memory package records around
`botster_core::PackageManifest` values through `PackageRegistry`. The registry
keeps enabled/disabled state, records provenance/checksum and pin/update
metadata placeholders, classifies providers from `botster_core::ExtensionKind`,
and validates enable decisions against the profile-derived hub grant set.

The registry deliberately uses core package contracts instead of defining a hub
manifest or capability vocabulary. Provider packages must carry host-profile
metadata before they can be enabled; provider host-profile packages are admitted
through `botster_core::admit_host_profile`, so core still owns the narrow
manifest/admission preconditions while hub owns install, enable, disable, pin,
grant, provenance, update, and audit policy.

Local dogfood installs accept either an explicit JSON manifest path or a package
directory containing `botster-package.json`. The file is parsed as
`botster_core::PackageManifest`; the hub rewrites the manifest source to
`PackageSource::Path` with the canonical package root, records
`local:<canonical-package-root>` provenance, and rejects absolute, traversing,
or symlink-escaped entrypoints before registry mutation. Enabled local records
can be prepared into canonical entrypoint paths for the core lifecycle adapter.

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
registry. Marketplace browsing, git cloning/fetching, network download, lockfile
formats, binary/CLI package-install commands, and concrete plugin/provider
runtime implementations remain excluded.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
