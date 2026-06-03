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

The host profile embeds `botster-core` through the default local-runtime-backed
engine facade for the reusable tmux-like local engine and shared package
contracts: session spawning, PTY/process mechanics, session lifecycle and
activity, subscription fanout, notifications, plugin worker primitives, package
manifests, `Capability`, `CapabilitySurface`, host-profile admission contracts,
capability runtime primitives, and consumer conformance behavior. `HubRuntime`
is a hub-owned adapter and policy facade over that engine, not a separate
runtime engine.

## HubRuntime facade audit

The hub exposes explicit methods where the operation is host-policy,
admission, scheduling, or visibility adjacent. It hides generic core routers and
keeps byte routing, PTY/session mechanics, fanout, plugin workers, capability
surfaces, and transport contracts in `botster-core`.
Client admission is host-profile policy over core admission and transport
contracts, not a hub replacement for `TransportIngress`, `TransportEgress`,
`SessionIo`, or client stream contracts.

| Core operation | HubRuntime decision | Reason |
| --- | --- | --- |
| `execute_command(DefaultEngineCommand)` | Hidden | A generic command router would obscure hub admission and policy boundaries. |
| `list_sessions` | Exposed | Host visibility over core-recorded sessions. |
| `inspect_session` | Exposed | Host visibility over lifecycle and activity. |
| `read_screen` | Exposed | Explicit host request for core-owned session screen state. |
| `capture_snapshot` | Exposed | Explicit host request for core-owned snapshot mechanics. |
| `replay_snapshot` | Exposed | Explicit host request for core-owned snapshot replay mechanics. |
| `drain_runtime_all_once` | Exposed | Host scheduler drain hook over live core sessions. |
| `report_backpressure` | Exposed | Typed pressure evidence without hub-owned retry policy. |
| `report_delivery_lag` | Exposed | Typed slow-delivery evidence without hub-owned retry policy. |
| `report_delivery_failure` | Exposed | Typed failed-delivery evidence without hub-owned retry policy. |
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
src/profile.rs             first-party host profile manifest and policy metadata
src/main.rs                thin binary smoke path through the profile facade
src/config.rs              hub-owned config policy seam
src/daemon.rs              deterministic local daemon lifecycle over runtime/state
src/persistence.rs         hub-owned persistence policy seam
src/auth.rs                hub-owned auth hook seam
src/packages.rs            hub package policy over core package contracts
src/lifecycle.rs           hub package lifecycle adapter over core plugin workers
src/capabilities.rs        hub-owned local capability runtime policy
src/runtime.rs             hub runtime facade over botster-core
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
default core engine facade, prints deterministic scrubbed status, and stops
cleanly. Future transports, provider runtimes, sockets, and supervisors should
attach after this lifecycle object has started; they should not recreate config
or durable state ownership.

The no-arg binary path is a side-effect-light host-profile summary. It builds
resolved config and an in-memory `HubRuntime::new` summary only; it does not
load or save `hub-state.json` through HOME/XDG fallback paths. `run-one` remains
an explicit-data-dir runtime smoke path through `HubRuntime::load`. Registry,
grant, and admission mutation saves are covered by storage-boundary tests and
await the operator/package-manager commands that will call
`HubStateStore::update`.

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

The hub runtime embeds `botster-core`'s default local engine path via
`DefaultBotsterEngine`. Keep core default features enabled so the `local-runtime`
feature remains active unless the hub intentionally replaces that runtime
contract.

## Runtime smoke proof

The production binary includes a deliberately thin smoke entrypoint:

```sh
cargo run -- run-one --data-dir target/botster-hub-smoke-data -- /bin/sh -c "printf 'botster-hub-smoke-ok\n'"
```

`run-one` requires an explicit `--data-dir`, builds hub config without falling
back to user paths, then crosses `HubRuntime -> DefaultBotsterEngine` through
spawn, attach, drain, marker observation, and shutdown. Its output is scrubbed
to profile, host, session, marker, byte-count, and shutdown-observation facts so
pipeline artifacts do not need local paths, environment dumps, keys, or
fingerprints.

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
