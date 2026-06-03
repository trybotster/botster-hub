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

## Crate layout

```text
src/lib.rs                 public facade over runtime and profile metadata
src/profile.rs             first-party host profile manifest and policy metadata
src/main.rs                thin binary smoke path through the profile facade
src/config.rs              hub-owned config policy seam
src/persistence.rs         hub-owned persistence policy seam
src/auth.rs                hub-owned auth hook seam
src/packages.rs            hub package policy over core package contracts
src/runtime.rs             hub runtime facade over botster-core
```

This scaffold is intentionally shallow. The module tree makes the intended
ownership boundaries compile-checked, but it is not a final API freeze and does
not add a physical multi-crate split.

## Scaffold-only exclusions

This repo does not yet implement Rails, TryBotster Cloud, ActionCable, WebRTC,
signaling servers, browser shells, API clients, OAuth/device-code flows,
provider processes, persistence databases, plugin runtimes, marketplace fetches,
package installers, or client transports.

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

## Package registry policy

`PackageRegistry` is the first concrete hub-owned package policy surface. It
stores in-memory package records around `botster_core::PackageManifest` values,
keeps enabled/disabled state, records provenance/checksum and pin/update
metadata placeholders, classifies providers from `botster_core::ExtensionKind`,
and validates enable decisions against a hub-owned `CapabilitySet`.

The registry deliberately uses core package contracts instead of defining a hub
manifest or capability vocabulary. Provider host-profile packages are admitted
through `botster_core::admit_host_profile`, so core still owns the narrow
manifest/admission preconditions while hub owns install, enable, disable, pin,
grant, provenance, update, and audit policy.

This is the policy gate future package lifecycle loading should call before
starting plugin/provider execution through core APIs. It is intentionally
in-memory in this ticket: persistence belongs under `PersistenceBucket::PackageState`,
and marketplace browsing, git cloning/fetching, network download, lockfile file
formats, and lifecycle load/unload wiring remain excluded.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
