# botster-hub

`botster-hub` is the first-party Botster host profile over `botster-core`.

The hub owns trusted startup composition, product policy, and host integration.
It does not fork or replace core runtime mechanics, and it does not implement
every provider itself. Cloud federation, signaling relays, browser shells, and
API integrations belong in installable providers behind hub-owned capability
contracts.

## Responsibility split

| Layer | Owns |
| --- | --- |
| `botster-core` | Reusable local engine mechanics and transport-neutral primitives: session spawning, PTY/process mechanics, lifecycle, activity, fanout, `TransportIngress`/`TransportEgress`, `SessionIo`, client stream contracts, notifications, plugin worker primitives, capability surfaces, and reusable crypto/identity mechanisms. |
| `botster-hub` | First-party host profile policy: config locations, persistence policy, auth hooks, provider capability contracts, client admission/enforcement, package install/enable/pin/update policy, lifecycle ordering, timeout/failure policy, and audit hooks. |
| CLI | Thin operator entrypoints that start, discover, or attach to a hub without owning product policy. |
| Clients | Browser, TUI, socket, and custom renderers that consume hub contracts. Clients do not own provider behavior. |
| Plugins/providers | Installable behavior packages that declare capabilities, compatibility, entrypoints, provenance, checksums, enabled state, and update policy. |
| External provider implementations | Cloud federation, signaling relay, browser shell, API, and other privileged integrations implemented outside the hub crate. |

The hub embeds `DefaultBotsterEngine` for the reusable tmux-like local engine:
session spawning, PTY/process mechanics, session lifecycle and activity,
subscription fanout, notifications, plugin worker primitives, and consumer
conformance behavior. `HubRuntime` is a hub-owned adapter and policy facade over
that engine, not a separate runtime engine.

## HubRuntime facade audit

The hub exposes explicit methods where the operation is host-policy,
admission, scheduling, or visibility adjacent. It hides generic core routers and
keeps byte routing, PTY/session mechanics, fanout, plugin workers, capability
surfaces, and transport contracts in `botster-core`.

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
src/lib.rs                 public facade and architecture summary
src/main.rs                thin binary smoke path through the facade
src/core.rs                boundary docs for embedded botster-core mechanisms
src/config.rs              hub-owned startup policy for core-owned knobs
src/persistence.rs         hub-owned persistence policy seam
src/auth.rs                hub-owned auth hook seam
src/packages.rs            plugin/provider package policy seam
src/providers.rs           provider capability vocabulary
src/adapters/mod.rs        host adapter contract namespace
src/adapters/clients.rs    client admission taxonomy
src/adapters/cloud.rs      cloud provider contract seam
src/adapters/signaling.rs  signaling relay contract seam
src/adapters/api.rs        external API provider contract seam
```

This scaffold is intentionally shallow. The module tree makes the intended
ownership boundaries compile-checked, but it is not a final API freeze and does
not add a physical multi-crate split.

## Scaffold-only exclusions

This repo does not yet implement Rails, TryBotster Cloud, ActionCable, WebRTC,
signaling servers, browser shells, API clients, OAuth/device-code flows,
provider processes, persistence databases, plugin runtimes, marketplace fetches,
package installers, or client transports.

`botster-core` is currently sourced from the `main` branch in `Cargo.toml` with
default features enabled so `DefaultBotsterEngine` and the default local runtime
remain available. Development follows current core `main` through the checked-in
`Cargo.lock` revision. Release builds should move to a deliberate tag or
revision pin before shipping.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
