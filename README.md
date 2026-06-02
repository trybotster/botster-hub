# botster-hub

`botster-hub` is the Botster product host around `botster-core`.

The hub owns product policy and host integration. It does not implement every
provider itself; cloud federation, signaling relays, browser shells, and API
integrations belong in installable providers that declare capabilities through
`botster-core` package contracts.

## Responsibility split

| Layer | Owns |
| --- | --- |
| `botster-core` | Reusable local engine mechanics and transport-neutral primitives: session spawning, PTY/process mechanics, lifecycle, activity, fanout, notifications, plugin worker primitives, reusable crypto/identity mechanisms, package manifests, `Capability`, `CapabilitySurface`, host-profile admission contracts, and capability runtime primitives. |
| `botster-hub` | Product host policy over core contracts: config locations, persistence policy, auth hooks, admission/enforcement, package install/enable/pin/update policy, lifecycle ordering, timeout/failure policy, and audit hooks. |
| CLI | Thin operator entrypoints that start, discover, or attach to a hub without owning product policy. |
| Clients | Browser, TUI, socket, and custom renderers that consume hub contracts. Clients do not own provider behavior. |
| Plugins/providers | Installable behavior packages that declare capabilities, compatibility, entrypoints, provenance, checksums, enabled state, and update policy. |
| External provider implementations | Cloud federation, signaling relay, browser shell, API, and other privileged integrations implemented outside the hub crate. |

The hub should embed `botster-core` for the reusable tmux-like local engine and
shared package/capability contracts: session spawning, PTY/process mechanics,
session lifecycle and activity, subscription fanout, notifications, plugin
worker primitives, package manifests, `Capability`, `CapabilitySurface`,
host-profile admission contracts, capability runtime primitives, and consumer
conformance behavior.

## Crate layout

```text
src/lib.rs                 public facade and architecture summary
src/main.rs                thin binary smoke path through the facade
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

`botster-core` is currently sourced from the `main` branch in `Cargo.toml`;
source reproducibility relies on `Cargo.lock` until the dependency policy is
made stricter.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.
