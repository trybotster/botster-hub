# ADR: Hub As Host Profile Over Botster Core

Status: accepted for `ticket_1780376606_123665`

## Context

`botster-hub` is the first-party Botster product host around `botster-core`. It
is not a thick wrapper that forks, hides, or re-implements core mechanics. The
accepted shape is closer to a curated LazyVim-style host profile: a trusted
bundle of default policy, provider enablement, startup ordering, package
management, and first-party plugins over reusable core mechanisms.

The source of truth for concrete core surfaces is the locked `botster-core`
dependency at `a6b4a7a92a09028c9abe239ba8aab2385d7f8835`. The checked source
exposes core contracts, identity, package, runtime, actor, entity, transport,
UI, and engine modules from `crates/botster-core/src/lib.rs`. It also documents
that the default feature set enables `local-runtime`, while contract-only
embedders can disable default features and keep `BotsterEngine`, runtime traits,
and transport contracts without the local process dependency. The matching
feature definition is in `crates/botster-core/Cargo.toml`.

This ADR treats the existing hub scaffold as useful evidence, not gospel. The
scaffold currently embeds `DefaultBotsterEngine` in `src/runtime.rs` and exposes
shallow seams for config, auth, persistence, packages, providers, and adapters.
Those names are kept only where they line up with the accepted boundary model.

## Decision

`botster-hub` is a first-party host profile and plugin/provider bundle over
`botster-core`.

The hub owns product policy and trusted startup authority. Core owns reusable,
policy-free mechanisms and transport-neutral contracts. Ordinary plugins and
providers compose capabilities granted by the hub; they do not gain bootstrap,
auth, transport, secrets, package-manager, or private key authority by default.

The public command surface to embed is `BotsterEngine`, with
`DefaultBotsterEngine` as the default local PTY-backed instance when
`local-runtime` is enabled. `MultiplexerEngine` remains the lower-level assembled
primitive exposed for advanced use, not the API that ordinary hosts should wire
directly. This is derived from `crates/botster-core/src/engine/botster.rs`, where
`BotsterEngine<R, W>` wraps `MultiplexerEngine<R, W>` and where
`DefaultBotsterEngine` is guarded by `#[cfg(feature = "local-runtime")]`.

## Boundary Tiers

| Tier | Owns | Does not own | Source evidence |
| --- | --- | --- | --- |
| Non-replaceable core mechanisms | `BotsterEngine`, `DefaultBotsterEngine` when `local-runtime` is enabled, `MultiplexerEngine`, session/runtime traits, transport ingress/egress frames, actor mailbox contracts, bounded queue metadata, session I/O requests/events, client worker messages, plugin worker engine contracts, package manifest/capability types, entity frames, UI contract types, reusable crypto/envelope operations, and public device identity/fingerprint helpers. | Product startup policy, provider selection, auth/admission decisions, package install/update policy, cloud federation policy, browser shell delivery, or workflow-specific plugin behavior. | `crates/botster-core/src/lib.rs`, `src/engine/botster.rs`, `src/contract/actor.rs`, `src/contract/transport.rs`, `src/package/manifest.rs`, `src/package/capability.rs`, `src/contract/entity.rs`, `src/identity/crypto.rs`, `src/identity/device.rs` at `botster-core@a6b4a7a`. |
| Trusted host profile privileges | Startup composition, runtime config, host identity policy, client admission, provider enablement, capability grants, package install/enable/disable/pin/update policy, persistence locations, audit hooks, lifecycle ordering, timeout/failure policy, and wiring the default local runtime when this host wants local PTY execution. | Reimplementing core engine/session/actor contracts, owning raw terminal byte delivery, treating cloud/Rails/browser shell providers as hardcoded hub internals, or executing ordinary plugin callbacks inline as hub policy. | Hub scaffold `src/config.rs`, `src/runtime.rs`, `src/packages.rs`, `src/providers.rs`; vault notes `botster packages should enforce core hub cli plugin provider boundaries`, `botster cloud should be an installable privileged provider not a hub dependency`, and `botster-core local process runtime is feature-gated from contract-only embeds`. |
| Ordinary user-installed plugins/providers | Declared behavior through package manifests, entrypoints, descriptors, handlers, plugin-owned entity families, MCP/tools/resources, session actions, UI surfaces, provider implementations, and provider-specific readiness/probing. Privileged providers may request capabilities such as secrets, crypto, client admission, pairing invites, signaling relay, hub presence, or browser shell. | Implicit hub internals, unpinned privileged authority, package manager policy, raw private key material, direct ownership of client admission without a grant, terminal data-plane ownership, or global client hydration. | `PackageManifest` and `CapabilitySurface` in core; `PluginWorkerRegistration` in `src/engine/plugin_worker.rs`; `PluginHandlerRef`, `PluginOwnedDescriptor`, `PluginResourceRef`, and `PluginInvocationRequest` in `src/contract/actor.rs`; vault notes `botster package manifests and lockfiles should declare capabilities and provenance`, `botster plugin runtime uses supervisor plus per plugin workers`, and `botster plugin entities are canonical for plugin-owned dynamic state`. |

The hub profile can ship with first-party plugins and privileged providers, but
first-party does not mean core. First-party packages should still declare
capabilities and provenance so they can later be pinned, audited, disabled,
updated, or replaced through the same package policy as user-installed packages.

`default_package_policy()` is the concrete hub-owned policy gate for that
package layer in the current scaffold. It derives the grant set from
`host_profile()` default capability grants, then uses `PackageRegistry` to store
records around `botster_core::PackageManifest` values, keep hub-owned
enabled/disabled/pin/provenance/update metadata, compare requested `Capability`
values against profile-owned grants, and delegate privileged provider
host-profile admission to `botster_core::admit_host_profile`. Provider packages
without host-profile metadata are denied before enablement so provider authority
cannot bypass core admission. Accepted and denied decisions carry audit reasons
and deterministic package/action/state/classification context for operator
review. Future package lifecycle loading should call this policy before
executing plugin or provider code; this ADR does not make it a marketplace
fetcher, lockfile persistence layer, or lifecycle runtime.

## Startup Ownership

Startup proceeds in this order:

1. The host profile resolves explicit hub configuration: host identity, data
   directory, session defaults, plugin/provider directories, transport bindings,
   and core engine knobs. The current scaffold for this is `src/config.rs`.
2. The host profile initializes core mechanisms. For local PTY execution, the
   current hub path constructs `HubRuntime` with `DefaultBotsterEngine::new()`
   in `src/runtime.rs`. That is a host-profile decision to use the default
   `local-runtime` core adapter, not proof that every embedder must take the
   local process dependency.
3. The host enables privileged providers from pinned package metadata and
   explicit grants. Providers that affect trust, admission, reachability,
   pairing, signaling, registry publication, secrets, remote network access, or
   browser shell delivery load before ordinary plugins and under stricter
   timeout, exclusivity, and audit policy.
4. The host loads first-party and ordinary plugins through plugin-worker
   boundaries. Core owns reusable worker mechanics such as handler lookup,
   per-plugin capacity, capability checks, deadline attribution, reload/unload
   cleanup, and resource tracking. Concrete Lua, WASM, or process runtimes are
   supplied outside core, as stated in `crates/botster-core/src/engine/plugin_worker.rs`.
5. Clients subscribe or attach through transport-neutral contracts. Subscription
   opens a transport path; it does not hydrate all global application state.
   Opened views and UI bindings drive route, entity, and surface pulls. The
   current scaffold exposes this for local dogfood clients through
   `HubClientApi::handle_request`, an in-process request/response/event boundary
   that routes status, session, package, lifecycle, and terminal control
   requests through hub facades instead of raw core routers.

The hub owns the control plane: topology, lifecycle, discovery, authorization,
admission, routing decisions, recovery, cleanup, and provider/plugin
supervision. It must not become the byte relay for terminal output, file
payloads, scrollback, or per-client egress. Core already names `SessionIo` and
`ClientWorker` queue sources, `SessionIoRequest`/`SessionIoEvent`, and
`ClientWorkerMessage` in `crates/botster-core/src/contract/actor.rs`.

## Policy Ownership

| Policy area | Owner | Boundary |
| --- | --- | --- |
| Config | Host profile | Resolve config files, environment, data directories, plugin/provider dirs, local sockets, TCP bindings, and defaults before handing explicit requests to core. Core accepts policy-resolved requests and stable config-shaped primitives. |
| Persistence | Host profile and plugin/provider packages | Hub chooses persistence locations and durability policy. Plugins own plugin data such as `plugin.db` through granted storage capabilities. Core may define storage-capability contracts but should not own product data policy. |
| Auth and admission | Host profile plus privileged providers | Core owns reusable crypto, public identity metadata, fingerprints, envelopes, and operation contracts. Hub and enabled providers decide login, pairing, client admission, SSO, cloud federation, and audit policy. Providers receive scoped operations, not raw private key material. |
| Providers | Trusted host profile policy over installable packages | Cloud, signaling relay, browser shell, registry, external APIs, and SSO are privileged provider packages. The hub owns capability vocabulary, grant policy, lifecycle ordering, timeout policy, and audit logging. Provider implementations live outside this scaffold. |
| Marketplace and packages | Host profile policy over core contracts | Core owns manifest and capability declaration types such as `PackageManifest`, `PackageSource`, `Capability`, and `CapabilitySurface`. Hub owns install, enable, disable, pin, update, provenance checks, lockfile policy, compatibility checks, and marketplace/index resolution. |
| Transport, signaling, and clients | Split control/data plane | Core owns transport-neutral ingress/egress frames and actor contracts. Hub owns admission, routing, peer lifecycle, relay provider selection, and cleanup policy. Client workers and session I/O actors own byte-bearing stream state; clients render and adapt concrete transports. |
| Plugin lifecycle | Core worker mechanics plus host supervision | Core owns handler refs, descriptor refs, invocation/result contracts, bounded worker capacity, capability checks, cleanup scopes, and reload/unload mechanics. Hub supervises package selection, startup order, policy grants, and audit. Plugin code runs behind worker boundaries. |
| Entity and UI model state | Core contracts, plugin-owned records, client rendering | Core owns entity frame and UI contract types. Plugins publish plugin-owned dynamic state through entity frames. UI tree snapshots remain structural, and clients pull bound entity families rather than relying on subscribe-time global hydration. |
| Audit and observability | Host profile over typed core signals | Core emits typed observations, failures, backpressure summaries, lifecycle states, and invocation results. Hub decides retention, redaction, operator presentation, provider accountability, and package audit trails. |

## Risks

- Thick-wrapper drift: the hub may start hiding or duplicating `BotsterEngine`
  behavior instead of embedding it. Mitigation: keep runtime paths facade-backed
  and cite core symbols in public docs.
- Provider privilege escalation: providers can affect trust, reachability, or
  secrets. Mitigation: require explicit capabilities, provenance, pin/update
  policy, and audit records before privileged provider startup.
- Product policy leaking into core: workflow defaults, cloud assumptions, Rails
  coupling, and Project Pipelines behavior must stay in host profile,
  providers, plugins, or templates. Core keeps mechanisms and contracts.
- Hub data-plane gravity: terminal bytes, scrollback, and file payloads can
  creep back through hub events. Mitigation: keep data-plane behavior in
  SessionIo and ClientWorker paths while the hub owns control-plane decisions.
- Plugin execution in hub paths: slow plugin callbacks can block client/session
  paths if they execute inline. Mitigation: route plugin-owned handlers through
  worker boundaries with bounded capacity and timeout attribution.
- Documentation outpacing enforcement: the north-star boundary is already
  clearer than all package/runtime enforcement (`botster north star is sound but
  stale boundaries remain`). Mitigation: document first, then audit seams, then
  enforce with tests and package layout once current surfaces agree.
- Dual-path migration ambiguity: preserving old and new boundary names for too
  long will make ownership unclear. Mitigation: when a boundary becomes
  enforceable, replace old names cold turkey unless a real deployment boundary
  requires temporary compatibility.
- Feature-gated docs drift: references to `DefaultBotsterEngine` can become
  wrong for contract-only embedders. Mitigation: call it the default
  `local-runtime` instance and keep `BotsterEngine` as the always-available
  facade.

## Migration Path For `botster-hub`

1. Keep the current shallow crate as a host-profile scaffold. It is useful for
   naming policy seams, but it is not the final authority.
2. Make this ADR the public discovery point for core/host/plugin/provider
   boundaries, linked from `README.md`.
3. Audit current scaffold modules against this ADR:
   `src/runtime.rs`, `src/config.rs`, `src/auth.rs`, `src/persistence.rs`,
   `src/packages.rs`, `src/providers.rs`, and `src/adapters/*`.
4. Keep runtime proof paths facade-backed. The current `HubRuntime` should
   continue to use core's default local facade for local execution rather than
   assembling `MultiplexerEngine` directly.
5. Move product policy into host-profile/provider/plugin packages without
   widening core. Cloud federation, signaling relays, browser shell delivery,
   SSO, package indexes, marketplace UX, and workflow apps should compose core
   mechanisms through explicit capability contracts.
6. Treat package manifests and lockfiles as the installable expression of the
   boundary. Add provenance, checksums/signatures, compatibility, grants,
   enabled state, and update policy before privileged provider installation
   becomes broad.
7. Make plugin lifecycle boundaries enforceable: descriptor registries in the
   parent hub, executable behavior in per-plugin workers, resource cleanup on
   reload/unload, bounded queues, and timeout attribution.
8. Keep client subscription lightweight. Browser, TUI, socket, and custom
   clients should pull route registries, surfaces, and bound entity families
   when views need them.
9. Convert documented boundaries into compile-time/package/test enforcement
   only after the ADR, README, current scaffold, and locked core surfaces agree.

This migration path is intentionally documentation-first. The ticket acceptance
does not require runtime implementation beyond the current scaffold and public
doc discovery path.
