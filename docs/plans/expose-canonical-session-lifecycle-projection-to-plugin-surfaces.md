# Expose canonical session lifecycle projection to plugin surfaces

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket: `ticket_1785295607_887142`; run
  `run_1785296206_981403`.
- Repository routing was resolved from the admitted Botster spawn-target
  registry, not from the process working directory. The assigned worktree is
  clean at `35e92f46a98c445765b6ba7755e029f5dde702f8`, the current
  `trybotster/botster-hub` main commit at Plan time.
- Role and repository playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], and [[botster-hub-playbook]].
- Surface guidance: [[botster-runtime-reviewer-playbook]] for the daemon,
  lifecycle subscription, bounded delivery, and reconnect path; and
  [[botster-package-reviewer-playbook]] for the real plugin-worker fixture and
  generated Hub test-support package. [[project-pipelines-playbook]] is
  intentionally not loaded because this ticket does not change Project
  Pipelines package/plugin paths or workflow policy.
- Architecture maps and targeted notes:
  [[botster-architecture]], [[cli-patterns]], [[spa-patterns]],
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[botster packages should enforce core hub cli plugin provider boundaries]],
  [[session UUID is the sole routing key across all layers]],
  [[botster hub client state sync is entity frame only]],
  [[botster entity snapshots are authoritative reconnect baselines]],
  [[botster client subscriptions should not hydrate global state]],
  [[plugin surfaces request model state through ui bindings not hub subscribe]],
  [[botster plugin entity hydration has full id and scoped contracts]],
  [[ui bind list where filters plugin entity rows before template expansion]],
  [[ui bind list empty template renders entity backed empty rows]],
  [[botster workspace records are plugin owned references not hub authority]],
  [[plugin capability tests must validate against real lua runtime table not injected stubs]],
  [[plugin tests must prove worker boundaries not hub leakage]],
  [[botster wire v2 clients must consume ui tree snapshots and render composites with entity stores]],
  [[botster first party client support matrices belong in hub test support]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]],
  [[conformance fixture revisions must be unique per published content]], and
  [[hub test support npm releases need external consumer smoke]].
- Repository evidence inspected: root and package READMEs; current crate layout;
  `HubRuntime` and `HubClientApi`; daemon request/response and connection-scoped
  entity delivery; the sanitized `DaemonSessionEntity` and
  `DaemonEntityFrame` contract; the existing lifecycle baseline/delta
  projector; `botster-ui-contract` binding grammar; the packaged
  plugin-contract-matrix; the Rust/Node Hub test-support generators and
  conformance runners; real daemon lifecycle, client API, and Lua worker tests;
  `test.sh`; and the loaded-daemon CI workflow.
- Downstream code was inspected read-only at current authoritative refs to
  identify ownership seams. Neither shipped generic client currently consumes
  this contract: botster-web at `e044484` has no canonical `$bind`,
  `bind_list`, or `bind_if` resolver and silently drops canonical bound
  children in its legacy adapter; botster-tui at `0d26ce0` and
  botster-tui-kit at `22df686` render `bind_list` as `empty_template` without
  an entity store. Those repositories are not edited here.
- Human answer `question_1785297681_855888` selects a staged cold switch:
  this Hub run owns the `/session` producer, admission, contract, and
  Hub-authored reference conformance. Shipped-client runtime proof belongs to
  downstream Web ticket `ticket_1785298229_125024` and TUI ticket
  `ticket_1785298229_854008`, both registered as depending on this Hub ticket.
- Registry preflight on 2026-07-28: public latest is
  `@trybotster/hub-test-support@0.1.14`, protocol version 4 / conformance
  revision 22, with `@trybotster/ui-contract@0.1.0`. The current main checkout
  still records Hub test-support `0.1.13` with the same protocol/revision.
  Implementation must reconcile that immutable published coordinate before
  selecting the next version; it must not republish or overwrite `0.1.14`.

## Contract decision

Choose the ticket's bindable-entity option, not a Lua worker read capability.

The canonical plugin-authored path is the Hub-owned `/session` entity family.
Its row id is the canonical session UUID, and its delivered row is the existing
sanitized `DaemonSessionEntity` projection. Plugin trees may bind a referenced
UUID through direct paths such as `/session/<uuid>/lifecycle` or through
`bind_list` with an exact top-level `session_uuid` filter. The production
transport remains `SubscribeEntities { entity_type: "session" }` on a held-open
connection; `/session` is the renderer-neutral UiNode name for that same
family, not a second store or subscription.

Lifecycle classification is Hub contract, not workspace policy:

- `starting`, `running`, and `stopping` are current.
- `exited` and `failed` are ended.
- A present row with `lifecycle: None`, including `registry_state: "stale"`,
  is indeterminate.
- A UUID absent from the authoritative snapshot/family is unknown or
  unavailable.
- Session shutdown changes lifecycle but does not remove the row. Only the
  existing explicit Hub retention/removal operation produces
  `entity_remove`.

Every present session row must expose an always-present, Hub-derived
`lifecycle_class` with exactly `current | ended | indeterminate`. This field is
the generic binding input; it is not workspace status. Snapshot/upsert/patch
projection must set it explicitly on every present row, including transitions
from a concrete lifecycle to `None`, so merge-patch consumers never retain a
stale class because an optional source field was omitted. Absence from the
family—not an omitted lifecycle field—is the only unknown/unavailable state.

This option is the smallest contract that satisfies push and reconnect
semantics. A scoped Lua read would return a correct point-in-time projection,
but the current production surface path has no generic active-surface
invalidation/re-render mechanism. Adding one would be a new orchestration
system and would still require client-specific refresh behavior, contradicting
the ticket.

## Scope

1. **Name and enforce the canonical binding seam.**
   - Document `/session` as the only built-in Hub lifecycle entity family
     available to plugin-authored UiNode bindings.
   - Keep session UUID as row identity and preserve the existing sanitized row
     fields. Do not expose Core records, process handles, commands, session
     context, package state, worktree state, or arbitrary Hub storage.
   - Bind only against the connection's already admitted `session` entity
     subscription. A plugin worker authors paths and templates; it does not
     receive the entity values or a raw Hub-state handle.
   - After generic UiNode validation, enforce a Hub-owned binding-family
     admission pass at the existing plugin-surface validation boundary:
     accept `/session`, preserve separately sanctioned owner-namespaced plugin
     families, and reject unauthorized built-in/foreign absolute families.
     Do not create a general Hub entity introspection API.

2. **Add a real plugin-worker producer.**
   - Extend the canonical plugin-contract-matrix source with one surface that
     accepts or contains a bounded set of referenced UUIDs and returns a
     Hub-validated UiNode tree using `/session` bindings.
   - The fixture must demonstrate matching current, ended, and indeterminate
     rows before demonstrating unknown/missing through
     binding/filter/empty-template grammar.
   - Load and invoke that fixture through the real package registry,
     supervisor, plugin worker VM, `surface_route` registration,
     `HubRuntime::render_plugin_surface`, client API, and daemon response path.
     A hand-authored Rust tree or injected Lua global is complementary only.

3. **Compose the tree with authoritative connection-scoped lifecycle frames.**
   - Reuse the existing lifecycle baseline/delta projector and bounded
     subscription delivery. No polling, `ListSessions` refresh, plugin
     lifecycle cache, or workspace-owned status field is added.
   - Prove an initial snapshot materializes referenced current, ended,
     indeterminate, and absent UUIDs; ordered lifecycle patches move a
     referenced row current -> ended -> indeterminate with an explicit
     `lifecycle_class` update; explicit remove makes the reference unknown
     without deleting the plugin-owned reference.
   - Drop the connection, create a fresh subscription generation, and require
     a new authoritative snapshot before accepting deltas. The rematerialized
     plugin view must converge without a client-specific list or surface
     refresh.
   - Preserve the existing overflow contract: authoritative snapshot resync or
     subscription close on failed resync. Do not add an unbounded queue or
     timing-only retry.

4. **Publish one Hub-owned reference conformance contract.**
   - Add a source-derived Hub test-support scenario/report that combines the
     real plugin surface tree with public `DaemonEntityFrame` values and
     materializes renderer-neutral current, ended, indeterminate, and
     absent/unknown results.
   - Expose the same scenario through Rust and JSON/Node as reference
     materializers for downstream client tickets. Both shapes must consume the
     canonical UiNode tree and public daemon DTOs rather than parallel
     lifecycle structs. They prove producer/fixture self-consistency, not the
     shipped botster-web or botster-tui runtime path.
   - Update the first-party support matrix to name the canonical `/session`
     path, lifecycle-class mapping, missing-row semantics, real runtime runner,
     reference materializers, and the two downstream consumer tickets. State
     plainly that shipped Web/TUI rendering is not supplied by this Hub
     artifact. Do not claim general plugin-owned entity hydration if it is
     still unsupported.
   - Advance the conformance fixture revision because the public fixture/matrix
     meaning changes. Change the daemon protocol version only if implementation
     changes required request/response compatibility rather than composing
     existing public frames.

5. **Regenerate and prepare the normal test-support artifact.**
   - Generate every checked Rust/Node fixture, matrix, metadata, declaration,
     and plugin mirror from its authoritative source. Never hand-edit generated
     copies.
   - Reconcile the published `0.1.14` artifact with repository history, then
     select the next unused patch (expected `0.1.15`, subject to preflight).
   - Pack and test the exact tarball in clean browser-shaped and
     terminal-shaped consumers. If npm credentials or 2FA prevent publication,
     stop with merged/packed artifacts and report the one exact operator
     publish command; do not invent a local override or publishing-only ticket.

6. **Update current contract documentation.**
   - Document `/session`, subscription lifetime, row shape, lifecycle mapping,
     unknown/removal semantics, and reconnect/resync behavior in the public
     client protocol and Hub/test-support READMEs.
   - State that Workspaces owns references and presentation/grouping policy;
     Hub owns only canonical session identity/lifecycle and sanitized delivery.
   - Keep historical plans/reports historical unless the implementation itself
     makes one of their current claims misleading.

## Non-scope

- No `botster-workspaces` schema, grouping record, detail-route, action, spawn,
  rename, delete, move, or product presentation implementation.
- No persistence of lifecycle or `current`/`ended` truth in a plugin database.
- No workspace-specific Hub DTO, capability, entity family, surface id, or
  policy branch.
- No arbitrary Hub-state read capability, raw CoreDaemon handle, Lua session
  registry exposure, process metadata, terminal bytes, history, screen, files,
  packages, targets, worktrees, or session contexts.
- No client-triggered `ListSessions`, imperative list refresh, polling loop,
  duplicated session model, or surface re-render protocol.
- No terminal data-plane changes; SessionIo/ClientWorker remain owners of
  terminal bytes and attach history.
- No edits to `botster-web`, `botster-tui`, `botster-tui-kit`,
  `botster-workspaces`, `botster-core`, or Project Pipelines source in this run.
- No speculative generalized entity-query language, join engine, negative
  predicate system, new renderer, broad UiNode redesign, or adjacent cleanup.

## Repository ownership boundaries and cross-repository dependencies

- **botster-core:** remains authoritative for session lifecycle records,
  baseline/cursor ordering, and lifecycle transitions. Hub consumes the
  lockfile-pinned Core APIs. If the required authoritative state or ordering is
  absent, stop and register a prerequisite against the botster-core target
  rather than copying it into Hub.
- **botster-hub:** owns host admission, the sanitized `/session` projection,
  connection-scoped subscription delivery, plugin-worker/surface validation,
  compatibility advertisement, conformance fixture, real runtime harness, and
  normal test-support artifact.
- **In-repository botster-hub-client:** owns public daemon request, entity frame,
  projection DTO, generated TypeScript, feature, and compatibility metadata.
  No Hub-local DTO mirror is allowed.
- **In-repository botster-ui-contract:** owns renderer-neutral binding grammar
  and validation only. `/session` is Hub family policy documented beside the
  Hub protocol; the UI package must not acquire workspace policy or runtime
  access.
- **botster-workspaces:** downstream ticket
  `ticket_1785296184_677408` consumes this seam after it lands. It owns stored
  references, current/ended layout, and unknown-reference presentation. It is
  registered as depending on this Hub ticket plus both client consumer
  tickets, because its owner-authored detail surface cannot render the seam
  until both generic clients resolve it.
- **botster-web:** downstream ticket `ticket_1785298229_125024`, target
  `tgt_40abcf71ccf049f4ac0c99953a799869`, owns the production browser
  entity-store/binding resolver and is registered as depending on this Hub
  ticket.
- **botster-tui:** downstream ticket `ticket_1785298229_854008`, target
  `tgt_c3d470bab78549df920a41e8fb0e58d8`, owns the production terminal client
  entity-store/binding resolver and is registered as depending on this Hub
  ticket. Any reusable TUI-kit gap is separately routed by that ticket.
- This Hub run supplies shared producer/reference conformance but cannot claim
  shipped Web/TUI runtime proof. Do not add client-specific aliases in Hub to
  hide either downstream gap.
- **Final integration:** `ticket_1785192726_335558` owns the eventual complete
  Workspaces browser/TUI click-through. It does not replace this ticket's real
  Hub/plugin-worker proof or the named downstream clients' runtime proof.
- No open prerequisite blocks this Hub producer. Dependency direction is Hub
  producer -> Web/TUI consumers -> Workspaces lifecycle consumer; the existing
  production session subscription, plugin surface path, and generic binding
  grammar are present on the Hub run base.

## Assumptions and unknowns

- Binding decision: existing `UiBind`, `UiBindList.where`, and
  `empty_template` semantics are the selected grammar. The Hub reference
  materializer must prove matching current, ended, and indeterminate records
  before absence, including a negative control that a matching UUID never
  selects `empty_template`.
- Ask-human threshold: if missing-reference behavior cannot be expressed
  without a new renderer-neutral binding primitive, stop and present that
  precise contract choice rather than silently adding a query language or
  weakening unknown behavior.
- Contract decision: the existing optional `lifecycle` field is insufficient
  to distinguish a present unclassified/stale row from an absent UUID. Every
  present row therefore carries required `lifecycle_class:
  current | ended | indeterminate`.
- Assumption: a plugin-authored binding does not grant the worker raw state;
  values remain in the admitted client entity store. Any implementation that
  sends entity values into the worker needs a fresh security/ownership review.
- Assumption: connection-scoped full session snapshots remain the current
  generic client contract. The plugin fixture scopes what it renders to its
  referenced UUIDs; this ticket does not redesign subscription-level server
  filtering.
- Unknown: the narrowest stable Hub reference report/materialization API
  shared by Rust and Node. Prefer existing public UiNode and daemon DTO
  serialization plus relational assertions over a second view-model schema;
  never describe this reference code as a shipped Web/TUI adapter.
- Unknown: whether a new compatibility feature constant is necessary. Add one
  only if clients must explicitly negotiate canonical plugin session binding;
  if added, update advertised features, required features, and support-matrix
  expectations together.
- Unknown: why public Hub test-support `0.1.14` is ahead of the checked main
  package version while retaining revision 22. Reconcile provenance before
  changing version/revision metadata.
- No loaded convention conflicts or requested waivers are known.

## Affected surfaces and likely files

- `docs/plans/expose-canonical-session-lifecycle-projection-to-plugin-surfaces.md`
  — this reviewable plan.
- `docs/client-protocol.md` and `README.md` — canonical `/session` binding,
  lifecycle mapping, subscription/reconnect semantics, and ownership boundary.
- `crates/botster-hub-client/src/lib.rs` and
  `crates/botster-hub-client/src/typescript.rs` — canonical constants or feature
  metadata, public projection compatibility only if needed, and generated DTO
  tests.
- `crates/botster-hub-client/generated/daemon-protocol.ts` — generated output
  only if the public client contract changes.
- `src/daemon_transport.rs` — existing session projection and connection-scoped
  delivery tests; production changes only where the canonical binding seam
  requires them.
- `src/runtime.rs` / `src/client_api.rs` — Hub-owned binding-family admission
  after generic UiNode validation plus preservation of the existing real
  plugin surface render path.
- `fixtures/plugins/plugin-contract-matrix/plugin.lua`,
  `fixtures/plugins/plugin-contract-matrix/botster-package.json`, and
  `fixtures/plugins/plugin-contract-matrix/README.md` — authoritative packaged
  binding producer and documentation.
- `crates/botster-hub-test-support/src/lib.rs` — combined
  plugin-surface/session-entity conformance scenario, real runner/report,
  support matrix, Rust reference materialization, and source-equality tests.
- `crates/botster-hub-test-support/examples/node_package_assets.rs` — generated
  asset metadata/emission.
- `tests/hub_lua_runtime_test.rs` — focused real worker registration/render
  proof if not fully covered by the reusable runner.
- `tests/hub_daemon_lifecycle_test.rs` — exact
  HubDaemon/HubRuntime/CoreDaemon/session-worker subscription, transition,
  removal, disconnect, and reconnect proof through the runner.
- `packages/hub-test-support/scripts/sync-assets.mjs`,
  `package.json`, `index.js`, `index.d.ts`, `test.mjs`, and `README.md` —
  generated asset pipeline, exports, Node reference materialization, version,
  tests, and consumer instructions.
- `packages/hub-test-support/fixtures/plugin-contract-matrix/**` — generated
  plugin fixture copies only.
- `packages/hub-test-support/metadata.json`,
  `packages/hub-test-support/first-party-client-support-matrix.json`, and the
  new root-level session-plugin-binding conformance JSON — generated contract
  copies only.
- `packages/ui-contract/README.md` and existing contract tests only if
  clarifying the generic absolute-path grammar is required. No schema/type
  change is expected by default.
- A versioned implementation/release report under `docs/reports/` if the normal
  Hub test-support artifact changes or publishes.

## Risks and mitigations

- **Point-in-time truth masquerades as reactive state:** require one held-open
  session subscription and prove transition plus reconnect materialization.
  Do not accept render-time Lua reads or repeated surface render requests.
- **Plugin gains arbitrary Hub access:** keep values out of the worker, expose
  only the sanitized `/session` family to the client binding resolver, and
  reject any need for raw Hub/Core state.
- **Workspace policy leaks into Hub:** document only lifecycle states and
  the generic `current | ended | indeterminate` classification; keep labels,
  ordering, retention of references, and product actions in Workspaces.
- **Present-but-unclassified rows collapse into unknown:** require the
  always-present `lifecycle_class`, map `lifecycle: None` and stale registry
  rows to `indeterminate`, and reserve unknown/unavailable for an absent UUID.
- **A concrete-to-None patch leaves stale client state:** projection and patch
  tests must prove current -> ended -> indeterminate explicitly updates
  `lifecycle_class`; omitted optional source fields cannot be the clear
  mechanism.
- **Unknown UUIDs disappear or false-pass through an always-empty renderer:**
  conformance must materialize matching current, ended, and indeterminate rows
  first, prove those rows do not select `empty_template`, then include a
  never-known UUID and an explicitly removed UUID that retain the owner-authored
  reference and render the deliberate unavailable state.
- **Shutdown is mistaken for deletion:** prove exited/failed rows remain until
  explicit removal; only `entity_remove` creates absence.
- **Reconnect accepts stale generation deltas:** require a new subscription id
  and authoritative snapshot before deltas; preserve snapshot baseline
  semantics and old-connection cleanup.
- **Overflow creates silent gaps:** reuse bounded delivery and explicit resync
  snapshot/close policy; never paper over overflow with retries or polling.
- **Static fixture claims production registration:** install, enable, and
  render the real packaged plugin through the worker VM and real daemon path.
- **Web and TUI learn different family aliases:** publish one `/session` path
  and one source-derived scenario. Reject `botster-web.session`,
  TUI-local names, or renderer-specific fields from the authored tree.
- **Reference conformance is mistaken for shipped-client proof:** Hub's Node
  and Rust materializers must execute the delivered tree and public frames,
  but their reports must be labelled producer/reference self-consistency.
  Shipped runtime proof belongs only to the registered Web and TUI tickets.
- **Current client implementation gap is hidden:** record the verified failing
  client baseline and exact downstream ticket IDs. Do not claim full final
  integration or add a Hub compatibility alias.
- **Public artifact collision/drift:** preflight the registry and compare
  `0.1.14` metadata/integrity before selecting a version; allocate a unique
  conformance revision and use generated equality checks.
- **Test harness leaks processes or uses wrong binaries:** use the existing
  isolated Hub builder with explicit Hub and lockfile-pinned worker provenance,
  bounded waits, shutdown, and kill/wait fallback.

## Acceptance checks and downstream proof

1. **Focused contract and projection tests**
   - `./test.sh --locked -p botster-ui-contract` if binding validation/docs
     require a contract change; otherwise keep the crate untouched.
   - `./test.sh --locked -p botster-hub-client` proves public session entity
     serde, feature/compatibility metadata, and TypeScript generation.
   - Focused `src/daemon_transport.rs` tests prove every lifecycle state maps
     to the documented `current | ended | indeterminate` class, every present
     row serializes that field, concrete -> `None` and stale transitions
     explicitly patch it to `indeterminate`, patches preserve the canonical
     UUID, explicit remove creates absence, and no extra Hub state enters the
     row.
   - Focused Hub surface-admission tests accept canonical `/session` bindings,
     preserve explicitly sanctioned owner-namespaced plugin families, and
     reject unauthorized built-in or foreign absolute families after generic
     UiNode validation.

2. **Real plugin-worker registration and render**
   - A focused
     `./test.sh --locked --test hub_lua_runtime_test <session-binding-test> -- --exact --nocapture`
     installs/enables the canonical fixture, observes its real
     `surface_route` descriptor, renders through the plugin worker, and
     validates the delivered tree contains only canonical `/session`
     dependencies scoped to the authored UUID set.
   - The same proof must fail if replaced with an injected capability/global
     or a hand-authored response that bypasses worker registration.

3. **Real lifecycle, push, remove, and reconnect path**
   - A focused
     `./test.sh --locked --test hub_daemon_lifecycle_test <session-plugin-projection-test> -- --exact --nocapture`
     uses the reusable conformance runner against
     `HubDaemon -> HubRuntime -> CoreDaemon -> botster-session-worker`.
   - It proves initial current/ended/indeterminate/absent materialization,
     ordered current -> ended -> indeterminate transition without list/surface
     refresh, explicit removal to unknown, concurrent connection isolation,
     disconnect cleanup, and a fresh authoritative reconnect snapshot.
   - Existing deterministic overflow tests remain green and assert snapshot
     resync or close rather than a hidden delta gap.

4. **Generated artifact and Node reference materialization**
   - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
   - `npm test --prefix packages/hub-test-support`
   - The Node test imports the generated scenario, applies public session
     frames to a generic entity store, resolves the delivered `/session`
     bindings, and asserts matching current, ended, and indeterminate rows
     before the absent UUID selects `empty_template`. It must include the
     negative control that a matching UUID never selects `empty_template`, and
     assert the same distinction after patch/remove/reconnect. JSON existence
     or token checks do not pass this gate.
   - This is a Hub-owned reference materializer. It is not botster-web runtime
     evidence and must not be labelled Web compatibility.

5. **Rust reference materialization**
   - `./test.sh --locked -p botster-hub-test-support`
   - The Rust test consumes the same delivered UiNode and public entity frames
     and asserts matching current, ended, and indeterminate rows before the
     absent UUID selects `empty_template`, including the same matching-row
     negative control and no client-specific family alias.
   - This is a Hub-owned reference materializer. It is not botster-tui runtime
     evidence and must not be labelled TUI compatibility.

6. **Package and external clean-consumer proof**
   - Recheck `npm view @trybotster/hub-test-support version dist-tags --json
     --prefer-online` and exact candidate-version availability.
   - Run `npm pack --dry-run --json` and inspect that the new fixture, matrix,
     declarations, plugin mirror, and metadata are present once.
   - Install the exact packed tarball plus `@trybotster/ui-contract@0.1.0` into
     a clean temporary Node consumer and run the Node reference
     materialization.
   - Run the repository's Rust external-client/subprocess smoke against fresh
     Hub and lockfile-pinned session-worker binaries.
   - If published in this run, verify the public integrity/metadata and repeat
     the clean install from the registry. If 2FA blocks publication, attach the
     packed evidence and report:
     `cd packages/hub-test-support && npm publish --access public`.

7. **Repository gates**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
   - `./test.sh --locked`
   - Run the loaded-daemon focused Lua/lifecycle campaign if the focused real
     runtime test exposes timing, cleanup, or pressure behavior not covered by
     one deterministic invocation.
   - Record exact Hub SHA, Core SHA from `Cargo.lock`, and resolved fresh binary
     realpaths for live evidence.

8. **Downstream shipped-client proof (not a Hub completion claim)**
   - Web ticket `ticket_1785298229_125024` against
     `tgt_40abcf71ccf049f4ac0c99953a799869` must run the packed scenario through
     the real botster-web browser renderer/entity store and prove
     current/ended/indeterminate, absent-only unavailable, patch, removal, and
     reconnect behavior. Its expected baseline is currently failing because
     the legacy adapter drops canonical bound children.
   - TUI ticket `ticket_1785298229_854008` against
     `tgt_c3d470bab78549df920a41e8fb0e58d8` must run the same scenario through
     the real botster-tui application renderer/entity store with matching-row
     negative controls. Its expected baseline is currently failing because
     every `bind_list` resolves to `empty_template`.
   - Those tickets depend on this Hub producer and are required before
     Workspaces lifecycle ticket `ticket_1785296184_677408` can claim actual
     browser/TUI detail-surface proof. Their current expected failure does not
     fail this producer-only Hub run, and Hub reference tests cannot substitute
     for their eventual passing runtime evidence.

9. **Traceability**
   - Every changed line must map to canonical `/session` documentation,
     production plugin registration/binding, public conformance generation,
     lifecycle/reconnect proof, or cleanup made necessary by those changes.
   - The implementation report must name the registered Web and TUI downstream
     tickets, state plainly that shipped-client proof is outside this Hub run,
     record registry coordinate/integrity if published, and list every
     deviation from this plan.

## Vault gaps worth capturing

- The durable candidate is a focused note that Hub-owned entity families may
  be bindable from plugin-authored UiNode trees without transferring raw state
  authority to the plugin worker, provided the family/path allowlist and
  connection-scoped delivery contract are explicit.
- Capture that only after implementation proves the exact production path,
  family name, unknown semantics, and downstream consumer behavior. Until then,
  this plan is evidence of intent, not a shipped convention.
- A second capture may be warranted if the existing binding grammar cannot
  represent per-reference absence without a new primitive. Record the proven
  limitation and approved replacement contract, not speculative design.
