# Hub: project session state without blocking operation paths

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Spawn-target name: `botster-hub`.
- Authoritative target path is the admitted spawn-target path, not the
  ambient process working directory.
- Pipeline ticket: `ticket_1786663582_169720`.
- Run: `run_1786689005_381068`.
- Project: Botster Non-Blocking Event Plane, Stage A Hub slice.
- Assigned worktree is the pipeline-created Hub worktree for this ticket.
- Plan-time HEAD: `173e528` (`Align the UI contract plan and protocol
  examples with the Git tag.`).
- Locked Core in this worktree: `033cd01`. Core `main` already contains the
  closed parent APIs (`5e1c1fa` journal wake/page, `bb334d7` class-aware
  admission, `b832e47` Hub-shaped consume recovery). Implement must refresh
  `Cargo.lock` to current Core `main` before compiling against those APIs.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role overlays:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Planner must-load maps and orchestration notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]

Hub charter notes implicated by this ticket:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub synchronizes plugin workers with session lifecycle events and a baseline]]
- [[worker isolation now has a Core try-admit non-blocking primitive]]
- [[worker isolated and non blocking are different dispatch guarantees]]
- [[Core class-aware plugin admission reserves request-response executors]]
- [[package entity hydration uses explicit providers not mcp naming]]
- [[botster plugin entity hydration has full id and scoped contracts]]
- [[plugin surfaces request model state through ui bindings not hub subscribe]]
- [[botster hub client state sync is entity frame only]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[session UUID is the sole routing key across all layers]]
- [[hub shutdown preserves durable session workers]]
- [[session wide drains cannot deliver subscription owned initial state]]

Hub-client surface overlay (in-repo crate; DTO/feature changes only if
Implement must add client-visible session snapshot frames):

- [[botster-hub-client-playbook]]
- [[botster hub client crate is the external client boundary]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[generated typescript dtos must encode serde field optionality]]

Process notes:

- [[vault example paths are not repository placement conventions]]
- [[plan steps need reviewable plan artifacts]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Intentionally not loaded:

- [[project-pipelines-playbook]] — this ticket does not change Project
  Pipelines package/plugin paths or workflow policy.
- [[botster runtime teardown lenses]] — this ticket is not runtime-teardown
  class. It removes Hub terminal Drain from lifecycle inference; it does
  not implement WebRTC/peer teardown, SessionIo/ClientWorker teardown,
  multi-peer ownership, or CPU/battery/FD spin. Core already answered
  teardown lenses on the journal parent.
- Session-type eligibility consumer pins — this ticket is not a consumer
  of Hub session-type eligibility.

## Context loaded

Repository evidence:

- Root `README.md` production path: `HubDaemon` / `HubRuntime` /
  `CoreDaemon`. `docs/plans/` is the living plan home (135 mainline
  plans; no retired-directory stub).
- `src/daemon_entity_subscriptions.rs` owns one shared
  `EntityReconciliationState` cursor plus `drive_entity_subscriptions`.
- That pump early-returns when `entity_subscriptions` is empty after
  package fanout/resync. Zero Web/TUI subscribers therefore stop session
  projection. Ticket acceptance forbids that.
- The same pump calls `HubRuntime::drain_runtime_once` for every
  non-exited, non-attached session. That is terminal Drain used to
  discover lifecycle. Ticket forbids it.
- Owner loop (`src/daemon_transport.rs`) also calls
  `drive_entity_subscriptions` after Spawn / Resize / ShutdownSession /
  RemoveSession when any subscriber exists, and calls
  `drive_package_entity_fanout` plus provider resync after every control
  reply.
- `HubRuntime::session_lifecycle_changes` still uses unbounded
  `CoreDaemon::lifecycle_changes`. It does not call
  `observe_lifecycle`, `take_journal_advanced_wake`, or
  `lifecycle_changes_page`.
- `HubRuntime::invoke_plugin` is blocking. `emit_plugin_event` and
  package `entity_provider` / MCP / UI paths all wait on `invoke`.
  Request-response MCP/UI/subscribe snapshots may keep blocking
  `invoke`. Background lifecycle and session-family delivery must move
  to `try_admit(Background)` plus bounded `drain_completions`.
- `src/maintenance.rs` is software-update identity, not owner-loop
  slices. New session-projection maintenance belongs in a dedicated
  daemon-control module, not that file.
- Client session wire is `DaemonEntityFrame::{Snapshot,Upsert,Patch,Remove}`
  at protocol 7 / conformance 38. Plugin UI bindings already consume
  Hub-owned `/session` through that family.
- Existing lifecycle classification in `session_lifecycle_class` already
  matches the ticket's ended-evidence rule: stale or missing lifecycle
  is `indeterminate`; `exited`/`failed` is `ended`; remove is not ended.
- Existing tests
  `session_entity_subscription_observes_natural_exit_without_terminal_attach`
  and the focused idle counter that requires
  `lifecycle_session_drains` to increase are the production-path
  oracles that must change meaning.

Closed Core parents (already registered; do not re-open):

- `ticket_1786663581_962361` on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`
  (`botster-core`): `observe_lifecycle`, coalesced
  `take_journal_advanced_wake`, bounded `lifecycle_changes_page`.
- `ticket_1786663581_723222` on the same Core target:
  `PluginInvocationClass::{RequestResponse,Background}`, `try_admit`,
  `drain_completions`.

Core's isolated Hub-shaped consumer
(`botster-core-test-support` `hub-lifecycle-shaped`) is the consume
contract this Hub run must adopt:

1. `observe_lifecycle` is the progress tick. Page/wake/baseline do not
   advance runtimes.
2. Safe consume order: take, page until `next == watermark` or resync,
   take again, re-page if that second take is true.
3. Never page-then-take-then-sleep.
4. `BudgetTooSmall` raises `max_bytes` to `minimum_bytes`. It is not
   catch-up and not sleep.
5. Empty successful page after a valid budget recovers through a fresh
   `lifecycle_baseline`.
6. Existing unbounded `lifecycle_changes` is compatibility only.

## Scope

One Hub-internal session projection that stays live without client
subscribers, without blocking operation handlers, and without using
terminal Drain to infer lifecycle.

1. Refresh `Cargo.lock` to Core `main` that contains both closed
   parents. Record the locked Hub SHA and locked Core SHA separately.
2. Split the owner loop so a ready Spawn, Attach, Drain, Input, Resize,
   Shutdown, MCP, UI, or entity-mutation request is handled without
   `drive_entity_subscriptions`, package fanout, provider resync, or
   background plugin invocation. After an authoritative mutation, set
   at most one O(1) coalesced `try_wake` bit.
3. Keep one Hub lifecycle cursor and one canonical in-memory session
   projection, independent of subscriber count.
4. Process work in round-robin maintenance slices. One owner turn runs
   at most one slice, then yields to a ready operation. Slice kinds:
   - observe (`CoreDaemon::observe_lifecycle`)
   - journal pull (take wake + one bounded `lifecycle_changes_page`)
   - projection apply
   - host-bridge fulfillment (pending RequestResponse bridges plus
     authorized session-family delivery)
   - subscriber delivery
   - completion drain (`drain_completions`)
   - package-entity provider resync
5. Bound every slice by item count, encoded byte count, and elapsed
   time. Publish those budgets as named test constants.
6. Deliver Hub-owned `/session` to any authorized synthetic plugin
   through `snapshot_begin`, bounded `snapshot_chunk`, and
   `snapshot_end` at one snapshot sequence. Queue live deltas behind
   `snapshot_end`. Pressure or handler failure marks a gap and requires
   a complete baseline. Session state frames never expire.
7. Keep client `SubscribeEntities { entity_type: "session" }` on the
   existing `entity_snapshot` / upsert / patch / remove host-control
   contract unless a single snapshot exceeds `DAEMON_MAX_FRAME_BYTES`.
   Do not raise the default client requirement. Do not bump
   `PROTOCOL_VERSION` for an additive plugin host-bridge.
8. Move background plugin work (`emit_plugin_event` and session-family
   delivery) to `try_admit(Background)`. Keep blocking `invoke` only for
   RequestResponse MCP, UI render/action, and package-provider subscribe
   snapshots.
9. Add Hub source tests that fail if the new projection path imports
   terminal semantic bodies or names Workspaces/membership/package
   cleanup policy.
10. Update the existing Drain-based natural-exit and
    `lifecycle_session_drains` proofs to the observe/page/projection
    path.

## Non-scope

- Package event declarations, `events.emit`, the Send-safe event
  router, or client `SubscribeEvents`. Those are later Hub tickets in
  this project.
- Web or TUI UI work. Downstream clients keep consuming the existing
  session entity frames.
- Workspaces membership, cleanup rules, or any package-specific
  session policy.
- Changing ClientWorker, SessionIo, attach phase machines, GHOSTSNP,
  or terminal Drain semantics. Terminal Drain remains a terminal-plane
  operation for attached subscribers.
- Automatic `remove_session` after exit. Session shutdown still does
  not remove the row.
- Replacing RequestResponse `invoke` for MCP/UI with `try_admit`.
- Publishing `@trybotster/hub-test-support` unless Implement actually
  changes shipped fixture bytes. Current published coordinate is
  `0.1.33` / protocol 7 / conformance 38. Do not mutate those bytes
  in place.
- Dual-pipelining a teardown-lens implementation. One Plan → Implement
  path.

## Repository ownership boundaries and cross-repo dependencies

Hub owns:

- Whether and when to call `observe_lifecycle`.
- The canonical sanitized session projection and `/session` admission.
- Owner-loop scheduling, slice budgets, and try-wake coalescing.
- Host-bridge fulfillment and authorized plugin consumption.
- Host retention/removal policy after exit.

Core owns:

- Lifecycle facts, the journal, the wake bit, and bounded pages.
- `try_admit` / `drain_completions` / class reservations.
- Terminal Drain, `ProcessExited`, and attach phases.

`botster-hub-client` owns public host-control DTOs. This ticket should
not invent a second client snapshot machine. Plugin
`snapshot_begin`/`chunk`/`end` live on the Hub-owned plugin host-bridge
and `docs/lua-plugin-abi.md`, not on the terminal protocol crates.

Hub must not depend on `botster-terminal-protocol-client`. Hub may
forward opaque terminal envelopes only. Session projection must not
decode `ProcessExited`, terminal silence, terminal output, or attach
state.

Cross-repo dependencies already registered against the Core target
`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`:

- `ticket_1786663581_962361` (closed)
- `ticket_1786663581_723222` (closed)

Do not register a second Hub ticket to consume those APIs. Do not
silently implement Core journal or admission changes in this run.

No new Web, TUI, Workspaces, or Project Pipelines dependency is
required if client session frames stay `entity_snapshot`. If Implement
discovers that a single session snapshot cannot fit
`DAEMON_MAX_FRAME_BYTES` without client-visible chunking, stop and ask
a human before adding `DaemonEntityFrame` variants. That would be an
additive capability with a new unpublished hub-test-support version,
not a silent protocol 8 flag day.

## Assumptions and unknowns

Assumptions:

- Target routing from `list_spawn_targets` is authoritative. This run
  edits only `botster-hub`.
- Core `main` at Implement time still exposes
  `observe_lifecycle`, `take_journal_advanced_wake`,
  `lifecycle_changes_page`, `SessionLifecyclePageError::BudgetTooSmall`,
  `PluginInvocationClass`, `try_admit`, and `drain_completions`. If the
  lockfile refresh cannot compile those names, stop; do not shim them
  in Hub.
- The Hub-shaped consume loop in Core test-support is the required
  consume order. Hub may slice that loop across owner turns, but it
  must not invert take/page/take.
- One `observe_lifecycle` call is one observe slice. Core already walks
  live sessions in `SessionId` order and retains per-session errors.
- RequestResponse `invoke` on MCP/UI/package-provider subscribe is the
  operation. Removing it would break the request. The ticket forbids
  *background* invocation on those handlers, not the request itself.
- Client `entity_snapshot` remains the host-control session contract
  for this ticket. Plugin `snapshot_begin`/`chunk`/`end` is the same
  semantic contract on the plugin host-bridge: one snapshot sequence,
  deltas only after `snapshot_end`, gap requires a complete baseline,
  frames never expire, ended evidence is only a live ended patch or a
  finished baseline ended row.
- Existing `session_lifecycle_class` stays the total classifier.
  Incomplete baseline, omitted UUID, `indeterminate`, `entity_remove`,
  and gap must not be treated as ended.
- `lifecycle_session_drains` as a required idle-progress counter is
  obsolete. Replace the producer with observe/page counters. Do not
  keep calling terminal Drain just to satisfy the old counter.
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty. No `CARGO_TARGET_DIR` override.
- This is not a Hub session-type eligibility consumer.
- Direct-merge pipeline. No pull request.

Unknowns Implement must not invent:

- Exact numeric owner-turn and ready-operation budgets. Publish them
  as named test constants after measuring the isolated daemon path.
  Proposed starting points to beat, not silent defaults:
  `MAX_OWNER_TURN_MS` and `MAX_READY_OPERATION_WAIT_MS`. Acceptance
  fails if they are unpublished.
- Whether later saturated-event Stage D needs client-visible snapshot
  chunking. Out of scope unless the current daemon frame limit is hit
  by the tests in this ticket.
- Event-router product names, schemas, or audiences.

## Implementation shape

Production entry:

```
authoritative Hub mutation
  -> at most one coalesced try_wake
ready operation on the owner loop
  -> handle request, reply, return
idle / wake
  -> one maintenance slice
  -> yield
observe slice
  -> CoreDaemon::observe_lifecycle
journal-pull slice
  -> take_journal_advanced_wake
  -> lifecycle_changes_page(after, max_changes, max_bytes)
projection-apply slice
  -> upsert/remove into the one Hub projection
host-bridge slice
  -> fulfill pending RequestResponse bridges
  -> try_admit(Background) session snapshot_begin/chunk/end
subscriber-delivery slice
  -> bounded client entity_snapshot / upsert / patch / remove
completion-drain slice
  -> drain_completions
resync slice
  -> bounded package provider resync
```

Natural zero-client exit must converge through observe + page +
projection with no `CoreDaemon::drain`, no attach, and no Web/TUI
subscriber.

Suggested module split (keep gravity down; do not grow
`daemon_transport.rs`):

- `src/session_projection.rs` — cursor, canonical rows, apply, ended
  evidence helpers, architecture-test hooks.
- `src/daemon_maintenance.rs` — wake bit, round-robin slice scheduler,
  slice budgets. Do not reuse `src/maintenance.rs`.
- Thin `HubRuntime` facades for observe / take / page / try_admit /
  drain_completions.
- `src/daemon_entity_subscriptions.rs` — client delivery only. Remove
  Drain-based discovery and the empty-subscriber early return that
  skips projection.
- `docs/lua-plugin-abi.md` — document the `/session` host-bridge
  snapshot sequence.
- `docs/client-protocol.md` — document that lifecycle projection no
  longer depends on subscribers or terminal Drain.

## Affected surfaces/files

Expected to change:

- `Cargo.lock` — pin Core `main` after the closed parents.
- `src/runtime.rs` — Core observe/wake/page/try_admit/drain_completions
  facades; stop using unbounded `lifecycle_changes` on this path; move
  `emit_plugin_event` to Background admission.
- `src/daemon_transport.rs` — owner-loop scheduling only. Remove
  inline `drive_entity_subscriptions` / fanout / resync from request
  handlers.
- `src/daemon_entity_subscriptions.rs` — projection independence,
  Drain removal, sliced client delivery.
- New `src/session_projection.rs` and `src/daemon_maintenance.rs`.
- `src/lib.rs` / `src/daemon.rs` module wiring as needed.
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, this plan.
- Tests:
  - `tests/hub_daemon_lifecycle/sessions.rs`
  - `tests/hub_client_api_test.rs`
  - `tests/hub_lua_runtime_test.rs` or a new synthetic-plugin fixture
  - new source architecture tests for terminal-body and Workspaces
    policy exclusion
- `src/daemon_entity_subscriptions.rs` unit tests for ended evidence.

Likely untouched:

- `src/local_webrtc.rs`, attach/drain terminal adapters
- `src/session_types.rs` eligibility
- package event router ticket surfaces
- `src/maintenance.rs` update-check code
- Workspaces fixtures except to prove they are not named by the new
  projection path

## Risks

- Refreshing Core past `033cd01` can pull unrelated Core attach or
  admission changes. Compile and rerun Hub daemon lifecycle tests
  against the new lockfile before rewriting the owner loop.
- Leaving `drain_runtime_once` in the session pump would keep the
  forbidden lifecycle-from-Drain path and fail the new architecture
  tests.
- Keeping the empty-subscriber early return would fail zero-subscriber
  projection.
- Calling `lifecycle_changes` instead of `lifecycle_changes_page`
  would ignore page budgets and the Hub-shaped consume contract.
- Page-then-take-then-sleep can drop a wake. Copy Core's take / page /
  take order, sliced across turns.
- Treating worker isolation as non-blocking would leave Spawn/MCP/UI
  coupled to slow handlers. Review must see `try_admit(Background)` on
  the background path.
- Blocking `invoke` inside a maintenance slice would recreate owner
  stalls. Host-bridge session delivery must be admit-and-yield.
- Changing client `DaemonEntityFrame` without a feature constant would
  break Web/TUI at protocol 7. Do not do that in this ticket.
- Existing focused-idle test that requires
  `lifecycle_session_drains` will fail if Drain is removed without
  rewriting the oracle.
- Growing `daemon_transport.rs` further recreates Hub gravity. Put
  the scheduler and projection in new modules.

## Acceptance checks/tests

Charter gates (CI-matching):

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `./test.sh --locked`
- `cargo test --doc --workspace --locked`

Product proofs (new or rewritten; must exercise the production owner
loop, not only helper existence):

1. Zero Web/TUI subscribers: spawn a short-lived session, never
   `SubscribeEntities`, never Attach/Drain. After observe/page slices,
   the Hub projection contains a finished baseline ended row.
2. Synthetic plugin consumption: an admitted fixture plugin with no
   Workspaces names receives `snapshot_begin`, one or more bounded
   `snapshot_chunk`s, and `snapshot_end` at one sequence, then a live
   ended patch or a later complete baseline ended row. Same ended
   evidence rules as clients.
3. False-ended matrix: incomplete baseline, omitted UUID,
   `indeterminate` row, `entity_remove`, and gap each fail an
   `is_ended` helper. A live ended patch and a finished baseline ended
   row pass.
4. Slow Background handler: a synthetic handler sleeps beyond the
   ready-operation budget. Spawn, Drain, MCP, and UI requests still
   complete within the published ready-operation wait. Completions
   drain on a later slice.
5. Owner-turn budget: with ready operations queued, one maintenance
   slice returns within `MAX_OWNER_TURN_MS`. The next owner turn
   handles the operation rather than another slice.
6. Consume-order: dropped wake still converges by paging from the last
   cursor. `BudgetTooSmall` raises `max_bytes`. SourceChanged /
   CursorExpired / CursorAhead install a fresh baseline.
7. Architecture: Hub source tests fail if the projection/maintenance
   modules import `botster-terminal-protocol-client`, match
   `ProcessExited` bodies, or contain `botster-workspaces`,
   `membership`, or package cleanup-rule identifiers.
8. Existing session entity subscription tests still pass on
   `entity_snapshot` / upsert / patch / remove, including reconnect
   snapshot authority and stale-as-indeterminate.
9. Rewrite
   `session_entity_subscription_observes_natural_exit_without_terminal_attach`
   so the oracle is the Hub projection / entity patch, not a terminal
   Drain event. Keep the no-attach fixture.

Downstream proof:

- Not required in Web or TUI checkouts while the client session frame
  vocabulary is unchanged.
- Required in-repo: generated TypeScript / hub-test-support stay at
  protocol 7 / conformance 38 unless Implement actually changes
  shipped client frames. If frames change, bump conformance, pick a
  new unpublished hub-test-support version, and ask a human before
  raising protocol.

Live Hub pin / charter live proof:

- Record Hub source SHA and lockfile-pinned Core SHA separately after
  the Core refresh.
- Isolated daemon with an explicit data directory is the production
  path. Do not use a second in-process engine story.

## Runtime-teardown class

`teardown_class_applies`: no.

This ticket projects host session state from Core journal pages and
moves background work off operation handlers. It does not implement
peer/session/runtime teardown. Terminal-state vs live-runtime
divergence is owned by the closed Core journal parent. Hub's duty is
to stop using terminal Drain as a lifecycle oracle.

## Worktree hygiene

- Tracked `.gitignore` is present and non-empty (53 bytes; matches
  HEAD). Do not truncate it.
- Assigned worktree path contains no `:`. Do not set
  `CARGO_TARGET_DIR`.

## Vault gaps worth capturing

- Hub now has a required consume loop (observe, take, page, take)
  distinct from terminal Drain. After merge, capture that Hub
  lifecycle projection must not early-return on zero subscribers and
  must not call `drain_runtime_once` to discover exit.
- Worker isolation vs non-blocking already exists. After merge, extend
  it with the concrete Hub Background `try_admit` adoption on session
  projection, or leave a short inbox note if the existing drift note
  is enough.
- Do not capture Workspaces cleanup policy. That remains a package
  concern.
- Do not capture unpublished owner-turn numbers until Verify measures
  them.

## Pipeline gates and artifacts

- Plan destination: `docs/plans/project-session-state-without-blocking-operation-paths.md`
  (repo-owned living plan directory).
- Direct merge into `main` after Verify. No pull request.
- Implement must leave command evidence for the charter gates and the
  product proofs above.
- Review overlays: [[botster-runtime-reviewer-playbook]] for the owner
  loop; [[botster-package-reviewer-playbook]] only for the synthetic
  plugin fixture and lua-plugin-abi host-bridge contract.
