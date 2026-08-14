# Hub: project session state without blocking operation paths

## Plan Review revision

Plan Review `review_1786690443_941755` returned `changes_required`.
This revision answers the four product findings. It does not reopen
target routing.

| Finding | Response |
| --- | --- |
| Core observe and baseline defeat slice bounds | Do not call unbounded `observe_lifecycle` or `lifecycle_baseline` as a maintenance slice. Registered Core ticket `ticket_1786690597_161141` on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` for item/byte/elapsed sliced observe and paged baseline. Hub Implement waits on that merge. |
| Core refresh does not compile | Scope now includes the six new `PluginWorkerEngineConfig` host knobs and the `CoreDaemonError::BindTerminalAdapter` projection, with policy and tests. |
| Baseline gates cannot reach tests | Recorded exact current-lock evidence. This ticket owns the `derivable_impls` cleanup. WebRTC ownership failure is owned by `ticket_1786690597_154692` on the Hub target and is a blocking dependency. |
| Snapshot order can depend on default executor width | One in-flight session-family frame per plugin and snapshot sequence. Advance only after the matching successful completion. Test with background concurrency above one and skewed handler durations. |

Duplicate vault checklist `checklist_1786689631_664448` remains unused.
This visit keeps `checklist_1786689614_825667` and does not create another.

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
- First Plan HEAD: `173e528`. Plan artifact commit: `ddb0c60`.
- Locked Core in this worktree: `033cd01`. Verified Core `origin/main`
  at Plan Review: `a047574`.

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
- [[cross repo dependency registration must use dependency repo target]]

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
- [[plugin worker queue capacity and executor concurrency are independent host profile knobs]]
- [[package entity hydration uses explicit providers not mcp naming]]
- [[botster plugin entity hydration has full id and scoped contracts]]
- [[plugin surfaces request model state through ui bindings not hub subscribe]]
- [[botster hub client state sync is entity frame only]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[session UUID is the sole routing key across all layers]]
- [[hub shutdown preserves durable session workers]]
- [[session wide drains cannot deliver subscription owned initial state]]

Hub-client surface overlay:

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
- [[botster runtime teardown lenses]] — `teardown_class_applies` is no.
  The closed Core journal parent already answered teardown lenses.
  This Hub ticket consumes bounded journal/observe pages; it does not
  implement WebRTC/peer teardown or SessionIo/ClientWorker teardown.
- Session-type eligibility consumer pins — not that consumer.

## Context loaded

### Current Hub (this worktree, Core `033cd01`)

- `drive_entity_subscriptions` early-returns when
  `entity_subscriptions` is empty after package fanout/resync. Zero
  Web/TUI subscribers stop session projection.
- The same pump calls `HubRuntime::drain_runtime_once` for every
  non-exited, non-attached session. That is terminal Drain used to
  discover lifecycle.
- Owner loop also runs that pump after Spawn / Resize /
  ShutdownSession / RemoveSession when any subscriber exists, and runs
  fanout plus provider resync after every control reply.
- `HubRuntime::session_lifecycle_changes` uses unbounded
  `CoreDaemon::lifecycle_changes`.
- `HubRuntime::invoke_plugin` is blocking. Background lifecycle
  emission uses that path.
- Client session wire is `DaemonEntityFrame::{Snapshot,Upsert,Patch,Remove}`
  at protocol 7 / conformance 38.
- `session_lifecycle_class` already matches ended-evidence rules.
- `src/config.rs` `plugin_worker_config()` constructs
  `PluginWorkerEngineConfig` with only
  `per_plugin_queue_capacity` and `per_plugin_executor_concurrency`.
- `managed_session_core_error_class` does not match
  `CoreDaemonError::BindTerminalAdapter`.
- `AttachStreamRegistry` has a hand-written `Default` that clippy
  `derivable_impls` rejects at `src/daemon_attach_stream.rs:54`.

### Current-lock baseline evidence (Plan Review, Hub `173e528`)

- `cargo fmt --all -- --check` passed.
- `cargo test --doc --workspace` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
  failed at `src/daemon_attach_stream.rs:54` (`derivable_impls`).
- `./test.sh --locked` reached execution and repeatedly failed
  `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner`
  in `src/local_webrtc.rs`.

### Core `a047574` consume contract

Closed parents already published:

- `observe_lifecycle` — visits **every** live session. No item, byte,
  cursor, or elapsed input. Not a maintenance-slice primitive.
- `lifecycle_baseline` — `registry.load_all()` of every row. Same
  problem.
- `take_journal_advanced_wake` and bounded `lifecycle_changes_page`.
- `PluginInvocationClass`, `try_admit`, `drain_completions`.
- `PluginWorkerEngineConfig` now has eight fields. Hub maps two.
- `CoreDaemonError::BindTerminalAdapter`.

Hub-shaped consume order remains: take, page until
`next == watermark` or resync, take, re-page if woke. That order still
applies to journal pages. It does **not** make unbounded observe or
unbounded baseline legal slices.

Plan Review compiled Hub against `a047574` in a temporary clone.
Compilation failed at `src/config.rs:356` and `src/runtime.rs:2390`
before any test ran.

## Scope

Hub Implement of the owner-loop projection is gated on
`ticket_1786690597_161141` (Core, `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`).
Do not shim sliced observe or paged baseline inside Hub.

### A. Consume the Core refresh (this repo, before owner-loop work)

1. Refresh `Cargo.lock` to Core `main` that includes both closed
   parents **and** `ticket_1786690597_161141` once that ticket merges.
   Until then, compatibility work may compile against `a047574`, but
   projection slices must not call the unbounded observe/baseline
   forms as if they were bounded.
2. Map all eight `PluginWorkerEngineConfig` fields from Hub host
   policy. Keep the two existing knobs. Add six new
   `CoreEngineOptions` fields with Core defaults:
   - `reserved_request_response_executors` (default 1)
   - `request_response_queue_byte_capacity`
   - `background_queue_capacity`
   - `background_queue_byte_capacity`
   - `completion_queue_capacity`
   - `completion_queue_byte_capacity`
3. Validate: every capacity is positive; reserved executors `>= 1`
   and strictly less than `plugin_worker_executor_concurrency`. Reject
   invalid configs the way Core rejects them.
4. Prove distinct live queue, executor, reservation, background, and
   completion values through Core's `PluginWorkerDebugSnapshot` on the
   real plugin lifecycle path, plus unload retirement. Configuration
   JSON is not enough ([[plugin worker queue capacity and executor concurrency are independent host profile knobs]]).
5. Project `CoreDaemonError::BindTerminalAdapter` in
   `managed_session_core_error_class` to typed class strings for
   `BindBeforeAttach`, `UnknownSubscription`, `StaleGeneration`, and
   `AlreadyBound`. This uses the Core engine contract, not
   `botster-terminal-protocol-client`.
6. Replace `AttachStreamRegistry`'s hand-written `Default` with
   `#[derive(Default)]` so current-lock clippy passes.

### B. Owner-loop projection (after Core sliced observe/baseline)

7. Split the owner loop so a ready Spawn, Attach, Drain, Input,
   Resize, Shutdown, MCP, UI, or entity-mutation request is handled
   without `drive_entity_subscriptions`, package fanout, provider
   resync, or background plugin invocation. After an authoritative
   mutation, set at most one O(1) coalesced `try_wake` bit.
8. Keep one Hub lifecycle cursor and one canonical in-memory session
   projection, independent of subscriber count.
9. Process work in round-robin maintenance slices. One owner turn
   runs at most one slice, then yields to a ready operation. Slice
   kinds:
   - observe — **sliced** Core observe only
   - journal pull — take wake + one `lifecycle_changes_page`
   - projection apply
   - host-bridge fulfillment
   - subscriber delivery
   - completion drain
   - package-entity provider resync
   - paged baseline recovery when a resync is required
10. Bound every slice by item count, encoded byte count, and elapsed
    time using the Core sliced APIs. Hub must not call unbounded
    `observe_lifecycle` or `lifecycle_baseline` from the owner loop.
    An incomplete baseline page is not finished ended evidence.
11. Publish slice budgets as named test constants after measuring the
    isolated daemon path. Add a load test that scales live-session
    count and proves ready-operation wait through the production
    owner loop.

### C. Plugin session-family delivery

12. Deliver Hub-owned `/session` through `snapshot_begin`, bounded
    `snapshot_chunk`, and `snapshot_end` at one snapshot sequence.
13. Admit at most one in-flight session-family frame per plugin and
    snapshot sequence. Do not admit the next chunk, `snapshot_end`,
    or any live delta until the matching completion for the previous
    frame succeeds. FIFO `try_admit` is not a completion fence.
14. On admission failure, completion failure, or handler failure,
    mark a gap and require a complete baseline. Session state frames
    never expire.
15. Keep client `SubscribeEntities { entity_type: "session" }` on
    `entity_snapshot` / upsert / patch / remove unless a single
    snapshot exceeds `DAEMON_MAX_FRAME_BYTES`. Do not raise the
    default client requirement. Ask a human before adding
    `DaemonEntityFrame` variants.
16. Move background plugin work to `try_admit(Background)`. Keep
    blocking `invoke` only for RequestResponse MCP, UI render/action,
    and package-provider subscribe snapshots.

### D. Evidence and architecture

17. Add Hub source tests that fail if the new projection path imports
    terminal semantic bodies or names Workspaces/membership/package
    cleanup policy.
18. Update Drain-based natural-exit and `lifecycle_session_drains`
    proofs to observe/page/projection.
19. Do not merge this ticket while
    `ticket_1786690597_154692` is open unless that focused WebRTC
    test is already green on the same revision.

## Non-scope

- Implementing sliced observe or paged baseline inside Hub.
- Package events, `events.emit`, client `SubscribeEvents`.
- Web or TUI UI work.
- Workspaces membership or package cleanup rules.
- Changing ClientWorker, SessionIo, attach phase machines, or
  terminal Drain semantics, except the separately owned WebRTC
  replacement-owner ticket.
- Replacing RequestResponse `invoke` for MCP/UI.
- Publishing `@trybotster/hub-test-support` unless shipped fixture
  bytes change. Current coordinate: `0.1.33` / protocol 7 /
  conformance 38.
- Dual-pipelining teardown-lens implementation.

## Repository ownership boundaries and cross-repo dependencies

Hub owns projection, scheduling, host-bridge, `/session` admission,
and host-profile worker knobs.

Core owns lifecycle facts, the journal, wake/page, sliced observe,
paged baseline, and `try_admit`.

`botster-hub-client` owns public host-control DTOs. Plugin
`snapshot_begin`/`chunk`/`end` stay on the Hub-owned host-bridge.

Hub must not depend on `botster-terminal-protocol-client`.

Registered dependencies (this ticket cannot complete until these
close):

| Ticket | Target | Repo | Why |
| --- | --- | --- | --- |
| `ticket_1786663581_962361` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | Journal wake/page (closed) |
| `ticket_1786663581_723222` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | Class-aware `try_admit` (closed) |
| `ticket_1786690597_161141` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | Sliced observe + paged baseline (open). Run `run_1786690610_471868`. |
| `ticket_1786690597_154692` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | Current-lock WebRTC replacement-owner failure (open). Run `run_1786690609_367424`. |

Do not implement Core observe/baseline bounding in this Hub worktree.
Do not treat the WebRTC failure as an acceptable caveat on
`./test.sh --locked`.

No new Web, TUI, Workspaces, or Project Pipelines dependency while
client session frames stay `entity_snapshot`.

## Assumptions and unknowns

Assumptions:

- Target routing from `list_spawn_targets` is authoritative.
- Hub Implement of slices waits for
  `ticket_1786690597_161141` to publish named Core APIs. This plan
  names the required properties, not speculative Rust signatures.
  Implement must consume the merged names, not invent a second Hub
  walk over `list_sessions`.
- RequestResponse `invoke` on MCP/UI/package-provider subscribe is
  the operation.
- Client `entity_snapshot` remains the host-control session contract.
- Plugin begin/chunk/end is the same semantic contract on the
  host-bridge, with a completion fence.
- Existing `session_lifecycle_class` stays the total classifier.
- `lifecycle_session_drains` as a required idle-progress counter is
  obsolete.
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty.
- This is not a Hub session-type eligibility consumer.
- Direct-merge pipeline. No pull request.

Unknowns Implement must not invent:

- Exact Core sliced-observe and paged-baseline type names. Read the
  merged Core docs and rustdoc after
  `ticket_1786690597_161141` closes.
- Exact numeric `MAX_OWNER_TURN_MS` and
  `MAX_READY_OPERATION_WAIT_MS`. Publish them after measurement.
  Acceptance fails if they are unpublished or if they only hold
  because observe still walks every session.
- Whether later Stage D needs client-visible snapshot chunking.

## Implementation shape

Production entry after the Core parent merges:

```
authoritative Hub mutation
  -> at most one coalesced try_wake
ready operation on the owner loop
  -> handle request, reply, return
idle / wake
  -> one maintenance slice
  -> yield
observe slice
  -> Core sliced observe(max_sessions, max_bytes, max_elapsed)
  -> resume cursor if incomplete
journal-pull slice
  -> take_journal_advanced_wake
  -> lifecycle_changes_page(after, max_changes, max_bytes)
projection-apply slice
  -> upsert/remove into the one Hub projection
paged-baseline slice
  -> only on resync; assemble one snapshot sequence
  -> incomplete page is not ended evidence
host-bridge slice
  -> fulfill pending RequestResponse bridges
  -> admit at most one session-family frame per plugin/sequence
  -> wait for that completion before the next admit
subscriber-delivery slice
  -> bounded client entity_snapshot / upsert / patch / remove
completion-drain slice
  -> drain_completions
resync slice
  -> bounded package provider resync
```

Suggested module split (keep gravity down):

- `src/session_projection.rs` — cursor, rows, apply, ended evidence.
- `src/daemon_maintenance.rs` — wake bit, round-robin scheduler,
  slice budgets. Do not reuse `src/maintenance.rs`.
- `src/config.rs` / `src/runtime.rs` — worker knobs and
  `BindTerminalAdapter` class mapping.
- `src/daemon_attach_stream.rs` — derive `Default`.
- `src/daemon_entity_subscriptions.rs` — client delivery only.
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`.

## Affected surfaces/files

Expected to change in this Hub ticket:

- `Cargo.lock`
- `src/config.rs` — six new worker knobs, validation, serde
- `src/runtime.rs` — facades; `BindTerminalAdapter` class mapping;
  Background `try_admit`
- `src/persistence.rs` / config tests if durable core_engine JSON
  grows
- `src/daemon_attach_stream.rs` — `#[derive(Default)]`
- `src/daemon_transport.rs` — owner-loop scheduling only
- `src/daemon_entity_subscriptions.rs`
- New `src/session_projection.rs` and `src/daemon_maintenance.rs`
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, this plan
- Tests:
  - `tests/hub_plugin_lifecycle_test.rs` / `plugin_bounds.rs` —
    distinct live knobs including reservation, background, and
    completion
  - `tests/hub_daemon_lifecycle/sessions.rs`
  - `tests/hub_client_api_test.rs`
  - new synthetic-plugin fixture with executor concurrency `> 1`
  - new session-count load / ready-operation test
  - new architecture tests

Likely untouched in this ticket:

- `src/local_webrtc.rs` — owned by `ticket_1786690597_154692`
- `src/session_types.rs` eligibility
- package event router surfaces
- `src/maintenance.rs` update-check code

## Risks

- Implementing slices against unbounded `observe_lifecycle` would
  fail the ticket's own owner-turn proof. Do not do that.
- Refreshing Core before `ticket_1786690597_161141` merges can
  compile compatibility work but cannot finish projection.
- Leaving `drain_runtime_once` in the session pump keeps the
  forbidden lifecycle-from-Drain path.
- Keeping the empty-subscriber early return fails zero-subscriber
  projection.
- Page-then-take-then-sleep can drop a wake.
- Admitting the next snapshot chunk before the previous completion
  returns can reorder frames when background executors `> 1`.
- Treating worker isolation as non-blocking leaves Spawn/MCP/UI
  coupled to slow handlers.
- Changing client `DaemonEntityFrame` without a feature constant
  would break Web/TUI at protocol 7.
- Claiming `./test.sh --locked` while the WebRTC replacement-owner
  test still fails is a gate lie. That failure has an owner ticket.
- Growing `daemon_transport.rs` further recreates Hub gravity.

## Acceptance checks/tests

Charter gates (CI-matching), only after both open dependencies are
closed or independently green on the same revision:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `./test.sh --locked`
- `cargo test --doc --workspace --locked`

Product proofs through the production owner loop:

1. Zero Web/TUI subscribers: spawn a short-lived session, never
   `SubscribeEntities`, never Attach/Drain. After sliced observe and
   page slices, the Hub projection contains a finished baseline ended
   row. Incomplete baseline pages must not satisfy this.
2. Session-count load: raise live sessions well above one observe
   slice budget. Ready Spawn/Drain/MCP/UI wait stays within
   `MAX_READY_OPERATION_WAIT_MS`. Owner turn stays within
   `MAX_OWNER_TURN_MS`. Red on revert: calling unbounded
   `observe_lifecycle` from the owner loop.
3. Synthetic plugin consumption with
   `plugin_worker_executor_concurrency > 1` and reserved
   RequestResponse `= 1`. Earlier frames have longer handlers than
   later frames. The plugin still sees
   `snapshot_begin` then chunks then `snapshot_end` then deltas.
   A live ended patch or a finished complete baseline ended row is
   the only ended evidence.
4. False-ended matrix: incomplete baseline, omitted UUID,
   `indeterminate`, `entity_remove`, and gap each fail `is_ended`.
5. Slow Background handler cannot delay Spawn, Drain, MCP, or UI
   beyond the published ready-operation wait.
6. Consume-order: dropped wake still converges. `BudgetTooSmall`
   raises `max_bytes`. Resync installs a **paged** complete baseline,
   not one unbounded `load_all`.
7. Architecture: projection/maintenance modules must not import
   `botster-terminal-protocol-client`, match `ProcessExited` bodies,
   or contain `botster-workspaces`, `membership`, or package
   cleanup-rule identifiers.
8. Distinct live worker knobs: queue, executor, reservation,
   background queue/bytes, and completion queue/bytes all appear in
   Core debug snapshots with the configured values. Unload returns
   to baseline.
9. `BindTerminalAdapter` class mapping is total over the four
   published variants.
10. Existing session entity subscription tests still pass on
    `entity_snapshot` / upsert / patch / remove.
11. Rewrite
    `session_entity_subscription_observes_natural_exit_without_terminal_attach`
    so the oracle is the Hub projection, not a terminal Drain event.

Downstream proof:

- Not required in Web or TUI checkouts while client session frames
  stay `entity_snapshot`.
- In-repo TypeScript / hub-test-support stay at protocol 7 /
  conformance 38 unless client frames change.

Live Hub pin:

- Record Hub source SHA and lockfile-pinned Core SHA separately after
  the Core refresh. The Core SHA must be at or after the merge of
  `ticket_1786690597_161141`.

## Runtime-teardown class

`teardown_class_applies`: no.

## Worktree hygiene

- Tracked `.gitignore` is present and non-empty (53 bytes; matches
  HEAD). Do not truncate it.
- Assigned worktree path contains no `:`. Do not set
  `CARGO_TARGET_DIR`.

## Vault gaps worth capturing

- After the Core sliced-observe parent merges, capture that Hub
  maintenance slices must call the sliced observe/baseline APIs, not
  the compatibility full-walk wrappers.
- After this Hub merge, capture that Hub projection must not
  early-return on zero subscribers and must not use
  `drain_runtime_once` to discover exit.
- Do not capture unpublished owner-turn numbers until Verify
  measures them.

## Pipeline gates and artifacts

- Plan destination:
  `docs/plans/project-session-state-without-blocking-operation-paths.md`
- Direct merge into `main` after Verify. No pull request.
- Vault checklist for this ticket:
  `checklist_1786689614_825667`. Skip further vault-checklist
  creation. Unused duplicate:
  `checklist_1786689631_664448`.
- Review overlays: [[botster-runtime-reviewer-playbook]] for the
  owner loop; [[botster-package-reviewer-playbook]] for the synthetic
  plugin fixture.
