# Hub: project session state without blocking operation paths

## Plan Review revision

Plan Review `review_1786733111_104540` returned `changes_required`.
This fourth Plan visit answers the two new product findings and keeps
the earlier answers.

| Finding | Response |
| --- | --- |
| Core baseline pages still perform unbounded full-set work | `159d926` `lifecycle_baseline_page` still `load_all()`s on mint and clones remaining rows before applying page limits. It has no elapsed budget. New Core ticket `ticket_1786733177_803101` on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. Hub Implement waits on that merge SHA. |
| Plan omits exact Core pins in three Cargo manifests | Pin one exact Core `.git` URL and revision in `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`, and `Cargo.lock`. No `branch = "main"`. No `>=`. Add a drift check that every Git-visible Hub member uses that one URL and revision ([[Git-consumed Hub members pin Core protocol by exact revision]]). |
| Core observe and baseline defeat slice bounds | Observe is solved by `observe_lifecycle_slice` on `159d926`. Baseline is not. Do not treat `159d926` as the consumable pin. |
| Core refresh does not compile | Merge Hub `main` first. Then pin the new Core ticket's exact merge SHA, not `>= 159d926`. |
| Baseline gates cannot reach tests | WebRTC owner ticket `ticket_1786690597_154692` is closed (`d92aace`). Merge Hub `main` before `./test.sh --locked`. |
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
- First Plan HEAD: `173e528`. Prior plan commits: `ddb0c60`, `d4dfb8e`,
  `a37a430`.
- This worktree still sits behind current Hub `main`. Implement must
  merge Hub `main` (includes `d92aace` WebRTC owner fix) before owner-loop
  work.
- Required Core pin: the exact merge SHA of
  `ticket_1786733177_803101`, once that ticket is closed. Not
  `159d926`. Not `branch = "main"`. Not a `>=` range. Current Hub
  `main` still pins Core `f4f6bf5babe92dfb9241a760c414187f711c2c42`.
  This worktree still uses `branch = "main"` in `Cargo.toml`; that
  form is forbidden for Git-visible members.

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
- [[Git-consumed Hub members pin Core protocol by exact revision]]

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

- [[project-pipelines-playbook]] — not package/plugin policy work.
- [[botster runtime teardown lenses]] — `teardown_class_applies` is no.
- Session-type eligibility consumer pins — not that consumer.

## Context loaded

### Merged Core parent (`159d926`) — observe only

Inspected `botster-core-daemon` on Core `origin/main` at `159d926`.
Living contract: Core `docs/architecture/control-plane-lifecycle-journal.md`.
`observe_lifecycle_slice` is consumable. `lifecycle_baseline_page` on
this revision is **not** consumable as a bounded owner-loop slice.

Production APIs Hub must call:

```
CoreDaemon::observe_lifecycle_slice(
    now_seconds,
    resume: Option<&ObserveLifecycleCursor>,
    budget: ObserveLifecycleBudget {
        max_sessions,
        max_encoded_result_bytes,
        max_elapsed,
    },
) -> Result<ObserveLifecycleSlice, SessionLifecyclePageError>
```

- `resume = None` mints `ObserveLifecyclePassId` over the ordered live
  set. Later slices walk only the unvisited suffix.
- Resume requires both `pass_id` and `last_visited` to match. Otherwise
  `resync_required = ObservePassUnavailable`, `complete = false`, no
  suffix.
- `ObserveLifecycleSlice.complete` is true only when the pass attempted
  every remaining live session.
- Elapsed starts at API entry and includes pass setup. A setup-only
  yield returns `last_visited = None`. Resume with that exact cursor.
- Byte admission reserves a 256-`x` public error before each visit.
  `BudgetTooSmall` keeps the pass open.
- Public slice errors are sanitized strings, not typed
  `CoreDaemonError`.
- Sessions that appear after mint wait for a new pass.

`159d926` `lifecycle_baseline_page` is insufficient. On that revision:

- There is no elapsed-time argument.
- `snapshot = None` calls `mint_baseline_freeze()`, which `load_all()`s
  every registry row, maps `lifecycle_record`, sorts the full set, and
  retains it before any page returns.
- Each later page clones every remaining frozen row, then applies
  `max_rows` / `max_bytes`.

Hub must wait for `ticket_1786733177_803101` to publish a baseline
page that:

- Takes an elapsed-time budget in addition to `max_rows` and
  `max_bytes`.
- Can return the first page before the whole registry is copied.
- Walks only the next bounded suffix on later pages. No full remaining
  clone.
- Keeps one `snapshot_sequence`, `complete = false` on incomplete
  pages, and the existing resync reasons.

Implement must consume the merged names from that ticket. Do not
invent a Hub-side walk over `list()` / `load_all()`.

Forbidden on the Hub owner loop:

- `observe_lifecycle` — unbounded compatibility wrapper
  (`max_sessions/bytes/elapsed = MAX`).
- `lifecycle_baseline` — unbounded `load_all()` compatibility wrapper.

Unchanged consume order for journal pages: take,
`lifecycle_changes_page` until `next == source_watermark` or resync,
take, re-page if woke.

New resync reasons Hub must handle:
`SnapshotUnavailable`, `ObservePassUnavailable`, plus existing
`SourceChanged`, `CursorExpired`, `CursorAhead`. The enum is
`#[non_exhaustive]`. Match known variants and a wildcard.

Core production host loop for this ticket:

1. One `observe_lifecycle_slice` per owner turn until `complete` or a
   budget yield. Store the resume cursor.
2. `take_journal_advanced_wake`.
3. The post-`ticket_1786733177_803101` bounded baseline page until
   `complete` when installing or resyncing. Not `159d926`
   `lifecycle_baseline_page`. Not `lifecycle_baseline()`.
4. `lifecycle_changes_page` after the snapshot watermark.
5. Take again; re-page if woke.

### Current Hub `main` vs this worktree

Hub `main` already contains:

- `#[derive(Default)]` on `AttachStreamRegistry` (clippy finding gone).
- `CoreDaemonError::BindTerminalAdapter(_) => "bind_terminal_adapter"`
  (compiles; not variant-complete).
- `plugin_worker_config()` filling the six new Core fields from
  `PluginWorkerEngineConfig::default()` rather than Hub knobs.
- WebRTC replacement-owner merge `d92aace`
  (`ticket_1786690597_154692` closed).
- Core lock `f4f6bf5`, which is **before** sliced observe/baseline.

This Plan worktree is still on the plan-only branch. Implement must
merge Hub `main`, then pin every Git-visible Core dependency to the
exact merge SHA of `ticket_1786733177_803101`.

### This worktree (pre-merge) still has the original defects

- `drive_entity_subscriptions` early-returns with zero subscribers.
- The pump calls `drain_runtime_once` to discover lifecycle.
- Operation handlers still drive fanout/resync/reconciliation.
- `session_lifecycle_changes` still uses unbounded
  `lifecycle_changes`.

## Scope

### A. Rebase and pin one exact Core revision

1. Merge current Hub `main` into this worktree so the WebRTC owner
   fix and `AttachStreamRegistry` derive land first.
2. After `ticket_1786733177_803101` closes, pin **one exact** Core
   revision (that merge SHA) in all four places, using the `.git` URL
   form from current Hub `main`:
   - `Cargo.toml` (`botster-core`, `botster-core-daemon`,
     `botster-terminal-protocol`, and the matching dev-dependencies)
   - `crates/botster-hub-client/Cargo.toml`
     (`botster-terminal-protocol`)
   - `crates/botster-hub-test-support/Cargo.toml`
     (`botster-core`, `botster-terminal-ghostty`)
   - `Cargo.lock`
3. Replace this worktree's `branch = "main"` selectors. Do not leave
   any Git-visible Core dependency on a branch or a `>=` range.
4. Add a Hub source test that every Git-visible member manifest uses
   the same Core git URL and the same `rev`.
5. Record Hub SHA and locked Core SHA separately.
6. Fix any additional compile breaks the pin introduces. Do not invent
   Hub wrappers that call unbounded `observe_lifecycle` or
   `lifecycle_baseline`. Do not call `159d926`
   `lifecycle_baseline_page` as a bounded slice.
7. Promote these six host-profile knobs from silent Core defaults to
   `CoreEngineOptions` fields with Core defaults and positive-value
   validation:
   - `reserved_request_response_executors` (`>= 1` and strictly less
     than `plugin_worker_executor_concurrency`)
   - `request_response_queue_byte_capacity`
   - `background_queue_capacity`
   - `background_queue_byte_capacity`
   - `completion_queue_capacity`
   - `completion_queue_byte_capacity`
8. Prove distinct live queue, executor, reservation, background, and
   completion values through Core `PluginWorkerDebugSnapshot` on the
   real plugin lifecycle path, plus unload retirement.
9. Split `BindTerminalAdapter` class mapping into
   `BindBeforeAttach`, `UnknownSubscription`, `StaleGeneration`, and
   `AlreadyBound`. Use the Core engine contract, not
   `botster-terminal-protocol-client`.

### B. Owner-loop projection

7. Ready Spawn, Attach, Drain, Input, Resize, Shutdown, MCP, UI, or
   entity-mutation requests run without `drive_entity_subscriptions`,
   package fanout, provider resync, or background plugin invocation.
   After an authoritative mutation, at most one O(1) coalesced
   `try_wake`.
8. One Hub lifecycle cursor and one canonical in-memory session
   projection, independent of subscriber count.
9. Round-robin slices. One owner turn runs at most one slice, then
   yields to a ready operation:
   - observe — `observe_lifecycle_slice` with published
     `ObserveLifecycleBudget`
   - journal pull — take + one `lifecycle_changes_page`
   - projection apply
   - paged baseline recovery — the post-`ticket_1786733177_803101`
     bounded baseline page until `complete` on resync only. Must
     honor item, byte, **and elapsed** budgets. Must not
     `load_all()` or clone the remaining freeze on the owner turn.
   - host-bridge fulfillment
   - subscriber delivery
   - completion drain
   - package-entity provider resync
10. Bound every slice by item count, encoded bytes, and elapsed time.
    Incomplete baseline pages are not ended evidence.
    `ObservePassUnavailable` / `SnapshotUnavailable` start a new pass
    or mint, never a partial suffix treated as complete.
11. Publish `MAX_OWNER_TURN_MS` and
    `MAX_READY_OPERATION_WAIT_MS` after measuring the isolated daemon
    path. Add a load test that scales live-session count above one
    observe-slice budget and proves ready-operation wait through the
    production owner loop. Red on revert: calling unbounded
    `observe_lifecycle` from the owner loop.

### C. Plugin session-family delivery

12. Deliver Hub-owned `/session` through `snapshot_begin`, bounded
    `snapshot_chunk`, and `snapshot_end` at one snapshot sequence.
13. Admit at most one in-flight session-family frame per plugin and
    snapshot sequence. Do not admit the next chunk, `snapshot_end`, or
    any live delta until the matching completion succeeds.
14. On admission, completion, or handler failure, mark a gap and
    require a complete baseline. Frames never expire.
15. Keep client `SubscribeEntities { entity_type: "session" }` on
    `entity_snapshot` / upsert / patch / remove unless a single
    snapshot exceeds `DAEMON_MAX_FRAME_BYTES`. Do not raise the
    default client requirement. Ask a human before adding
    `DaemonEntityFrame` variants.
16. Move background plugin work to `try_admit(Background)`. Keep
    blocking `invoke` only for RequestResponse MCP, UI render/action,
    and package-provider subscribe snapshots.

### D. Evidence and architecture

17. Source tests fail if the new projection path imports terminal
    semantic bodies or names Workspaces/membership/package cleanup
    policy.
18. Rewrite Drain-based natural-exit and `lifecycle_session_drains`
    proofs to observe-slice / page / projection.
19. After merging Hub `main`, do not re-fix the closed WebRTC
    replacement-owner ticket unless the focused test regresses.

## Non-scope

- Reimplementing sliced observe or paged baseline inside Hub.
- Calling unbounded `observe_lifecycle` / `lifecycle_baseline` from
  the owner loop.
- Calling `159d926` `lifecycle_baseline_page` as if it were bounded.
- Package events, `events.emit`, client `SubscribeEvents`.
- Web or TUI UI work.
- Workspaces membership or package cleanup rules.
- Changing ClientWorker, SessionIo, attach phase machines, or
  terminal Drain semantics.
- Replacing RequestResponse `invoke` for MCP/UI.
- Publishing `@trybotster/hub-test-support` unless shipped fixture
  bytes change.
- Dual-pipelining teardown-lens implementation.

## Repository ownership boundaries and cross-repo dependencies

Hub owns projection, scheduling, host-bridge, `/session` admission,
and host-profile worker knobs.

Core owns lifecycle facts, the journal, wake/page, sliced observe,
paged baseline, and `try_admit`.

Registered dependencies (all closed):

| Ticket | Target | Repo | Status |
| --- | --- | --- | --- |
| `ticket_1786663581_962361` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | closed (journal wake/page) |
| `ticket_1786663581_723222` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | closed (`try_admit`) |
| `ticket_1786690597_161141` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | closed at `159d926` (observe only) |
| `ticket_1786690597_154692` | `tgt_7e208a0c76a44980a83b63af976b1f22` | botster-hub | closed at `d92aace` |
| `ticket_1786733177_803101` | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | botster-core | open. Run `run_1786733190_220788`. Elapsed + no full-set clone on baseline pages. |

No new Web, TUI, Workspaces, or Project Pipelines dependency while
client session frames stay `entity_snapshot`.

## Assumptions and unknowns

Assumptions:

- Target routing from `list_spawn_targets` is authoritative.
- The consumable Core pin is the merge SHA of
  `ticket_1786733177_803101`, not `159d926`. Implement uses the
  merged baseline-page names from that ticket.
- RequestResponse `invoke` on MCP/UI/package-provider subscribe is
  the operation.
- Client `entity_snapshot` remains the host-control session contract.
- Existing `session_lifecycle_class` stays the total classifier.
- `lifecycle_session_drains` as a required idle-progress counter is
  obsolete.
- Worktree path has no `:`. Tracked `.gitignore` is present and
  non-empty.
- This is not a Hub session-type eligibility consumer.
- Direct-merge pipeline. No pull request.

Unknowns Implement must not invent:

- Exact numeric `MAX_OWNER_TURN_MS` and
  `MAX_READY_OPERATION_WAIT_MS`. Publish after measurement. They must
  fail if observe still walks every session in one turn.
- Additional compile breaks between Hub `main`'s Core `f4f6bf5` and
  the new baseline-bounded Core SHA. Fix them surgically; do not
  broaden into attach or terminal work.
- Whether later Stage D needs client-visible snapshot chunking.

## Implementation shape

```
authoritative Hub mutation
  -> at most one coalesced try_wake
ready operation
  -> handle request, reply, return
idle / wake
  -> one maintenance slice
  -> yield
observe slice
  -> observe_lifecycle_slice(now, resume, ObserveLifecycleBudget)
  -> store ObserveLifecycleCursor when !complete
  -> ObservePassUnavailable starts a new pass
journal-pull slice
  -> take_journal_advanced_wake
  -> lifecycle_changes_page(after, max_changes, max_bytes)
projection-apply slice
  -> upsert/remove into the one Hub projection
paged-baseline slice
  -> post-ticket_1786733177_803101 bounded baseline page
  -> elapsed starts at API entry
  -> first page may yield before the freeze is fully copied
  -> later pages walk only the next bounded suffix
  -> stop when complete; incomplete is not ended evidence
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

Suggested modules (keep gravity down):

- `src/session_projection.rs` — cursor, rows, apply, ended evidence.
- `src/daemon_maintenance.rs` — wake bit, scheduler, slice budgets.
  Do not reuse `src/maintenance.rs`.
- `src/config.rs` / `src/runtime.rs` — first-class worker knobs and
  variant-complete `BindTerminalAdapter` mapping.
- `src/daemon_entity_subscriptions.rs` — client delivery only.
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`.

## Affected surfaces/files

- merge of Hub `main` into this worktree
- `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-test-support/Cargo.toml`, `Cargo.lock` — one
  exact Core `.git` URL and revision
- Hub source test that those manifests share one Core URL and rev
- `src/config.rs`, `src/persistence.rs`, plugin-bounds tests — six
  host knobs
- `src/runtime.rs` — Core facades; variant-complete bind mapping;
  Background `try_admit`
- `src/daemon_transport.rs` — owner-loop scheduling only
- `src/daemon_entity_subscriptions.rs`
- new `src/session_projection.rs` and `src/daemon_maintenance.rs`
- `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, this plan
- Tests: plugin lifecycle / plugin_bounds; sessions.rs;
  hub_client_api_test.rs; synthetic plugin with executor concurrency
  `> 1`; session-count load; architecture tests

Likely untouched:

- `src/local_webrtc.rs` unless the focused test regresses after merge
- `src/session_types.rs` eligibility
- package event router surfaces
- `src/maintenance.rs` update-check code

## Risks

- Calling unbounded `observe_lifecycle` from the owner loop fails the
  ticket's own load proof.
- Calling `159d926` `lifecycle_baseline_page` reintroduces
  `load_all()` and a remaining-row clone into an owner turn.
- Pinning Core only in `Cargo.lock` while manifests stay on
  `f4f6bf5` or `branch = "main"` lets Git consumers float protocol
  identity.
- Silent Core defaults for the six worker knobs would fail the
  distinct-live-values proof.
- Admitting the next snapshot chunk before the previous completion
  returns can reorder frames when background executors `> 1`.
- Page-then-take-then-sleep can drop a wake.
- Changing client `DaemonEntityFrame` without a feature constant
  would break Web/TUI at protocol 7.
- Growing `daemon_transport.rs` further recreates Hub gravity.

## Acceptance checks/tests

Charter gates after merge of Hub `main` and an exact pin to the
`ticket_1786733177_803101` merge SHA:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `./test.sh --locked`
- `cargo test --doc --workspace --locked`

Product proofs through the production owner loop:

1. Zero Web/TUI subscribers: spawn a short-lived session, never
   `SubscribeEntities`, never Attach/Drain. After sliced observe and
   journal pages, the Hub projection contains a finished **complete**
   baseline ended row.
2. Session-count load: live sessions well above one observe-slice
   `max_sessions`. Ready Spawn/Drain/MCP/UI stay within
   `MAX_READY_OPERATION_WAIT_MS`. Owner turn stays within
   `MAX_OWNER_TURN_MS`. Red on revert: unbounded `observe_lifecycle`
   on the owner loop.
3. Observe resume: a yielded slice with `complete = false` resumes
   with the stored `ObserveLifecycleCursor` and does not re-visit
   earlier ids in that pass. `ObservePassUnavailable` starts a new
   pass.
4. Paged baseline: first-page and later-page large-registry proofs
   stay within item, byte, and elapsed budgets. Incomplete pages have
   `complete = false` and do not prove ended. `SnapshotUnavailable`
   remints. Assembled complete pages match the freeze watermark.
   Red on revert: mint still `load_all()`s or clones remaining rows
   on the owner turn.
5. Synthetic plugin with `plugin_worker_executor_concurrency > 1` and
   reserved RequestResponse `= 1`. Earlier frames have longer
   handlers. Order remains begin → chunks → end → deltas.
6. False-ended matrix: incomplete baseline, omitted UUID,
   `indeterminate`, `entity_remove`, and gap fail `is_ended`.
7. Slow Background handler cannot delay Spawn, Drain, MCP, or UI
   beyond the published ready-operation wait.
8. Consume-order: dropped wake still converges. `BudgetTooSmall`
   raises `max_bytes` / `max_encoded_result_bytes`.
9. Architecture: projection/maintenance modules must not import
   `botster-terminal-protocol-client`, match `ProcessExited` bodies,
   or contain `botster-workspaces`, `membership`, or package
   cleanup-rule identifiers.
10. Distinct live worker knobs including reservation, background, and
    completion. Unload returns to baseline.
11. `BindTerminalAdapter` mapping is total over the four published
    variants.
12. Existing session entity subscription tests still pass.
13. Rewrite
    `session_entity_subscription_observes_natural_exit_without_terminal_attach`
    so the oracle is the Hub projection, not a terminal Drain event.

Downstream proof:

- Not required in Web or TUI checkouts while client session frames
  stay `entity_snapshot`.
- In-repo TypeScript / hub-test-support stay at the current protocol
  unless client frames change.

Live Hub pin:

- Record Hub source SHA and lockfile-pinned Core SHA separately.
- Core SHA must be the exact merge of `ticket_1786733177_803101`.
- Every Git-visible Hub member manifest must declare that same
  `.git` URL and `rev`.

## Runtime-teardown class

`teardown_class_applies`: no.

## Worktree hygiene

- Tracked `.gitignore` is present and non-empty. Do not truncate it.
- Assigned worktree path contains no `:`. Do not set
  `CARGO_TARGET_DIR`.

## Vault gaps worth capturing

- After this Hub merge, capture that Hub owner-loop slices must call
  `observe_lifecycle_slice` and `lifecycle_baseline_page`, not the
  unbounded wrappers.
- Capture that Hub projection must not early-return on zero
  subscribers and must not use `drain_runtime_once` to discover exit.
- Do not capture unpublished owner-turn numbers until Verify measures
  them.

## Pipeline gates and artifacts

- Plan destination:
  `docs/plans/project-session-state-without-blocking-operation-paths.md`
- Direct merge into `main` after Verify. No pull request.
- Vault checklist: `checklist_1786689614_825667`. Skip further
  vault-checklist creation.
- Review overlays: [[botster-runtime-reviewer-playbook]] for the
  owner loop; [[botster-package-reviewer-playbook]] for the synthetic
  plugin fixture.
