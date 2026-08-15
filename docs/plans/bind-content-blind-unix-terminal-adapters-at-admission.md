# Plan: Bind content-blind Unix terminal adapters at admission

Ticket: `ticket_1786661008_634435`
Run: `run_1786681880_322827`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Plan **revision 6** after Plan Review `review_1786687320_855362`

## Plan Review corrections (rev 6)

| Finding | Status |
| --- | --- |
| `finding_1786682759_*` | **Resolved** in rev 4. |
| `finding_1786687109_797525` one-frame Attaching exception | **Locked B.** Attach response may carry only the initial `Attaching` frame. Cold-cut `ticket_1786661010_198387` removes it. |
| `finding_1786687320_585332` fail-closed, do not drop | **Locked.** Core already removed `attached.client_egress` from pending drain. Any pre-bind terminal event other than the initial `Attaching` sentinel is fail-closed: cancel the Hub route, close any adapter candidate, detach the live generation, return `attach_failed`, and require a fresh attach. Do not silently drop. |

### Shipped Core API (origin/main `f4f6bf5`)

```text
CoreDaemon::bind_terminal_adapter(
    client_id,
    session_id,
    subscription_id,
    generation,
    capabilities: TerminalCapabilitySet,
    adapter: Box<dyn TerminalAdapter + Send>,
) -> Result<(), CoreDaemonError>
```

- `TerminalCapabilitySet` lives in `botster-terminal-protocol` and is re-exported from `botster-core`.
- Construct with `TerminalCapabilitySet::from_tokens(...)` (unknown tokens fail here) or `empty()`.
- Advertised tokens: `terminal_streaming`, `resize`, `snapshot_delivery=ready_then_history`.
- Inventory: `adapter_bound` plus `capabilities: Option<TerminalCapabilitySet>` (`None` before bind; bound empty is `Some` empty).
- Bind errors: `BindBeforeAttach`, `UnknownSubscription`, `StaleGeneration`, `AlreadyBound`. There is no bind-time capability-mismatch variant.
- Core uses `snapshot_delivery=ready_then_history` to encode Snapshot frames. Empty set still encodes live output and ProcessExit.

Hub intersection: take `TerminalCompatibility::current()` advertised tokens, keep those LocalOperator admits, and include `snapshot_delivery=ready_then_history` only when the client Hello requires that Hub/terminal feature. Pass the resulting set. Do not store a second Hub copy as attach-phase state. Prove inventory echoes the same tokens.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` (`trybotster/botster-hub`) |
| Plan worktree | this pipeline worktree; Plan does not mutate `Cargo.lock` |
| Worktree hygiene | tracked `.gitignore` has content; path has no `:`; no `CARGO_TARGET_DIR` override required |
| Merge policy | direct into `main`; do not create a PR |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. The ambient checkout is the same repository; routing did not infer it from the working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] (index only; ownership comes from the Hub charter)

Repository overlay implicated by public feature/DTO work inside this repo:

- [[botster-hub-client-playbook]]

Runtime-teardown class applies. Loaded:

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[spa-patterns]] — no React/SPA surface
- other repository charters — this run stays on `botster-hub`

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[hub daemon runtime stays on one owner thread while socket handlers submit requests]]
- [[accepted unixstreams from nonblocking listeners must restore blocking mode for line readers]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core owns the incremental attach phase machine]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Hub route registry names describe ownership not attach queues]]
- [[attach routes use subscription scoped Core drains]]
- [[attach failed cleanup is route aware and idempotent]]
- [[pre READY attach failure creates no attach ownership]]
- [[session shutdown during attach does not produce attach failed]]
- [[hub drain advances non attached session lifecycle]]
- [[hub shutdown preserves durable session workers]]
- [[daemon socket attach must detach subscriptions on disconnect and exit]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Core ClientWorker bind requires a live attach generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[transport ownership north star for modular Botster is proposed]]
- [[proposed Hub admission binds adapters with negotiated subscription capabilities]]
- [[proposed Core transport adapters use bounded writes without policy queues]]
- [[proposed Hub terminal tests enforce content blind adapters]]
- [[proposed Core publishes the transport adapter conformance harness]]
- [[proposed dead sink handling triggers one Core detach without a Hub round trip]]
- [[proposed Hub audits route ownership against Core subscription inventory]]
- [[proposed transport lifecycle lets control connections outlive terminal subscriptions]]
- [[proposed ProcessExited closes terminal subscriptions but not the host session]]
- [[proposed in-device Hub terminal relay is transitional only]]
- [[proposed terminal plane prefers a dedicated stream per subscription]]

The north-star notes remain `decision_state: proposed` in the vault. This ticket is an authorized slice of project `project_1786660949_205223` and therefore implements those rules for the Unix endpoint without treating the wider proposal as already ratified for WebRTC, cold-cut, or client decoders.

## Context loaded

- Pipeline ticket, run, dependency, project north star, and sibling tickets via `project_pipelines_current_context` / `project_pipelines_get_project` / `project_pipelines_get_ticket`
- Closed Core parent `ticket_1786661004_845807` (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`): ClientWorker push, bind, inventory, generation-aware detach
- Closed Core grandparent `ticket_1786661004_133253`: `TerminalAdapter` contract and published harness
- Closed Core dependency `ticket_1786682902_405026` merged at `f4f6bf5babe92dfb9241a760c414187f711c2c42` (`origin/main` equals that SHA). Human 1B.
- Human answer `question_1786682822_139812`: 1B + 2A
- Human answer `question_1786687076_252448`: staged option B — one-frame Attaching exception; cold-cut ticket description updated
- Shipped types read from Core `f4f6bf5`: `TerminalCapabilitySet`, `bind_terminal_adapter(..., capabilities, adapter)`, inventory `capabilities: Option<TerminalCapabilitySet>`
- Current Hub lock still pins Core `033cd01`. Implement `cargo update`s to `f4f6bf5`.

## Botster layers touched

- Rust Hub daemon Unix transport and `HubRuntime` facade
- In-repo `botster-hub-client` optional feature + content-blind mux helper
- Hub tests / isolated daemon proofs
- Not: Lua plugins, Web, TUI, TUI Kit, WebRTC adapter, Core implementation, Project Pipelines package

## Worktree / target assumptions

- Implement stays in this run's Hub worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Do not edit `botster-core`. Both Core parents are closed.
- Implement `cargo update`s Hub Core git deps to `f4f6bf5babe92dfb9241a760c414187f711c2c42` (`botster-core`, `botster-core-daemon`, `botster-core-test-support`, and `botster-terminal-protocol` if `from_tokens` / feature constants need a direct dep). `botster-core` already re-exports `TerminalCapabilitySet`.
- After the lock update, assert the locked Core SHA is `f4f6bf5babe92dfb9241a760c414187f711c2c42` unless `origin/main` moved forward while remaining a descendant of that commit.

This ticket is **not** a consumer of Hub session-type eligibility work. No `list_session_types_for_target` pin injection.

## Scope

1. **Admit, then bind (staged attach handshake).** After Hello + `LocalOperator` and `Attach`:
   1. `start_attach` records ownership.
   2. `attach_client` / `CoreDaemon::attach` returns the initial route-owned `Attaching` frame before a generation can be bound.
   3. **Temporary exception (`question_1786687076_252448` B):** the Attach response may translate **only** that initial `Attaching` frame through the existing Hub event path.
   4. **Fail-closed on any other pre-bind terminal event (`finding_1786687320_585332`):** if `client_egress` contains Snapshot, TerminalOutput, ProcessExit, or any later AttachState, do **not** drop those frames and do **not** bind. Core has already removed them from pending drain, so a silent drop loses them. Required outcome: cancel the Hub route, close any adapter candidate, detach the live generation, return `attach_failed` (no live attach ownership), and require a fresh attach.
   5. Only after the Attach egress is exactly the one `Attaching` sentinel: read the live generation, construct `TerminalCapabilitySet`, create `UnixTerminalAdapter`, and `bind_terminal_adapter`.
   6. After bind, every later terminal frame leaves only through the opaque adapter. Hub must not inspect or branch on later AttachState, Snapshot, TerminalOutput, or ProcessExited bodies.
   7. Cold-cut `ticket_1786661010_198387` must remove this one-frame exception. This ticket does not add a Core atomic attach-and-bind API.
2. **Keep route records ownership-only.** Extend `AttachStream` / `AttachStreamRegistry` with generation and adapter-bound flags only. Do not store READY / PAGE / FINISH / `Attached` / snapshot bytes, and do not keep a second copy of the capability set as Hub attach-phase state. Prefer route/ownership names.
3. **Capability intersection at admission; bind the result into Core.** Use the shipped `f4f6bf5` API:
   - Build `TerminalCapabilitySet` with `from_tokens` from the intersection of `TerminalCompatibility::current()` advertised tokens, LocalOperator admission, and the client's required terminal tokens. Unknown tokens fail at `from_tokens`, not at bind.
   - Include `snapshot_delivery=ready_then_history` only when the client asked for incremental snapshot delivery. Empty is valid and still delivers live output.
   - Call `bind_terminal_adapter(..., generation, capabilities, adapter)`.
   - Prove inventory `capabilities` equals the set Hub passed.
   - Advertise an optional **Hub** daemon feature for the Unix adapter *plane* so current clients stay unbound. Do not put that Hub feature in `DaemonCompatibilityRequirement::current()`. `PROTOCOL_VERSION` stays 7. Bump `CONFORMANCE_FIXTURE_REVISION` 38 → 39 if fixtures change. Bind when Hello requires the Hub Unix-adapter feature. Unbound Unix attaches keep Drain translation until the cold-cut ticket.
4. **Content-blind write path, with one named exception.** `try_write` calls `TerminalFrame::to_bytes()` and writes an envelope that exposes only a plane/kind tag plus opaque payload. After bind, Hub must not deserialize frame bodies or branch on READY, PAGE, FINISH, later `AttachState`, or `GHOSTSNP`. The only allowed old-path terminal event is the pre-bind initial `Attaching` handshake on the Attach response.
5. **Mux on the admitted connection.** Do not invent a second listen path or SCM_RIGHTS socket in this ticket. The control connection continues request/response for host control. After bind, the connection loop also writes unsolicited opaque terminal envelopes from the adapter's one in-flight slot. Dedicated per-subscription streams stay out of scope unless a later ratified protocol ticket requires them.
6. **One-slot non-blocking adapter.** Implement `TerminalAdapter` with typed `WouldBlock` / `Full` / `Closed`, `close()` that returns without I/O wait or taking a lock the writer holds, and `pressure()`. The one slot is transport state, not a ClientWorker policy queue. No adapter retry.
7. **Explicit Detach.** After authorization, forward Detach through `HubRuntime`. Close the adapter if it is still open. Core detach is idempotent by subscription id + generation.
8. **Disconnect / revocation vs explicit Detach (locked).** Split cleanup by route class.
   - Bound Unix route + EOF / write failure / Unix revocation: close the adapter only. Do **not** send `HubClientRequest::Detach` or `detach_client`. Core `Closed` is the one mechanical detach.
   - Authorized explicit client `Detach`: forward through `HubRuntime`, then close the adapter if it is still open.
   - Unbound Unix / WebRTC routes: keep today's cleanup Detach (transitional Drain path).
   - Never call `ShutdownSession` from adapter close. Adapter close must not shut down the host session.
9. **Reconcile against Core inventory.** On the existing control-plane reconcile tick, compare Hub route records to `list_terminal_subscriptions()`. Release Hub ownership for rows Core no longer reports. Never infer loss from terminal-stream silence.
10. **Keep control-plane drains.** Session lifecycle, entities, and unbound terminal routes continue to Drain. After bind, a bound Unix route must not emit AttachState, Snapshot, TerminalOutput, or ProcessExited via Drain or any later Attach response. The initial Attaching exception is Attach-response only, and only before bind.
11. **Import Core's harness.** The production Unix adapter (not Core's `UnixShapedTerminalAdapter`) implements `TerminalAdapterHarnessDriver` and passes `assert_terminal_adapter_conformance`.

## Non-scope

- WebRTC DataChannel adapter (`ticket_1786661008_247079`)
- Cold-cut of Hub terminal Drain translation **and** the one-frame Attaching exception (`ticket_1786661010_198387`; removal is registered on that ticket, not implemented here)
- Web / TUI protocol-plane consumers
- Implementing Core bind/inventory capability-set APIs (that is `ticket_1786682902_405026`)
- Core contract, queues, attach phase machine, or harness ownership beyond consuming the closed Core parent
- Dedicated Unix terminal sockets / SCM_RIGHTS
- Host session shutdown, worktree, or retention policy changes
- Raising the default client requirement or a protocol flag day
- Speculative write coordinators beyond the one-slot adapter
- Project Pipelines package/plugin work
- Optional configurability knobs beyond the existing advertised-feature admission pattern

## Binding decisions

| Topic | Decision |
| --- | --- |
| When Unix Attach binds | After attach returns the initial Attaching handshake, inventory has a live generation, Hello requires the optional Unix-adapter feature, and LocalOperator admission is live |
| One-frame exception | Only the pre-bind `Attaching` event on the Attach response. Any other pre-bind terminal event fail-closes (cancel, close candidate, detach generation, `attach_failed`). Cold-cut removes the exception |
| When Unix Attach stays unbound | Current clients that do not require the Hub Unix-adapter feature; WebRTC attaches (`grant_id` present) |
| Unix grant / revocation (2A) | Hello + `LocalOperator`. No `BootstrapGrant`. Admission lifetime = connection lifetime. EOF / write failure / close revokes admission and closes the adapter |
| Negotiated capabilities (1B) | `TerminalCapabilitySet` via `from_tokens`. Bind requires it. Inventory echoes `Option<TerminalCapabilitySet>` |
| Write sink | The admitted Unix connection, muxed with control JSON lines |
| Envelope | Kind/plane tag + opaque `TerminalFrame::to_bytes()` payload. No Snapshot/READY/PAGE/FINISH fields |
| Why not always-bind | Always-bind would insert unsolicited frames onto protocol 7 and empty Drain Snapshot for current TUI/Web before their tickets land, and would raise a de-facto flag day. The ticket also requires Drain translation to remain until cold-cut |
| Why not a second socket | Ticket says “from the admitted connection”; dedicated streams are still proposed |
| Core pin | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `HubRuntime` | Add thin `bind_terminal_adapter`, `list_terminal_subscriptions`, and `detach_terminal_subscription` facades. Socket handlers do not touch raw Core routers |
| Threading | `HubRuntime` stays on the owner thread. `try_write` / `close` run on that tick. The connection task owns the write half. Coordinator must be non-blocking and close-safe |
| Naming | Route / ownership terms only. Do not add queue names |
| Bound-route socket death | Close adapter only. No Hub Detach round trip. Prove one Core detach via inventory absence |
| Explicit Detach | Authorized request only. Forward, then close leftover adapter |
| `AttachStreamRegistry` | Replace the manual `Default` impl with `#[derive(Default)]` on the same edit |

## Runtime-teardown class answers

`teardown_class_applies`: yes. The ticket binds ClientWorker adapters, closes them on disconnect/revocation, and must not confuse adapter close with host session shutdown.

| Field | Answer |
| --- | --- |
| `teardown_isolation` | One Unix subscription owns one adapter, one route record, and one Core generation. Closing or failing that adapter tears down only that `(session_id, subscription_id, generation)`. Sibling subscriptions on the same session and other clients stay live. Host session workers stay up (`[[hub shutdown preserves durable session workers]]`). |
| `teardown_bounds` | `try_write` and `close()` are non-blocking. `close()` and `Drop` must not wait on socket I/O or a writer lock. Core already fails a stalled head frame after 512 ticks and hard-stops with synchronous `close()` + drop on the host tick. Hub must not add `block_on(close)` on the control thread. A hanging adapter `close()` is an adapter defect and fails the Core harness. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | Exact happy path: Unix accept → Hello + LocalOperator (Unix-adapter feature required) → intersect tokens → `Attach` → ownership row → `begin_core_attach` returns **only** the initial `Attaching` event on the Attach response → inventory generation → `UnixTerminalAdapter` → `bind_terminal_adapter(generation, capability_set, adapter)` → owner-thread pump. Live oracles: (1) Attach response contains only `Attaching`; (2) inventory shows `adapter_bound` and the same capability tokens; (3) later frames are opaque adapter envelopes only; (4) connection death closes adapter, no Hub Detach, inventory gone, session listed; (5) explicit Detach separately produces one Core detach. **Unexpected pre-bind frame:** inject or observe Snapshot / later AttachState / TerminalOutput / ProcessExit in attach egress → Hub cancels route, closes adapter candidate, detaches generation, returns `attach_failed`, no live ownership, inventory row gone. Record Hub SHA and locked Core SHA separately. |
| `ownership_identity` | Hub route key is `(client_id, session_id, subscription_id, generation)`. Unix has no `BootstrapGrant`; `grant_id` stays `None` by 2A. Core identity is `(session_id, subscription_id, generation)` plus the bound capability set. Reused `subscription_id` is generation N+1. Stale detach / late `Closed` for N must not delete N+1. Disconnect cleanup is owner-tagged by `client_id`. |
| `sibling_fail_closed_policy` | Successful adapter close: siblings keep working; host session stays. Ultimate close/write-budget failure: only that subscription dies. ProcessExited closes every terminal subscription on that session by Core design and still does not shut down the host session. Same-key replacement is Core's generation bump; Hub replaces its route record for that key and must not decrement a sibling. |

### Late-message matrix

| Message | Grant / owner tag | Reject after terminal failure | Residual sweep if it races close |
| --- | --- | --- | --- |
| `Attach` | Unix `client_id` after Hello + LocalOperator (`grant_id` is None by 2A) | Translate only the initial `Attaching` frame. Any other pre-bind terminal event: cancel route, close adapter candidate, detach generation, `attach_failed`, no ownership. Do not treat `ShutdownSession` as `attach_failed` | `cancel_stream` + generation detach; idempotent |
| `bind_terminal_adapter` | live attach generation + `TerminalCapabilitySet` | Unknown tokens fail at `from_tokens`. Bind rejects `BindBeforeAttach` / `UnknownSubscription` / `StaleGeneration` / `AlreadyBound`. Close the rejected adapter on the same stack | Rejected adapter never enters inventory |
| `Detach` | route owner `client_id` | Foreign owner forbidden. Generation mismatch leaves N+1 | Forward Detach; close adapter if open; Core idempotent |
| Disconnect / EOF / write failure | connection `client_id` (this **is** Unix revocation) | No new Attach/bind for that dead connection | Bound: `adapter.close()` only. Unbound: existing cleanup Detach. Never `ShutdownSession` |
| Unix admission revocation | same as connection death (2A). Not a WebRTC `BootstrapGrant` | No independent Unix revoke RPC in this ticket | Same as disconnect close |
| WebRTC grant revocation | existing `grant_id` peer map | Unix path does not create grant-owned adapters | Leave WebRTC forget/sweep unchanged |
| `Drain` | existing `authorize_drain` | Foreign owner forbidden | Bound routes return control events only; no Snapshot reconstruction |
| `SendInput` / `Resize` / `ModeGatedInput` | existing HubRuntime session owner | After detach, Core rejects; Hub does not recreate the adapter | No adapter write of input |
| `SubscribeEntities` | existing grant/client tag | Existing post-PeerClosed reject stays | Out of this ticket except “do not regress” |
| Inventory reconcile | control-plane only | Missing Core row → drop Hub route | Never infer from adapter silence |

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This ticket |
| --- | --- | --- |
| Unix admission (Hello + LocalOperator), route records, adapter instance, framing, transport write | Hub | Implement |
| Terminal queues, attach phases, slow-client policy, mechanical detach, inventory, **immutable bound capability set** | Core | Consume shipped `f4f6bf5` API |
| `TerminalAdapter` + conformance harness | Core test support | Import; do not fork |
| Opaque `TerminalFrame` | `botster-terminal-protocol` | `to_bytes()` only; do not depend on the client crate |
| External control DTOs / feature constants | `botster-hub-client` (this repo) | Optional feature + mux helper only |
| WebRTC adapter | Hub sibling ticket | Register, do not implement |
| Drain cold-cut | Hub sibling `ticket_1786661010_198387` | Leave translation in place |
| Web / TUI decoders | those targets | Downstream tickets; do not implement here |

Registered dependencies against Core `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`:

- `ticket_1786661004_845807` (closed) — ClientWorker push / teardown
- `ticket_1786682902_405026` (closed, merged `f4f6bf5babe92dfb9241a760c414187f711c2c42`) — immutable capability set on bind + inventory. Do not reimplement that API in Hub.

Sibling consumers to leave registered against their own targets:

- Hub WebRTC adapter `ticket_1786661008_247079`
- Hub cold-cut `ticket_1786661010_198387`
- Web `ticket_1786661008_897067`
- TUI `ticket_1786661009_551067`
- Integration `ticket_1786661010_115885`

## Assumptions and unknowns

- Vault north-star notes are still marked proposed; the project ticket plus `question_1786682822_139812` and `question_1786687076_252448` authorize this Unix slice.
- The one-frame Attaching exception is a staged dual path. The final architecture still has no dual path; cold-cut `ticket_1786661010_198387` removes it.
- Unexpected pre-bind terminal events are fail-closed, never dropped. Core already consumed those frames from pending drain.
- Hello.compatibility is the opt-in for the Hub Unix-adapter *plane* because it is already reserved for client-admission policy.
- Human 1B: capability tokens are `TerminalCapabilitySet`. Hub intersects and passes; Core stores. Type is shipped, not invented.
- Human 2A: Unix “grant” in the ticket text means Hello + LocalOperator admission. Unix “revocation” means connection death. This is transport-specific, not a silent waiver.
- Bound-route socket death is adapter close only. Idempotent Core detach is not a license for a second Hub Detach.
- `docs/plans/` is the live Hub plan home (no retired-directory stub).

Unknowns Implement must resolve from the lock after `cargo update`, not invent:

- Whether `terminal_subscription_generation` is public on `CoreDaemon` or only via inventory (`DefaultBotsterEngine` has it)
- Whether Hub needs a direct `botster-terminal-protocol` dep for `FEATURE_*` / `TerminalCompatibility`, or only the `botster-core` re-export of `TerminalCapabilitySet`

## Affected surfaces / files

Create:

- `src/unix_terminal_adapter.rs` (or `src/terminal_adapter/unix.rs`) — production adapter + harness driver
- `docs/plans/bind-content-blind-unix-terminal-adapters-at-admission.md` — this plan
- `docs/reports/bind-content-blind-unix-terminal-adapters-at-admission-implement.md` — Implement report

Edit:

- `Cargo.toml` / `Cargo.lock` — pin Core parent that contains bind/inventory
- `src/runtime.rs` — bind / inventory / generation-aware detach facades
- `src/daemon_attach_stream.rs` — ownership-only generation + bound flag; inventory reconcile; `#[derive(Default)]` instead of the manual impl that fails current clippy
- `src/daemon_transport.rs` — Hello opt-in, Attach bind, mux write, disconnect close, no session shutdown
- `src/lib.rs` / `src/daemon.rs` — module wiring only as required
- `crates/botster-hub-client/src/lib.rs` — optional feature constant, `for_unix_terminal_adapter()`, advertise in `current_feature_list()` only
- `packages/hub-test-support` support-matrix / protocol assets if the feature list is source-derived
- `docs/client-protocol.md` — adapter plane, Drain residual, feature opt-in
- Focused tests under `src/unix_terminal_adapter.rs`, `src/daemon_attach_stream.rs`, and `tests/hub_daemon_lifecycle/`

Do not add Hub-owned adapter-law crates. Do not decode GHOSTSNP in tests.

## Risks

- Lock update to `f4f6bf5` may pull more than bind/inventory. Stay inside this ticket; do not “fix forward” Core.
- Translating more than the initial Attaching frame would recreate a Hub terminal drain. Silently dropping a pre-bind Snapshot / output / exit loses Core frames that are no longer on pending drain. Fail-closed is mandatory.
- Muxed unsolicited frames can corrupt request/response if the write coordinator is racy. The adapter slot and control responses must serialize on the connection write half without blocking `close()`.
- Binding without the feature gate would break current Unix TUI/Web on `main`.
- Calling `ShutdownSession` from adapter close would violate host-policy ownership and durable-worker rules.
- Reconcile that inspects Drain silence would recreate a Hub attach state machine.
- Using Core's published `UnixShapedTerminalAdapter` as the production object fails consumer proof.
- `close()` that `block_on`s the writer is a Plan Review reject.
- Feature-constant work that also updates `default_required_feature_list()` raises the client floor.

## Acceptance checks / tests

Repository gates, in this order (clippy must be green before the test wrapper so `botster-session-worker` exists):

```sh
cargo fmt --all -- --check
BOTSTER_ENV=test cargo clippy --workspace --all-targets --locked -- -D warnings
./test.sh --locked
```

Plan Review proved the current base fails clippy on `AttachStreamRegistry`'s manual `Default`. That fix is in scope because this ticket already edits that file. After clippy is green, rerun `./test.sh --locked` and record whether any independent failure remains.

Focused proofs Implement must add or update:

1. **Harness:** Hub `UnixTerminalAdapter` driver passes `assert_terminal_adapter_conformance` (bounds, order, typed rejection, no retry, content-blind `to_bytes`, local close and transport close during an active write).
2. **Production bind path:** isolated Hub + worker from locked Core `f4f6bf5`; Hello requires the Hub Unix-adapter feature; spawn; Attach may return **only** the initial `Attaching` event on the old path; then bind; inventory `adapter_bound=true` with the live generation **and `capabilities ==` the `TerminalCapabilitySet` Hub passed**; later frames appear only as opaque adapter envelopes. Snapshot frames appear on the adapter only when the bound set contains `snapshot_delivery=ready_then_history`.
3. **One-frame exception + fail-closed proof:**
   - Happy: Attach response contains only the initial `Attaching` event; after bind, Drain and any later old-path response contain no AttachState, Snapshot, TerminalOutput, or ProcessExited.
   - Unexpected pre-bind frame: the route is cancelled, any adapter candidate is closed, the live generation is detached, the client sees `attach_failed`, and inventory has no live row. Silent drop is a test failure.
   - Document that `ticket_1786661010_198387` removes the Attaching exception.
4. **Negative content-blind:** Hub production sources and new tests do not branch on READY, PAGE, FINISH, or decode `GHOSTSNP`. The only permitted AttachState match is the pre-bind `Attaching` sentinel on the Attach response. Prefer compile/test guards over comments.
5. **Unix revocation / connection death (bound):** drop the client (EOF, write failure, or close); Hub issues no Detach; Core inventory row gone via adapter `Closed`; session still listed; no `ShutdownSession`. This is the 2A revocation proof.
6. **Explicit Detach (bound):** authorized Detach is forwarded; leftover adapter is closed; second Detach / close is idempotent; generation N cannot delete N+1. Prove this path separately from connection death.
7. **Unbound residual:** Unix Attach without the feature still Drains Snapshot as today. WebRTC `grant_id` attaches do not receive a Unix adapter.
8. **Pre-READY failure:** `attach_failed` creates no adapter bind and no live attach ownership.
9. **Reconcile:** after Core inventory drops a row, Hub route record is released without inferring from silence.
10. **Compatibility:** default requirement still accepts the previous descriptor; request-specific requirement rejects a daemon that lacks the new feature; `PROTOCOL_VERSION` remains 7.
11. **Provenance:** record Hub SHA, locked Core SHA, and realpaths of the tested `botster-hub` / `botster-session-worker` binaries.
12. **Unbound fast-exit visible state (Core pin contract):** after `f4f6bf5`, a `printf` attach still returns only the initial `Attaching` handshake then Snapshot/`Attached` on Drain. Visible text is on `ReadScreen`. Host lifecycle stays `running`. Production `hub smoke` must use Attach + scoped Drain until `Attached` + `ReadScreen`, not `stream_attach`'s ProcessExit/`exited` wait. The fast-exit diagnostic may accept `read_screen_marker` as the visible-state proof.
13. **Unexpected pre-bind fail-closed:** unit-test the predicate and cancel/detach helpers. IsolatedHub cannot inject extra Core attach egress without a Core hook; do not treat missing IsolatedHub injection as a silent-drop waiver.

Downstream proof required by charter: this ticket's consumer proof is Hub importing Core's harness and binding through `CoreDaemon` on the production Unix accept/Attach path. Web and TUI authentic Ghostty proofs belong to their tickets and the integration ticket. Do not weaken those later proofs.

## Required docs

- `docs/client-protocol.md`: optional Hub Unix-adapter feature, bind sequence including the one-frame Attaching exception and Core capability-set pass-through, opaque mux envelope, Drain residual until cold-cut, and the 2A meaning of Unix admission/revocation.
- Implement report under `docs/reports/`.
- No plugin README. No crates.io / npm publish in this ticket unless the support-matrix fixture is already published-in-tree and must stay source-derived.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Vault checklist: one Plan-visit checklist on the ticket; skip duplicates.
- Gate evidence must include `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, `target_repository`.

## Vault gaps worth capturing

- After both Core capability-bind and this Hub ticket ship: capture that Unix revocation is connection death (2A) and that negotiated terminal capabilities are Core subscription state (1B).
- Whether Hello.compatibility becomes the general client-admission policy switch, or stays Unix-adapter-specific, should be captured once a second caller exists.
- Do not capture the proposed north star as ratified from this ticket alone.
