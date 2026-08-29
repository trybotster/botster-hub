# Hub Decomposition 2: Extract Admission And Subscription Ownership

## Target Repository And Target Id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket: `ticket_1787894416_777916`. Run: `run_1787977061_443918`. Step: `botster_stack_plan`, run step `run_step_1787977062_988621` (first Plan visit).
- The pipeline resolved the target id through `list_spawn_targets`, which maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The plan does not infer the repository from the working directory.
- Blocking dependency `ticket_1787894414_324976` (Hub decomposition 1: client DTO, shutdown, and daemon support modules) is `closed` and merged. No open blocking dependency remains.
- Base commit: `fd540b6`. `git rev-parse HEAD` and `git rev-parse origin/main` both return `fd540b6` after `git fetch origin main`. The worktree is not behind `origin/main`.
- Per visit, the authoritative enumeration of plan commits lives in that visit's gate evidence, not in this document.

## Repository Playbook Loaded

- [[botster-hub-playbook]] -- the repository ownership charter for `botster-hub`.

## Other Role And Surface Playbooks And Atomic Notes Loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Atomic notes that constrain this ticket:

- [[daemon transport extraction moves ownership before deleting the facade]] -- the frozen target directory map and the migration order. This ticket is migration step 3.
- [[Hub extraction must reduce ownership rather than only split files]] -- an extraction moves implementation, state, policy, and tests; file count proves nothing.
- [[hub moves must extend source scanning guard file lists]] -- a move can leave a fixed `include_str!` or `hub_source()` guard green while it no longer scans the moved code. This note names commit `468bf7f` from decomposition 1 as the worked example.
- [[fixed source guard lists need one ablation per added file]] -- one representative destination arm cannot prove the other list entries.
- [[botster hub gravity must be watched before it becomes the new monolith]] -- the drift this decomposition answers.
- [[botster hub is a first party host profile over core]] -- Hub owns trusted product policy over policy-free Core.
- [[botster Hub Rust stays a trusted host kernel]] -- Hub Rust owns privileged boundaries only.
- [[Hub route registry names describe ownership not attach queues]] -- route state names must say ownership, not queue.
- [[ShutdownSession suppresses exact route generations before Core teardown]] -- the exact `(session_id, subscription_id, generation)` suppression order this ticket relocates without changing.
- [[ShutdownSession suppression live tests are not a red oracle]] -- suppression order relies on deterministic unit lanes, not live no-event tests.
- [[a public occupancy oracle must union Hub routes with Core inventory]] -- the occupancy invariant the moved occupancy code must preserve.
- [[PeerClosed attach occupancy must use the live attach route set]] -- peer cleanup uses the live route set, not an independent counter.
- [[Unix EOF occupancy must share the live attach route set]] -- Unix cleanup shares the same route-set occupancy path.
- [[Unix Hello can reject terminal admission while host operations remain available]] -- the Hello admission invariant that must survive the move.
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]] -- WebRTC adapter bind follows encrypted admission.
- [[webrtc bootstrap origin must be requested after the package server binds]] -- origin-bound grants are issued after the supervised listener binds.
- [[Client event holders are connection-scoped]] -- the holder identity that must survive the package-event relocation.
- [[exact owner plus name is the only package event subscription key]] -- the event subscription key invariant.
- [[Core terminal subscription ownership is session, subscription, and generation]] -- Core mints subscription generations; Hub records them.
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]] -- the seam this ticket must not cross.
- [[botster runtime teardown lenses]] -- loaded because this ticket moves peer ownership identity, route ownership, occupancy, and close bookkeeping. See the runtime-teardown section.
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]] -- the sibling policy that must survive the grant extraction.
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]] -- strict gates must run under Rust `1.97.0`.
- [[Hub official gates must not set CARGO TARGET DIR]] -- the official locked gate needs the default worktree `target/`.
- [[Hub suite runs prebuild the session worker before the locked test wrapper]] -- prebuild before `./test.sh --locked`.
- [[strict clippy can hide later crate diagnostics behind the first compile failure]] -- rerun the full workspace Clippy after each repair.
- [[a ui contract import line change costs one test line in each generic client]] -- the downstream cost rule that a zero-DTO-change move must keep at zero.
- [[a regression test must be shown to go red with the fix reverted]] -- every added guard entry needs a red arm.
- [[express scope limits as invariants not closed enumerations]] -- this plan states commit kinds as invariants rather than a fixed commit count.
- [[integration tests should use public agent apis not crate-internal test-only helpers]] -- moved unit tests stay unit tests inside their new modules.

Required Botster planning context from [[botster-planner-playbook]]:

- [[botster-architecture]] -- the Botster domain map. It names [[daemon transport extraction moves ownership before deleting the facade]] as current architecture, which confirms this ticket is a ratified migration step and not an opportunistic refactor.
- [[cli-patterns]] -- Rust CLI, TUI, PTY, and terminal-layer constraints.
- [[spa-patterns]] -- this ticket touches no SPA surface. No DTO field, serde name, or protocol version changes, so [[botster hub client state sync is entity frame only]] holds unchanged.
- [[botster orchestration should spawn agents with explicit target ids]] and [[botster orchestration prompts must bind agents to explicit worktrees]] -- satisfied: this run binds `tgt_7e208a0c76a44980a83b63af976b1f22` and the pipeline-provided ticket worktree.

[[project-pipelines-playbook]] is **not** loaded. This ticket changes no Project Pipelines package, plugin, or workflow policy path.

## Context Loaded

- Vault capture: `ops/archive/inbox/2026-08-27-botster-wake-driven-data-plane-and-hub-decomposition.md` (vault commit `8ef01f56`), distilled into [[daemon transport extraction moves ownership before deleting the facade]]. It freezes the target Hub directory map and puts this ticket at migration step 3.
- Project record `project_1787600579_585482`: the decomposition order, the cold-cut rules, and the rule that every extraction commits move-only before behavior changes.
- Decomposition 1 plan and report in this repository: `docs/plans/hub-decomposition-1-extract-client-dto-shutdown-and-daemon-support-modules.md` and `docs/reports/hub-decomposition-1-extract-client-dto-shutdown-and-daemon-support-modules-implement.md`. They establish the module shape (`src/daemon.rs` beside `src/daemon/`), the crate-private module rule, and the source-guard restore that commit `fd540b6` completed.

Repository evidence measured at `fd540b6`:

| Fact | Evidence |
|---|---|
| `src/daemon_transport.rs` | 8,052 lines |
| `src/local_webrtc.rs` | 7,871 lines |
| `src/daemon_entity_subscriptions.rs` | 4,066 lines |
| `src/daemon_event_subscriptions.rs` | 1,245 lines |
| `src/daemon_attach_stream.rs` | 1,138 lines |
| `src/unix_terminal_adapter.rs`, `src/webrtc_terminal_adapter.rs` | 942 and 926 lines |

- `src/daemon_attach_stream.rs`, `src/daemon_package_control.rs`, and `src/daemon_entity_subscriptions.rs` are declared inside `src/daemon_transport.rs` as `#[path = "..."] mod` submodules at lines 147, 158, and 166. They are therefore owned by `daemon_transport` today, not by the crate root.
- `src/daemon_event_subscriptions.rs` is declared at `src/lib.rs:59` as a private crate-root `mod`.
- `src/lib.rs:55-76` shows that `client_api_dto` is `pub(crate) mod` and `local_webrtc` is `pub(crate) mod`. Neither `daemon_attach_stream`, `daemon_entity_subscriptions`, nor `daemon_event_subscriptions` has any public path.
- Admission state lives in `PendingRuntimeState` at `src/daemon_transport.rs:5295`, with fields `streams: AttachStreamRegistry`, `unix_admissions`, `webrtc_admissions`, `close_work`, and `host_compatibility`. `UnixTerminalAdmission` is at line 5263, `WebrtcTerminalAdmission` at 5277, and `HostCompatibilityRecord` at 5290.
- Hello admission functions live at `src/daemon_transport.rs:4982` (`daemon_hello_ack`), `4991` (`unix_hello_admission`), `5022` (`terminal_compatibility_attach_error`), and `5360` (`next_admission_key`).
- Resource budgets live at `src/daemon_transport.rs:176-184`: `DAEMON_CLIENT_WRITE_TIMEOUT`, `DAEMON_HANDSHAKE_TIMEOUT`, `DAEMON_INCOMPLETE_FRAME_TIMEOUT`, `DAEMON_MAX_FRAME_BYTES`, `DAEMON_MAX_CONNECTIONS`, `DAEMON_MAX_REJECTION_TASKS`, `DAEMON_CONTROL_QUEUE_CAPACITY`, and `ENTITY_SUBSCRIPTION_QUEUE_CAPACITY`.
- Close bookkeeping is duplicated across the two transports. `UnixMuxInner` at `src/unix_terminal_adapter.rs:282` and `WebRtcMuxInner` at `src/webrtc_terminal_adapter.rs:307` each hold `routes: Mutex<BTreeMap<(String, String, u64), _>>`, `pending_events: Mutex<Vec<DaemonEvent>>`, `suppress_generations: Mutex<BTreeSet<(String, String, u64)>>`, and `close_work`.
- Measured duplication: `diff` between `src/unix_terminal_adapter.rs:378-545` and `src/webrtc_terminal_adapter.rs:411-585` produces 24 diff lines over roughly 170 lines. The only substantive differences are the wake call (`self.inner.notify.notify_waiters()` against `self.inner.wake.wake()`), the qualified path to `ClosedEventSliceProgress`, and the WebRTC-only `drop_pending_events`. The close-event state machine is therefore one machine written twice.
- `ClosedEventSliceProgress` is declared at `src/unix_terminal_adapter.rs:300` and the WebRTC mux refers to it as `crate::unix_terminal_adapter::ClosedEventSliceProgress`. `src/daemon_transport.rs:5370` (`empty_close_event_progress`) does the same.
- Suppression entry points live at `src/daemon_transport.rs:5069` and `5077` (`suppress_unix_session_close_events`, `suppress_webrtc_session_close_events`), with the close-events owner phase at `5378` (`run_close_events_phase`) and the emit decision at `5085` and `5089`.
- Route ownership residue in `daemon_transport` includes `AttachedSubscription` (5632), `AttachedSubscriptionChange` (5638), `apply_attached_subscription_change` (2427), `record_attached_subscription_change` (5711), `attached_subscription_change_for_response` (5914), `response_records_attach_ownership` (5910), `overlay_live_attach_occupancy` (5663), `live_attach_occupancy_rows` (5679), and `unix_eof_cleanup_ablation` (5651).
- `DaemonControlState` at `src/daemon_transport.rs:5519` holds subscription state that has no single owner today: `entity_subscriptions`, `event_plane`, `pending_runtime`, `released_entity_generations`, `released_attach_generations`, and `live_attach_routes`.
- Grant policy lives in `src/local_webrtc.rs`. `LocalWebrtcTransport.grants` is a field at line 178. `issue_bootstrap` is at 222, `signal` at 258, `prune_expired_grants` at 525, `LocalWebrtcSignalRequest` at 606, `LocalWebrtcGrant` at 613, and `LocalWebrtcGrant::validate` at 622. `GRANT_TTL_SECONDS` is at line 50.
- Key derivation from the admitted secret lives at `src/local_webrtc.rs:2436` (`secret_stream_key`), with `random_secret_token` at 2430 and `random_token` at 2424. `answer_offer` at line 1880 calls `secret_stream_key(&request.grant_secret)`, so the grant secret currently travels into the transport handshake path.
- Origin policy lives in two places: `expected_origin` on `LocalWebrtcGrant`, and `origin_from_local_url` at `src/daemon_transport.rs:4898` beside `issue_local_webrtc_bootstrap_response` at 4825.
- No peer-generation counter exists anywhere in `src/` or `tests/`. `grep -rn "peer_generation"` returns nothing. Peer ownership identity today is the `grant_id`, carried on `LocalWebrtcPeerState.grant_id` (`src/local_webrtc.rs:691`) and on `AttachStreamOwner` (`src/daemon_attach_stream.rs:83`), compared by `AttachStream::owner_matches` (`src/daemon_attach_stream.rs:109`).
- Source-scanning guards at `fd540b6`:
  - `production_sources_reject_terminal_drain_and_snapshot_phase_decode` in `src/lib.rs:996-1038` scans a fixed fifteen-entry `include_str!` list that names `src/daemon_transport.rs`, `src/daemon_entity_subscriptions.rs`, `src/daemon_attach_stream.rs`, `src/local_webrtc.rs`, the five `src/client_api_dto/` files, and the two `src/daemon/` files.
  - `no_lua_dispatch_in_terminal_input_or_output` in `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:540` names five files and then walks `src/` recursively.
  - `src/host_control_fair_write.rs:158` and `:163` pin exact source text in `src/daemon_transport.rs` and `src/local_webrtc.rs`.
  - `src/daemon_attach_stream.rs:1129` and `src/unix_terminal_adapter.rs:906` are self-scans through `include_str!`.
  - `src/daemon_transport.rs:6567` scans its own production text for `prefer_close_events`, `queue_unix_subscription_closed_events`, and `queue_webrtc_subscription_closed_events`.
  - `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` names `src/local_webrtc.rs` at lines 92 and 429, `src/daemon_transport.rs` at 441 and 738, `src/client_api.rs` at 450, and `src/daemon_attach_stream.rs` at 647.
- `test.sh` runs `node packages/hub-test-support/scripts/sync-assets.mjs --check` and then `BOTSTER_ENV=test cargo test --workspace "$@"`.
- `.github/workflows/ci.yml` pins Rust `1.97.0` and runs `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `./test.sh --locked`.

## Human Scope Decision

Three scope boundaries changed the diff by an order of magnitude, so this plan asked before choosing. Question `question_1787977279_295931` is `answered`. The recorded decision:

1. **Subscription families relocate whole.** `src/daemon_entity_subscriptions.rs` relocates to `src/subscription/entity.rs`. `src/daemon_event_subscriptions.rs` relocates to `src/subscription/package_events.rs`. Move-only commits come first. No forwarding module and no duplicate state owner may remain. If either source file holds a responsibility outside its subscription family, Implement must identify that exact responsibility before moving it elsewhere.
2. **Grant policy leaves `src/local_webrtc.rs` in this ticket.** `src/admission/grants.rs` owns grant issue, validation, origin policy, the grant registry, and session-key derivation from the admitted secret. WebRTC keeps accepted configuration, derived session keys, handshake mechanics, framing, sealing, and connection tasks. No grant secret and no origin policy may stay in the transport module. The extraction stays behavior-neutral.
3. **`src/admission/peer_generation.rs` is created now, from existing identity only.** It receives the existing peer-instance ownership identity and fencing. The current grant id, or peer-instance identity, is the existing ownership epoch. No new counter, wire field, or protocol behavior may be invented. Core-minted terminal subscription generations stay recorded values. `live_generation_for_route` and route binding transitions stay in `subscription/attach_routes.rs`. `released_attach_generations`, suppression sets, and close bookkeeping go to `subscription/closed_events.rs`. `fail_closed_pre_bind_attach` and replacement route ownership stay with `attach_routes`, unless a small peer-identity comparison helper genuinely belongs in `peer_generation.rs`. Label reservation stays deferred.

The answer also states that the large diff is intentional, because this ticket establishes one owner for each existing state machine, and that each relocation stays separate from import repair and from any necessary extraction.

## Official Baseline

Measured on this worktree at `fd540b6`, with `RUSTUP_TOOLCHAIN=1.97.0` exported and `CARGO_TARGET_DIR` unset, per [[Hub official gates must not set CARGO TARGET DIR]]. The worktree path contains no `:` character, so the colon-path rule does not apply.

| Gate | Result |
|---|---|
| `rustc --version` | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| `git rev-parse HEAD` | `fd540b6b21bdfe23f9280e13f650dff573fc5ae9` |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `cargo build --locked --bin botster-hub` | exit 0 |
| `./test.sh --locked` | exit 0, zero failures |
| `git status --porcelain` | empty |

The `./test.sh --locked` run was captured through `tail -60`, so this record keeps the exit code and the absence of any failure line rather than a total test count. Implement must record the full count from an untruncated run.

Tracked `.gitignore` is present and non-empty at HEAD, so no `git checkout HEAD -- .gitignore` restore is required.

## Scope

This ticket is move-only in behavior. It changes no wire format, no DTO, no serde name, no protocol version, no limit value, and no test name.

Commit kinds are stated as an invariant rather than a fixed count, per [[express scope limits as invariants not closed enumerations]]. **Every code commit on this branch is exactly one of four kinds, and no commit mixes two kinds:**

- **Relocation.** Exactly one `git mv` of one file to its owner path. The moved file's bytes do not change at all. The commit carries no other edit of any kind.
- **Import repair.** Every edit that a single relocation forces, and nothing else. Three families, all compile-only or lookup-only, none of them logic:
  1. Module declarations added or removed in `src/lib.rs`, `src/daemon.rs`, or `src/daemon_transport.rs`, including the `#[path = "..."]` attributes that currently mount two of the relocated files inside `daemon_transport`.
  2. `use` path rewrites, including the `use super::{...}` block whose meaning the move changed, plus the `pub(crate)` visibility widening the move forces.
  3. **Move-forced path references to the relocated file.** Compile-time literals such as `include_str!("...")`, and run-time path strings such as `hub_source("src/...")` or `fs::read_to_string(root.join("src/..."))`. The inventory below names all ten.

  The bright line between import repair and guard restore: **import repair may rewrite the path text of a reference that already exists; it may not add or remove a reference.** Adding a scan-list entry for a newly created file is guard restore.
- **Extraction.** One responsibility leaves its current file and lands in a named owner file, with its state, policy, and tests. No behavior changes.
- **Guard restore.** New entries added to source-scanning guard lists for files this ticket creates, and named-file assertions relocated for code that moved by extraction rather than by relocation.

Two Plan Review rounds shaped this section, and both findings were correct. Round 1 found the original three-kind list self-contradictory: a relocation cannot be byte-pure and also carry the module-path repair the recorded human decision requires to stay separate. Round 2 found that the repaired import-repair kind still excluded a compile repair the byte-pure move forces, because `src/daemon_attach_stream.rs:1129` scans itself through `include_str!("daemon_attach_stream.rs")`, and after the move that relative literal resolves to a missing `src/subscription/daemon_attach_stream.rs`.

**Measured inventory of move-forced path references.** Round 2 named one. `grep -rn "daemon_attach_stream\.rs\|daemon_entity_subscriptions\.rs\|daemon_event_subscriptions\.rs" src/ tests/ crates/ script/` at `fd540b6` returns ten, in seven files. Implement must repair every row, and check 8b proves none survives.

| # | Site | Reference | Fails at | Repair |
|---|---|---|---|---|
| 1 | `src/daemon_attach_stream.rs:1129` | `include_str!("daemon_attach_stream.rs")` self-scan inside `attach_stream_source_does_not_branch_on_snapshot_phases` | compile | `include_str!("attach_routes.rs")` |
| 2 | `src/lib.rs:1007` | `include_str!("daemon_entity_subscriptions.rs")` | compile | `include_str!("subscription/entity.rs")` |
| 3 | `src/lib.rs:1011` | `include_str!("daemon_attach_stream.rs")` | compile | `include_str!("subscription/attach_routes.rs")` |
| 4 | `src/daemon_transport.rs:147` | `#[path = "daemon_attach_stream.rs"]` | compile | removed with the module declaration |
| 5 | `src/daemon_transport.rs:166` | `#[path = "daemon_entity_subscriptions.rs"]` | compile | removed with the module declaration |
| 6 | `src/lib.rs:1006` | display string `"src/daemon_entity_subscriptions.rs"` | test message only | `"src/subscription/entity.rs"` |
| 7 | `src/lib.rs:1010` | display string `"src/daemon_attach_stream.rs"` | test message only | `"src/subscription/attach_routes.rs"` |
| 8 | `tests/session_projection_owner_loop.rs:176` and `:191` | `fs::read_to_string(root.join(relative))` list entry plus its exclusion comparison | test | `"src/subscription/entity.rs"` in both places |
| 9 | `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:647` | `hub_source("src/daemon_attach_stream.rs")` | test | `hub_source("src/subscription/attach_routes.rs")` |
| 10 | `tests/hub_daemon_lifecycle/event_plane_saturation.rs:126` and `:176` | `fs::read_to_string(root.join(...))` list entry and direct read | test | `"src/subscription/entity.rs"` and `"src/subscription/attach_routes.rs"` |

Rows 1 through 5 break `cargo build`. Rows 8 through 10 build fine and then panic inside a test, because each one calls `.expect(...)` on a read of a path that no longer exists. Rows 6 and 7 only degrade an assertion message. All ten are the same class of edit — a path that names the relocated file — so all ten belong to that relocation's import-repair commit. Only `src/daemon_event_subscriptions.rs` has no such reference; its relocation needs module-declaration and `use` repair alone.

**Pairing and greenness rule.** A byte-pure relocation cannot compile on its own, because the moved module is no longer declared where it was. Each relocation is therefore immediately followed by its own import-repair commit, with no other commit between them:

- The two commits form a contiguous pair, in the order relocation then import repair.
- The relocation commit is permitted to be non-green. It is the only commit kind on this branch with that permission, and the permission exists solely so the human-required separation stays visible in history.
- **At the second commit of every pair, both `cargo build --locked` and every test that names the relocated file must pass.** The path-reference inventory is what makes the second half of that obligation reachable: without rows 8 through 10 in the import-repair commit, the pair would build and then fail three named-file tests.
- Every extraction and guard-restore commit must be green on the same two obligations.
- Gates run at pair boundaries and at every non-relocation commit. They do not run inside a pair.

This keeps the recorded human rule ("keep each relocation separate from import repair and any necessary extraction") and keeps a bisect-usable history, because every commit except the first half of a pair is green.

### In scope

1. Mount `src/subscription/` as a crate-private module tree and relocate the three existing subscription-family files:
   - `src/daemon_attach_stream.rs` to `src/subscription/attach_routes.rs`.
   - `src/daemon_entity_subscriptions.rs` to `src/subscription/entity.rs`.
   - `src/daemon_event_subscriptions.rs` to `src/subscription/package_events.rs`.

   Two of these three are `#[path]` submodules of `daemon_transport` today. After the relocation they are crate-root-owned modules, which is the exact ownership reduction the ticket names.
2. Create `src/subscription/closed_events.rs` and give it one owner for the close-event state machine: the pending `TerminalSubscriptionClosed` queue, the suppression sets, the route generation bookkeeping, and the bounded slice classification. `UnixConnectionMux` and `WebRtcConnectionMux` keep their transport identity and delegate this state to the single ledger. `ClosedEventSliceProgress` moves here from `src/unix_terminal_adapter.rs`, which removes the current WebRTC-to-Unix type dependency. The owner-side entry points `run_close_events_phase`, `empty_close_event_progress`, `suppress_unix_session_close_events`, `suppress_webrtc_session_close_events`, `session_close_event_decision`, and `session_close_event_decision_for` move here from `daemon_transport`.
3. Move route ownership, occupancy, and their state to `src/subscription/attach_routes.rs`: `AttachedSubscription`, `AttachedSubscriptionChange`, `apply_attached_subscription_change`, `record_attached_subscription_change`, `attached_subscription_change_for_response`, `response_records_attach_ownership`, `overlay_live_attach_occupancy`, `live_attach_occupancy_rows`, `unix_eof_cleanup_ablation`, `UnixEofAblation`, and the `live_attach_routes` set. `released_attach_generations` moves to `subscription/closed_events.rs` per the recorded decision.
4. Create `src/admission/` and move Hello admission there:
   - `src/admission/unix_hello.rs` receives `unix_hello_admission`, `daemon_hello_ack`, `terminal_compatibility_attach_error`, `UnixTerminalAdmission`, `WebrtcTerminalAdmission`, `HostCompatibilityRecord`, the `unix_admissions`, `webrtc_admissions`, and `host_compatibility` registries now held by `PendingRuntimeState`, and `next_admission_key`.
   - `src/admission/budgets.rs` receives the Hub resource budget constants listed under Context Loaded and any admission decision that reads them.
5. Create `src/admission/grants.rs` and move grant issue, validation, origin policy, the grant registry, and session-key derivation out of `src/local_webrtc.rs`: `LocalWebrtcGrant`, `LocalWebrtcGrant::validate`, `GRANT_TTL_SECONDS`, `issue_bootstrap`, `prune_expired_grants`, the redeem decision inside `signal`, `random_secret_token`, `secret_stream_key`, and `origin_from_local_url` from `daemon_transport`.
6. Create `src/admission/peer_generation.rs` and move the existing peer-instance ownership identity and its comparison there. This is the grant-id identity that `AttachStream::owner_matches` and `LocalWebrtcPeerState.grant_id` already use. No counter and no wire field is added.
7. Split `PendingRuntimeState` so that its admission registries move to `admission/` and its `streams: AttachStreamRegistry` and `close_work` stay with the route and close owners. `daemon_transport` keeps only the composition that reaches those owners.
8. Move each test with the responsibility it proves, keeping every existing test name unchanged.
9. Extend every source-scanning guard list to name the new files, with one ablation per added entry per [[fixed source guard lists need one ablation per added file]].

### Not in scope

- No dedicated `DataChannel` and no change to terminal pumping. The ticket forbids both.
- No Core pin change. The ticket forbids it.
- No `src/admission/labels.rs`. Label reservation does not exist on main and belongs to the dedicated-channel ticket.
- No `src/transport/` tree, no `src/data_plane/`, and no deletion of `src/daemon_transport.rs`. Those are migration steps 4, 5, 6, and 8.
- No new peer-generation counter, wire field, or protocol behavior.
- No change to Core-minted terminal subscription generations, which stay recorded values.
- No public API addition and no public API removal. Every new module is `pub(crate)`.
- No behavior change of any kind. If any acceptance check shows a wire, DTO, or limit change, the ticket has left scope and must stop.

## Repository Ownership Boundaries And Cross-Repo Dependencies

- **botster-hub owns everything this ticket changes.** Admission, grants, key derivation, peer identity, route ownership, occupancy, close bookkeeping, and resource budgets are Hub host policy, per [[botster hub is a first party host profile over core]] and [[botster Hub Rust stays a trusted host kernel]].
- **botster-core is untouched.** Core keeps terminal subscription identity, attach phases, generations, and teardown, per [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]. This ticket records Core-minted generations and does not mint or reinterpret them. The Core pin does not move.
- **botster-hub-client is untouched.** The `Daemon*` DTOs live in `crates/botster-hub-client/src/lib.rs`. No mapper, field, serde name, or protocol version changes, so `crates/botster-hub-client/generated/daemon-protocol.ts` must stay byte-identical.
- **botster-web and botster-tui cost zero lines.** Per [[a ui contract import line change costs one test line in each generic client]], a client cost appears only when a UI contract import line changes. No such line changes here, and acceptance checks 6 through 9 prove it.
- **No cross-repository prerequisite exists**, so this plan registers no new dependency. The one dependency on record, `ticket_1787894414_324976`, is closed.
- Sibling decomposition tickets edit the same large source files. They must run in dependency order, per [[daemon transport extraction moves ownership before deleting the facade]]. This plan therefore requires the base re-verification in acceptance check 20 before Implement writes its first commit.

## Assumptions And Unknowns

1. **Assumption.** The three subscription-family relocations are `git mv` operations that keep the moved file byte-identical, with every compile-restoring edit deferred to the paired import-repair commit. Recorded decision 1 requires that separation. Risk if wrong: a reviewer reads a relocation as a rewrite. Acceptance checks 7 and 8a measure it.
2. **Assumption.** `src/daemon_entity_subscriptions.rs` and `src/daemon_event_subscriptions.rs` each hold exactly one subscription family. Evidence: `daemon_entity_subscriptions.rs` exports only `EntityFrameSender` and `EntitySubscriptionState` plus entity drive functions; `daemon_event_subscriptions.rs` exports only `ClientEventPlane`, `ClientEventMailbox`, subject compilation, and event responses. Implement must confirm this file-by-file before the relocation commit and record any responsibility that falls outside the family, per recorded decision 1.
3. **Assumption.** Relocating `daemon_entity_subscriptions.rs` out of `daemon_transport` forces rewriting its `use super::{DAEMON_MAX_FRAME_BYTES, DaemonControlState, HubDaemon, daemon_response_base, daemon_session_type_from_client, session_type_entity_snapshot}` block at line 24, because `super` changes meaning. `DAEMON_MAX_FRAME_BYTES` resolves to `crate::admission::budgets` after this ticket; the rest resolve to `crate::daemon_transport` and `crate::client_api_dto`. Some of those items are private today and must widen to `pub(crate)`. That widening is move-forced and adds no public API, exactly as decomposition 1 recorded for `pub(super)` mappers. This repair is an import-repair commit, paired with and immediately following its relocation.
4. **Assumption.** The close-event ledger can be unified behind one wake abstraction. Evidence: the measured 24-line diff between the two implementations. Implement must diff the two blocks again on the exact base it builds on, record which lines differ, and keep any genuinely divergent line in its own transport rather than forcing a false unification. `close_events_admitted` on the WebRTC mux is close-event Hello negotiation and stays with the WebRTC mux in this ticket; moving it is a transport-split concern.
5. **Assumption.** `admission/grants.rs` needs its own typed error rather than reusing `LocalWebrtcError`. `LocalWebrtcError` carries transport arms as well as the five grant arms `MissingGrant`, `RedeemedGrant`, `ExpiredGrant`, `SecretMismatch`, and `OriginMismatch`. The behavior-neutral seam is a `GrantAdmissionError` with exactly those five arms plus the random and key-derivation failures, and a `From` implementation in `local_webrtc.rs` that maps each arm to the identical existing `LocalWebrtcError` variant. Every `Display` string and every `DaemonResponse` mapping stays byte-identical. Acceptance check 13 proves it.
6. **Assumption.** `answer_offer` stops receiving `grant_secret`. After the extraction, admission validates and redeems the grant, derives the session key, and hands the transport an accepted-peer value that carries the `grant_id` and the derived `AesGcmKey`. `LocalWebrtcSignalRequest` keeps its current shape, because it is the inbound request that admission consumes, not a transport input. Acceptance check 14 proves the transport no longer names the secret.
7. **Unknown, resolved by Implement.** Whether `peer_generation.rs` should own the whole `AttachStreamOwner` and `owner_matches` pair, or only a small peer-identity comparison helper. The recorded decision leaves `fail_closed_pre_bind_attach` and replacement route ownership with `attach_routes` and permits only a small helper to move. Implement must choose the smaller of the two and state which.
8. **Unknown, resolved by Implement.** Whether `origin_from_local_url` can move to `admission/grants.rs` without dragging the surrounding `issue_local_webrtc_bootstrap_response` control dispatch. Dispatch stays in `daemon_transport` in this ticket; only origin derivation and origin policy move.
9. **Assumption.** No new public path appears. Every new module is `pub(crate)`. None of the moved items is reachable from outside the crate today, because `daemon_attach_stream`, `daemon_entity_subscriptions`, and `daemon_event_subscriptions` are private modules and `local_webrtc` is `pub(crate)`. The `pub const MAX_SUBJECTS_PER_SUBSCRIPTION` family inside `daemon_event_subscriptions.rs` is `pub` inside a private module and therefore has no public path today; it must not gain one. Acceptance check 16 proves both directions with a compiler probe.
10. **Assumption.** `./test.sh --locked` runs on a quiet host. Per the recorded Hub lifecycle sensitivity, a busy host can produce environment-dirty failures that do not attribute to the diff. A failure must be attributed by exact test name and rerun on a quiet window before anyone calls it unrelated.

## Affected Surfaces And Files

Created:

- `src/subscription.rs` and `src/subscription/{attach_routes.rs, entity.rs, package_events.rs, closed_events.rs}`.
- `src/admission.rs` and `src/admission/{unix_hello.rs, grants.rs, peer_generation.rs, budgets.rs}`.

Deleted by relocation:

- `src/daemon_attach_stream.rs`, `src/daemon_entity_subscriptions.rs`, `src/daemon_event_subscriptions.rs`.

Modified:

- `src/daemon_transport.rs` -- loses admission, route ownership, occupancy, close bookkeeping, budgets, and its three `#[path]` submodule declarations. Keeps connection handling, mux write scheduling, control dispatch, the owner loop, and the package and workspace control paths.
- `src/local_webrtc.rs` -- loses the grant registry, grant validation, origin policy, and key derivation. Keeps peer creation, signaling mechanics, framing, sealing, chunking, delivery, and bounded close.
- `src/unix_terminal_adapter.rs` and `src/webrtc_terminal_adapter.rs` -- delegate close bookkeeping to the single ledger and lose their duplicated copies.
- `src/lib.rs` -- adds `pub(crate) mod admission;` and `pub(crate) mod subscription;`, removes `mod daemon_event_subscriptions;`, and updates the `production_sources_reject_terminal_drain_and_snapshot_phase_decode` `include_str!` list.
- `src/host_control_fair_write.rs` -- only if a moved line changes one of its two pinned source strings. It names `src/daemon_transport.rs` and `src/local_webrtc.rs`, neither of which relocates, so no path repair applies to it.
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` -- row 9 of the path-reference inventory, plus guard file names that follow extracted code.
- `tests/session_projection_owner_loop.rs` -- row 8 of the path-reference inventory. Two sites: the scan list entry and the exclusion comparison that names the same path.
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs` -- row 10 of the path-reference inventory. Two sites: a scan list entry and a direct read.
- `docs/plans/hub-decomposition-2-extract-admission-and-subscription-ownership.md` -- this plan, in documentation commits that touch no code.

Not modified, and a change in any of them is a scope error:

- `crates/botster-hub-client/` and its generated TypeScript artifact.
- `packages/hub-test-support/`.
- `Cargo.toml`, `Cargo.lock`, and every Core pin literal.
- `src/main.rs`, unless a moved import forces one `use` line.

## Risks

1. **Blind guards.** This is the highest risk, and decomposition 1 already realized it once in commit `468bf7f`. Moving admission and close bookkeeping out of `daemon_transport.rs` and `local_webrtc.rs` can leave `production_sources_reject_terminal_drain_and_snapshot_phase_decode` and the `hub_source()` guards green while they no longer scan the moved code. Mitigation: acceptance checks 17 and 18, with one red ablation per added list entry.
2. **Self-scan drift.** `src/daemon_transport.rs:6567` scans its own production text for close-event constructs, and `src/host_control_fair_write.rs` pins exact strings in two files. Moving code can make such an assertion trivially true. Mitigation: acceptance check 19 relocates each assertion to the file that now holds the protected text and proves it can still fail.

2a. **Move-forced path references, which break the build or the suite rather than degrading silently.** This is the failure mode Plan Review round 2 found, and the measured inventory shows ten references in seven files rather than the one the finding named. Five break `cargo build`; three panic inside a named test through `.expect(...)` on a read of a vanished path; two only degrade an assertion message. The risk is not that they go unnoticed, because most are loud. The risk is that a plan which routes them to the wrong commit kind makes the pairing rule unsatisfiable, which is exactly what the round-1 and round-2 wordings did. Mitigation: the inventory table assigns all ten to the relocation's own import-repair commit, check 8b proves zero survive with a recorded starting count, and check 8a requires the named-file tests to pass at the pair boundary rather than only `cargo build`.
3. **Silent behavior change inside the grant extraction.** Grant validation order matters: `redeemed`, then expiry, then secret, then origin. A reordering changes which typed error a client sees. Mitigation: acceptance check 13 asserts the arm order and the identical error text.
4. **Secret leakage into the transport path.** The current `answer_offer` takes the grant secret. A partial extraction that leaves the secret on the peer path fails the ticket acceptance line. Mitigation: acceptance check 14 is a source assertion plus the compiler.
5. **False unification of the close-event ledger.** The two implementations are near-identical but not identical. Forcing one code path over a real difference changes wake behavior. Mitigation: assumption 4 requires a recorded diff before unification, and acceptance check 12 requires the existing per-transport close tests to stay green unmodified.
6. **Ownership split that creates two owners.** Splitting `PendingRuntimeState` can leave admission state reachable from two places. Mitigation: acceptance check 10 requires zero definitions of the moved items in the source file and forbids a forwarding wrapper.
7. **Public path drift.** Moving a `pub` item between modules can add or delete a public path without a compiler error. Mitigation: acceptance check 16 uses a temporary external-crate probe with a red ablation, the same design decomposition 1 validated.
8. **Base drift from sibling tickets.** Other decomposition tickets edit the same files. Mitigation: acceptance check 20 re-verifies the base against `origin/main` immediately before Implement writes its first commit, and the review is renewed after any semantic rebase.
9. **Suite noise attributed to the diff.** Mitigation: assumption 10 and acceptance check 22.

## Acceptance Checks And Tests

Ownership proof, which is the real acceptance test:

1. `src/daemon_transport.rs` contains zero **definitions** of `unix_hello_admission`, `daemon_hello_ack`, `terminal_compatibility_attach_error`, `UnixTerminalAdmission`, `WebrtcTerminalAdmission`, `HostCompatibilityRecord`, `next_admission_key`, `suppress_unix_session_close_events`, `suppress_webrtc_session_close_events`, `session_close_event_decision`, `session_close_event_decision_for`, `run_close_events_phase`, `empty_close_event_progress`, `AttachedSubscription`, `AttachedSubscriptionChange`, `apply_attached_subscription_change`, `record_attached_subscription_change`, `attached_subscription_change_for_response`, `response_records_attach_ownership`, `overlay_live_attach_occupancy`, `live_attach_occupancy_rows`, `unix_eof_cleanup_ablation`, `UnixEofAblation`, `origin_from_local_url`, and every budget constant listed under Context Loaded. A `use` or `pub use` line naming one of these is not a definition and is permitted.
2. `src/local_webrtc.rs` contains zero definitions of `LocalWebrtcGrant`, `GRANT_TTL_SECONDS`, `prune_expired_grants`, `random_secret_token`, and `secret_stream_key`, and zero occurrences of `grant_secret` outside the inbound `LocalWebrtcSignalRequest` field and the call that hands the request to admission.
3. No function in `src/daemon_transport.rs` or `src/local_webrtc.rs` forwards to a moved item while retaining its body. `grep -nE "^(pub )?(fn|impl|enum|struct|type) "` in both files shows no definition of any moved item.
4. `src/daemon_transport.rs` declares no `#[path]` submodule for attach routes or entity subscriptions. `src/lib.rs` declares `pub(crate) mod admission;` and `pub(crate) mod subscription;` and no longer declares `mod daemon_event_subscriptions;`.
5. Every moved test lives in the module that owns the responsibility it proves, and every test name is unchanged. The admission tests are `admission_cursor_uses_exclusive_range_not_a_prefix_scan` and `register_unix_admission_acks_before_request_loop`. The close-event tests are `shutdown_session_arm_installs_exact_suppression_before_core_request`, `close_event_suppression_matrix_matches_prior_predicate`, `close_events_phase_source_does_not_take_journal_wake`, `close_event_slice_uses_keyed_suppression_without_cloning_the_prefix`, `exact_generation_suppression_silences_running_close_and_preserves_later_generation`, and `empty_session_snapshot_installs_no_suppression_keys`. The route-ownership tests are `drain_does_not_inspect_legacy_attach_state_for_ownership`, `drain_does_not_change_attach_occupancy`, `occupancy_rows_union_hub_routes_and_core_inventory`, `independent_counter_sub_does_not_clear_named_occupancy`, `client_eof_detaches_connection_subscriptions`, and `attach_operator_error_does_not_detach_on_client_eof`. The grant test is `issuing_bootstrap_prunes_expired_grants_and_keeps_live_replay_diagnostics`.

Move-only proof:

6. `git show --color-moved=dimmed-zebra <commit>` renders each extraction as moved lines. Record the command and the reviewer instruction in each commit message.
7. Every relocation commit shows `similarity index 100%` under `git show --stat -M --summary`, and `git show --numstat -M` reports zero added and zero deleted lines for that commit. A relocation that reports any changed line has absorbed import repair and must be split.
8. Every code commit is exactly one of the four kinds, and no commit mixes two kinds. Prove it per commit: a relocation touches exactly one path pair and zero lines; an import-repair commit changes only module declarations and `#[path]` attributes, `use` lines, `pub(crate)` visibility, and the path text of references that already name the relocated file, and it adds no reference and removes no reference and changes no assertion body; an extraction moves one named responsibility; a guard restore only adds scan-list entries for files this ticket creates, or relocates named-file assertions for extracted code.
8a. Every relocation is immediately followed by its own import-repair commit, with no commit between them. Prove the ordering with `git log --oneline --reverse`. At the second commit of each pair, prove both obligations: `cargo build --locked` succeeds, and every test that names the relocated file passes. The relocation commit itself is the only commit permitted to fail either obligation.
8b. No stale path reference to a relocated file survives. After each import-repair commit, `grep -rn "daemon_attach_stream\.rs\|daemon_entity_subscriptions\.rs\|daemon_event_subscriptions\.rs" src/ tests/ crates/ script/` returns zero hits for the file that commit's pair relocated. After the last pair it returns zero hits in total. Run the same grep at the base first and record the ten-row starting inventory, so the check measures a real decrease rather than an empty search. Each of the three named-file test guards in rows 8 through 10 must be executed by name, not merely compiled: `cargo test --locked --test session_projection_owner_loop owner_loop_and_projection_sources_reject_unbounded_and_product_policy`, plus the `event_plane_saturation` and `subscription_ownership_baseline` guards that read those paths.

Client-contract oracle, authoritative and unchanged from decomposition 1:

9. `RUSTUP_TOOLCHAIN=1.97.0 cargo test -p botster-hub-client --locked` passes, including `generated_typescript_protocol_matches_checked_artifact`.
10. `git diff --exit-code -- crates/botster-hub-client/generated/daemon-protocol.ts` reports no change. Byte identity, not equivalence.
11. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passes, and `git status --porcelain -- crates/ packages/` is empty after the full test run.

Behavior-neutrality proof for the two extractions that are not pure moves:

12. The close-event ledger keeps both transports green with unmodified test bodies. Run the per-transport close lanes directly: `BOTSTER_ENV=test cargo test --locked --lib -- unix_terminal_adapter webrtc_terminal_adapter` plus the named tests in check 5. Implement records the measured diff between the two pre-move implementations, and every line it does not unify.
13. Grant validation order and error text are unchanged. Assert that `GrantAdmissionError` maps to `LocalWebrtcError::RedeemedGrant`, `ExpiredGrant`, `SecretMismatch`, `OriginMismatch`, and `MissingGrant` with byte-identical `Display` output, and that the validation arms still run in the order redeemed, expiry, secret, origin. Ablation: swap the secret and origin arms and show the corresponding test goes red.
14. The transport no longer receives the grant secret. `answer_offer` and every function it calls take the derived session key, not the secret. Prove it with the compiler by removing `grant_secret` from the accepted-peer value, and with a source assertion that `src/local_webrtc.rs` names `grant_secret` only on the inbound request path.

Public surface proof, both directions:

15. `grep -nE "^pub (fn|enum|struct|type|const) " src/admission/*.rs src/subscription/*.rs` returns only items that were `pub` inside a private module before the move, and every new module is declared `pub(crate)`.
16. External-crate compile probe, created, run, and deleted, never committed. Create `tests/public_path_probe.rs` naming every `botster_hub::` path that resolves today and is touched by this ticket, run `RUSTUP_TOOLCHAIN=1.97.0 cargo test --locked --test public_path_probe` at the base and again after the move, then delete it and confirm `git status --porcelain` is empty. A file under `tests/` compiles as its own crate against the workspace lockfile, which is why an out-of-repo crate is not a valid oracle here. Include one red ablation naming a path that does not exist, so the probe is not vacuous.

Guard coverage proof:

17. After the move, `production_sources_reject_terminal_drain_and_snapshot_phase_decode` names every new file under `src/admission/` and `src/subscription/`, and the entries for the three relocated files point at their new paths. Ablation, per [[fixed source guard lists need one ablation per added file]]: add a forbidden construct to each newly listed file in turn; each run must fail and name that exact file. Add the same construct to a still-listed file as the scanner-liveness arm. Remove every ablation before commit.
18. `grep -rn "hub_source(" tests/` and `grep -rn "include_str!" src/` enumerate every fixed source-scan list. Each scanned string still lives in the file its guard names. A guard whose list still names only the pre-move file is a coverage hole, not a green proof.
19. Each in-file self-scan follows its protected text. The `daemon_transport` self-scan for `prefer_close_events`, `queue_unix_subscription_closed_events`, and `queue_webrtc_subscription_closed_events` moves to `src/subscription/closed_events.rs` and keeps a red arm. The two `src/host_control_fair_write.rs` pinned strings are re-measured against their post-move files.

Strict Rust gates and the repository wrapper, each with `RUSTUP_TOOLCHAIN=1.97.0` and `rustc --version` recorded from the same shell:

20. Re-verify the base before Implement writes its first commit. Compare the worktree base against `origin/main`. If `origin/main` has advanced, rebase, rerun checks 21 through 23, and renew the review.
21. `cargo fmt --all -- --check`.
22. `cargo clippy --workspace --all-targets --locked -- -D warnings`, rerun in full after every repair, per [[strict clippy can hide later crate diagnostics behind the first compile failure]].
23. `unset CARGO_TARGET_DIR`, then `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` into the default worktree `target/`, then `./test.sh --locked` on a quiet host. Any failure is attributed by exact test name before anyone calls it unrelated.

Downstream proof:

24. Downstream client cost is asserted at zero and proved by checks 9 through 11. Because no DTO field, serde name, or protocol version changes, `botster-web` and `botster-tui` need no edit. If check 10 shows any diff, the ticket has left move-only scope and must stop.

Provenance:

25. Record the exact verified commit SHA, `git status --porcelain` output showing a clean tracked worktree, and `rustc --version`. Renew review after any semantic rebase. Preserve unrelated changes and merge directly to `main`, per the ticket.

## Runtime-Teardown Class

The class applies. This ticket moves peer ownership identity, route ownership, occupancy, close bookkeeping, and suppression. Every slice is behavior-neutral, so each answer states the invariant that must survive the move rather than a new design.

- `teardown_class_applies`: **yes.** The ticket moves the `PeerClosed` and Unix EOF occupancy path, the exact-generation suppression path, and the peer ownership identity used for replacement-owner protection.
- `teardown_isolation`: **unchanged.** One peer's ownership set stays the grant-scoped set that `PeerRemoveResult` returns: its peer connection, its peer state, its attached subscriptions, and its entity subscription ids. Healthy siblings survive a successful close. The only sibling sacrifice on record is the ultimate local close failure on the dedicated runtime, per [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]], and this ticket does not touch that path. Proof: the existing WebRTC close and sibling tests stay green with unmodified bodies.
- `teardown_bounds`: **unchanged.** `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and `LOCAL_WEBRTC_PEER_CLOSE_HANDLER_JOIN_DEADLINE` stay in `src/local_webrtc.rs` with their current values. No `block_on`, timeout, or join deadline moves, and the grant extraction adds no wait: grant validation is a synchronous map lookup and key derivation is a synchronous decode. Proof: `git diff` shows no change to either constant and no new await on the close path.
- `late_message_matrix`: every message type that creates durable ownership, its owner tag, its post-failure rejection, its `PeerClosed` sweep, and whether this ticket moves the owning code.

| Message | Durable row and owner after this ticket | Owner tag | Rejection after terminal failure | Sweep on `PeerClosed` race | Moves in this ticket |
|---|---|---|---|---|---|
| `Attach` | `AttachStreamRegistry.streams`, `active_subscriptions`, `attach_owner_grant_ids`, `connection_bound_routes` in `src/subscription/attach_routes.rs` | `AttachStreamOwner.grant_id` for WebRTC; the connection id for Unix | Pre-READY attach failure creates no route and increments no lifecycle count | Route-aware idempotent cleanup keyed on route identity; cannot decrement another route | **Relocation plus its own import repair.** The file moves byte-pure; the paired commit repairs only module declarations, `use` lines, and move-forced `pub(crate)` visibility. No line of admission or cleanup logic changes |
| `Detach` | Same registry, removal path | Same route identity | Detach failure cleanup stays route-aware | Shares the live attach route set | Relocation plus its own import repair |
| `SubscribeEntities` / `UnsubscribeEntities` | Entity subscription rows in `src/subscription/entity.rs` | `owner_grant_id` | Typed operator error without dropping transport | Owner-scoped removal on peer close | Relocation plus its own import repair |
| `SubscribeEvents` / `UnsubscribeEvents` | Connection-scoped holders and bounded mailboxes in `src/subscription/package_events.rs` | Connection-scoped holder identity, per [[Client event holders are connection-scoped]] | Bounded shed and typed rejection; at most `MAX_SUBSCRIPTIONS_PER_CONNECTION` per connection | Connection-scoped unsubscribe and cleanup | Relocation plus its own import repair |
| `Spawn` / `SpawnSessionType` | Session ownership in Core, reached through `HubRuntime` | Core session id | Typed operator error; no Hub row on failure | Core-owned; survives peer close | No |
| Unix `Hello` terminal admission | `unix_admissions` and `host_compatibility` registries, moving to `src/admission/unix_hello.rs` | Connection id | Terminal admission can be rejected while host operations stay available, per [[Unix Hello can reject terminal admission while host operations remain available]] | EOF cleanup uses the live attach route set | **Yes.** The registry and the admission decision move together |
| WebRTC `Hello` terminal admission | `webrtc_admissions` registry, moving to `src/admission/unix_hello.rs` | `grant_id` | Encrypted admission required before bind, per [[WebRTC terminal admission requires an encrypted DataChannel Hello]] | `PeerClosed` occupancy uses the live attach route set | **Yes.** Registry and decision move together |
| WebRTC signaling grant redemption | Grant registry, moving to `src/admission/grants.rs` | `grant_id` plus the expected origin | Missing, redeemed, expired, secret-mismatch, and origin-mismatch stay five distinct typed arms | A redeemed grant cannot be replayed; `stop_all` clears the registry | **Yes.** Registry, validation, origin policy, and key derivation move |
| `ShutdownSession` | Creates no durable ownership; it removes ownership. Suppression state moves to `src/subscription/closed_events.rs` | Exact `(session_id, subscription_id, generation)` | Typed `Absent` and `Err` arms stay distinct, per [[host ShutdownSession classification must call the exact-session Core query]] | Exact-generation suppression installs before Core teardown | **Suppression and close bookkeeping only.** Classification stays in `src/daemon/shutdown.rs` where decomposition 1 put it |

- `production_path_proof`: the production paths do not change; only their owning modules do.
  - Admission: a client `Hello` arrives on the Unix listener or the WebRTC control channel, reaches `unix_hello_admission`, and writes the admission registry. After the move the same dispatch calls `crate::admission::unix_hello::unix_hello_admission`. Live oracle: `register_unix_admission_acks_before_request_loop` plus the `hub_daemon_lifecycle` Hello tests that drive a real socket.
  - Grants: `issue_local_webrtc_bootstrap_response` mints a grant, the browser signals, and `LocalWebrtcTransport::signal` redeems it. After the move `signal` calls admission and receives the accepted peer and derived key. Live oracle: the WebRTC lifecycle tests that open a real peer from a real bootstrap, which fail closed if redemption or key derivation changes.
  - Suppression: `DaemonRequest::ShutdownSession` installs exact-generation suppression before the Core teardown request. Live oracle: `shutdown_suppresses_exact_route_generations_before_core_teardown`, `attached_stopping_shutdown_session_suppresses_exact_generation`, and `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`, run directly with `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test`. Per [[ShutdownSession suppression live tests are not a red oracle]], the load-bearing ordering proof stays the deterministic unit lane `shutdown_session_arm_installs_exact_suppression_before_core_request`, which moves with the suppression code and keeps its name.
  - Occupancy: Unix EOF and `PeerClosed` both reduce occupancy through the live attach route set. Live oracle: `occupancy_rows_union_hub_routes_and_core_inventory`, `independent_counter_sub_does_not_clear_named_occupancy`, and `client_eof_detaches_connection_subscriptions`.
  - This ticket is not scaffold-only. Every moved item keeps its existing production caller, and acceptance checks 1 through 3 forbid a forwarding wrapper that would leave the new module unwired.
- `ownership_identity`: the peer owner id stays the `grant_id`. `src/admission/peer_generation.rs` receives that existing identity and its comparison; it mints no new epoch. Delayed `PeerClosed` snapshots must still not delete rows owned by a different live peer, which is the invariant `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings` (`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:1900`), `local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` (`src/local_webrtc.rs:7127`), `local_webrtc_late_unsubscribe_does_not_delete_replacement_owner_row` (`src/local_webrtc.rs:6914`), and `stale_generation_close_does_not_sweep_replacement_owner` (`tests/hub_daemon_lifecycle/unix_terminal_adapter.rs:2500`) protect. All four stay green with unmodified bodies. Core-minted terminal subscription generations stay recorded values and are never re-derived by Hub.
- `sibling_fail_closed_policy`: unchanged in both directions. On a successful close, siblings keep working. On ultimate local close failure, the recorded bounded sibling sacrifice on the dedicated runtime stays as it is. No blast radius widens, because no close, forget, or sweep path changes shape.

## Vault Gaps Worth Capturing

1. A note that a Hub decomposition slice which relocates a `#[path]` submodule out of `daemon_transport` reduces ownership by module parentage alone, and that this is a distinct proof from moving function bodies.
2. A note that the Hub close-event state machine existed twice, in `UnixMuxInner` and `WebRtcMuxInner`, differing only in the wake call and one WebRTC-only method, and that `subscription/closed_events.rs` is now its single owner.
3. A note that Hub grant policy and session-key derivation belong to `admission/grants.rs`, and that WebRTC transport code receives an accepted peer configuration and a derived key rather than a grant secret.
4. A note that peer ownership identity in Hub is the grant id, that no peer-generation counter exists, and that `admission/peer_generation.rs` records the existing identity rather than minting an epoch.
5. A note that a Hub extraction which unifies two near-identical implementations must record the measured diff before unifying, so a real behavioral difference cannot disappear into a false shared path.
6. A note that a relocated module's `use super::{...}` block changes meaning and must be rewritten with the exact reach it had before, which is the module-level companion to the `pub(super)` rule decomposition 1 captured.
7. A note that a byte-pure relocation of a declared Rust module cannot compile alone, so a decomposition that keeps relocation separate from import repair must pair the two commits and state which one may be non-green. A commit-kind invariant that omits import repair is unsatisfiable, which two Plan Review rounds proved on this ticket.
8. A note that a Hub relocation must enumerate move-forced path references before it moves, because Hub names its own source files in three distinct ways: compile-time `include_str!` literals, `#[path]` module attributes, and run-time path strings read through `hub_source()` or `fs::read_to_string(root.join(...))`. This ticket measured ten such references across seven files for three relocated files. The companion rule to [[hub moves must extend source scanning guard file lists]] is that a guard can also break loudly rather than go blind, and the two failure modes need separate inventories.
