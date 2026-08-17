# Implement report: fix flaky exact-bytes suite-load ShutdownSession OperatorError

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786977409_499180` |
| Run | `run_1786977413_341616` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786977409_499180` |
| Plan | `docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md` @ `5dff948`, with Implement binding notes for Scope item 6 |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown (`teardown_class_applies: yes` — ShutdownSession classification raced against worker exit observation) |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |

Independent routing: `project_pipelines_current_context` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan uses the same target. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[botster hub is a first party host profile over core]]
- [[botster runtime teardown lenses]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[hub shutdown preserves durable session workers]]
- [[test script required for rust tests not cargo test]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Follow the approved plan. Keep production classify path and budgets unchanged at default configuration.
- The only permitted production-file edit is the env-gated `BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_FOR` hook in `core_daemon_config`.
- For `ShutdownSession`, prove exact-session `Found`, `Absent`, and `Err`. Reject Drain, baseline, or capped-page classification.
- Implement every runtime-teardown lens. Do not drop a lens to informal follow-up.
- Use `./test.sh`. Do not use bare `cargo test`.
- Direct merge. Do not create a pull request.
- Do not absorb `ticket_1786938984_190098`, `ticket_1786937228_425608`, or `ticket_1786913892_208903`.

## Files changed

Feature behavior:

- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` — observed-exit wait (10 s of 50 ms `ListSessions` polls), sharpened `SessionCleanup{already_exited}` assert with full error-body diagnosis, Absent-leg `unknown_session` probe, rewritten oracle comment.
- `tests/hub_daemon_lifecycle/sessions.rs` — live `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`.
- `tests/hub_daemon_lifecycle/cli.rs` — `start_cli_daemon_with_runtime_drain_failure`.
- `src/runtime.rs` — env-gated `with_test_fail_runtime_drain_for` / message plumbing in `core_daemon_config`. Inert unless the env var is set.

Handoff:

- `docs/plans/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load.md` — Implement binding for the compound Scope item 6 construction; Review-return correction of `sibling_fail_closed_policy` and the byte-identical claim.
- `docs/reports/fix-flaky-webrtc-exact-bytes-shutdown-classification-under-lifecycle-suite-load-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns the daemon control plane, `ShutdownSession` classification, and these lifecycle tests. Default-configuration production classify, recover, miss, and budget paths are unchanged. Core, hub-client, Web, TUI, hub-test-support, packages, pins, and lockfiles were not edited.

## Cross-repo routing

No Core dependency ticket. Construction (c) did not apply because existing Hub and Core test surfaces produced a live `OperatorError` when combined.

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786938984_190098` | ready_spawn budget flake | Parent Implement; depends on this ticket. Not absorbed. |
| `ticket_1786937228_425608` | unix_adapter lifecycle failure | Open; not absorbed. |
| `ticket_1786913892_208903` | write-budget sibling | Open; not absorbed. |

## Deviations from plan

Scope item 6 construction order was followed, then a compound of the two named surfaces was bound:

1. Construction (a) SIGKILL-then-shutdown returned `SessionCleanup{already_exited}`. Core observed `ProcessExited` before the live shutdown path ran.
2. Construction (b) drain injection plus `BOTSTER_HUB_TEST_WORKER_EGRESS_CAPACITY=1` returned `Events`. Core shutdown completed inside the 2 s window.
3. Compound of (a) plus the drain-injection half of (b): classify hits `BOTSTER_HUB_TEST_FAIL_RUNTIME_DRAIN_FOR` and falls through; SIGKILL then makes the live Core shutdown return `OperatorError{code=runtime_error, operation=shutdown}`. The same `DaemonConnection` then serves `Status`, sibling `SendInput` -> `Events`, sibling attached `ReadScreen` (`echo:ping`), and sibling `ListSessions`. 4/4 isolated runs passed on the first binding; Review return re-ran the rewritten connection-reuse oracle.

The committed plan now records this binding. No Core `with_test_fail_shutdown_for` ticket. The observed-exit wait was inlined; `session_fixtures.rs` was not extracted.

## Review return (`review_1786985857_694858`)

Review submitted `changes_required`. This visit keeps the production Core-error branch that closes victim-session adapters (`src/daemon_transport.rs:3430`). It does not keep adapters open on that path.

1. `finding_1786985857_441627` (high, product): plan `sibling_fail_closed_policy` cited `src/daemon_transport.rs:3406-3408` (Cleanup keep-open) as the Core-error authority. The live test also used one-shot `botster_hub_client::request`, which opens a new Unix connection and cannot prove connection or adapter survival. Fix: state the two-branch policy exactly; reuse one `DaemonConnection` for `ShutdownSession`, `Status`, sibling `SendInput`, sibling terminal envelopes, and `ListSessions`; `Attach` the sibling before the failing shutdown.
2. `finding_1786985857_920982` (medium, process): plan scope intro claimed compiled production code stays byte-identical. That is false because `src/runtime.rs` adds the env-gated drain hook. Fix: replace the claim with default-configuration behavior and budgets unchanged, plus one inert-unless-set test hook.

## Review return (`review_1786986483_508459`)

Review submitted `changes_required` again. `ReadScreen` is a direct host read and does not prove the Attach adapter.

1. `finding_1786986483_906414` (high, product): after the victim `OperatorError`, send a unique sibling marker and assert a terminal envelope for the sibling session and subscription through `connection.take_skipped_terminal`. Drain pre-failure frames first. Keep `ReadScreen` as session-health only. Add a `Detach` ablation that drops occupancy while `ReadScreen` still shows the post-failure marker.

## Projection-source verification

`ListSessions` reads `HubRuntime::list_sessions()` -> `core_daemon.list()` registry rows. `daemon_session_to_core_session` maps `RegistrySessionState::Exited` to `SessionLifecycleState::Exited`, and `lifecycle_label` prints `"exited"`.

`classify_shutdown_session` reads `observe_session_lifecycle` and treats `complete_registry` (`registry_state` is `Exited` or `Stale`) or `complete_lifecycle` (engine `Exited` or `Failed`) as `Cleanup`. `ListSessions` `"exited"` means registry `Exited`, which is `complete_registry`, so the wait predicate implies `SessionCleanup{already_exited}`. The wait uses the same store classify reads for the registry half. `observe_session_lifecycle` can be ahead of `list()`; waiting for `list()` is the conservative direction.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | `ShutdownSession` still targets one `session_id` through the exact-session query. The new failure test uses one isolated hub, one victim, and one sibling. |
| Bounds | Production Core 2 s shutdown deadline, worker 500 ms grace, and Hub observe turn are unchanged. The oracle wait is a 10 s client-side poll that fails with the last lifecycle value. |
| Late-message matrix | No new ownership-creating message. Binding stale-peer lib filter (7 passed) keeps the production-handler rejection and sweep proofs. |
| Production-path proof | The repaired oracle still drives live WebRTC output, owner-loop `exited`, then production `ShutdownSession` -> `classify_shutdown_session` -> `SessionCleanup{already_exited}`. Control A forces `Active` and the test fails with `kind=Events`. The sibling-survival test drives a live `OperatorError` through the production handler. |
| Ownership identity | Sessions stay keyed by exact `session_id`. The Absent probe uses a never-spawned id. |
| Sibling / fail-closed | Live proof on one reused unix-adapter `DaemonConnection`: victim Core-error `OperatorError` closes victim adapters (`:3430`) and leaves Status occupancy, sibling `SendInput`, sibling `take_skipped_terminal` envelopes, and sibling listing intact. `ReadScreen` is session-health only. A later `Detach` ablation drops occupancy and the envelope oracle. |

No lens was dropped to informal follow-up.

## Tests and downstream proof run

Tracked `.gitignore` is present and non-empty. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 (cached) |
| Pre-change probe 1: `./test.sh --locked --test hub_daemon_lifecycle_test` | 219 passed, 0 failed, 1 ignored in 330.09s. Ticket test passed. Flake did not reproduce. Corroborating only; Plan Review already reproduced 217/2/1. Probe 2 skipped after non-reproduction. |
| Construction (a) isolated | exit 101; `kind=SessionCleanup` `outcome=already_exited` |
| Construction (b) isolated | exit 101; `kind=Events` |
| Compound construction isolated | 4/4 pass, including the first green run |
| Check 3: 10× repaired exact-bytes | 10 pass, 0 fail |
| Check 5 Found: `shutdown_after_observed_exit_returns_session_cleanup` | 1 passed |
| Check 5 Found: `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` | 1 passed |
| Check 5 Err: seven named `--lib` unit tests | 7 passed |
| Check 6 stale-peer: seven named `--lib` tests | 7 passed |
| Check 6 live failure | 1 passed (compound construction) |
| Review-return live failure (one `DaemonConnection` + sibling Attach/`ReadScreen`) | 1 passed in 50.52s |
| Review-return 2 live failure (`take_skipped_terminal` + occupancy + Detach ablation) | 1 passed in 40.77s |
| Review-return `cargo fmt --all -- --check` | covered by `cargo fmt --all` before the live test |
| Review-return `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| Control A: force `classify_shutdown_session` -> `Active` | exit 101; `got kind=Events error=None` |
| Control B: wait on `webrtc-exact-bytes-nonexistent` | exit 101; `last=None` after the 10 s bound |
| Ablation revert + green re-run | exit 0 |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| Check 4 suite run 1 | 219 passed, 1 failed, 1 ignored in 373.00s. Ticket test passed. Sibling failure test passed. Only failure: `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` panicked `ready spawn waited 68.653042ms` at `tests/hub_daemon_lifecycle/sessions.rs:3722`. Attributed to owner `ticket_1786938984_190098`. Counts as passing for this ticket. |
| Check 4 suite run 2 | 220 passed, 0 failed, 1 ignored in 372.19s. Ticket test passed. Sibling failure test passed. ready_spawn passed. |
| Check 4 suite run 3 | 219 passed, 1 failed, 1 ignored in 363.82s. Ticket test passed. Sibling failure test passed. Only failure: `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` panicked `ready spawn waited 74.0545ms` at `tests/hub_daemon_lifecycle/sessions.rs:3722`. Attributed to owner `ticket_1786938984_190098`. Counts as passing for this ticket. |

`-- --test-threads=1` was not used as a suite command.

Production entry point already using the behavior: Unix `ShutdownSession` -> `classify_shutdown_session` -> `HubRuntime::observe_session_lifecycle`. This ticket does not add a production branch. It makes the suite-load oracle wait for observed exit and trip if classify regresses to `Active`.

Downstream consumer proof: not required. No public surface, DTO, pin, or compiled default-configuration runtime behavior changed.

## Unverified behavior or residual risk

- Pre-change suite probe 1 did not reproduce the original flake. Plan Review already reproduced it. The repair removes the blind call, so the recorded class cannot recur on this oracle.
- The compound failure test depends on SIGKILL plus an env-gated observe-drain injection. A later Core change that turns a dead-worker shutdown into cleanup even after classify `Err` would fail this test and need a new ticket.
- `ready_spawn_stays_within_budget_when_live_sessions_exceed_one_observe_slice` remains owned by `ticket_1786938984_190098`. Check 4 attribution: run 1 waited 68.653042ms; run 3 waited 74.0545ms; run 2 passed.
- Full workspace `./test.sh --locked` is non-binding.

## Missing vault guidance discovered

[[host ShutdownSession classification must call the exact-session Core query]] still says the convention is not shipped. Hub main ships `classify_shutdown_session` over `observe_session_lifecycle`. That is a stale shipped-status on an existing note, not a new capture.

Captured after Implement confirmed the repair:

- inbox `suite-load-oracles-must-not-exceed-the-same-file-host-contract.md`
- inbox `typed-response-flake-oracles-must-print-the-error-body.md`

No convention conflict. Hub charter, runtime-teardown lenses, and the approved plan agree: repair the suite-load oracle here; keep production budgets; bind a live ShutdownSession failure without a weakened helper-only substitute.

`project_pipelines_create_vault_checklist` and `project_pipelines_create_checklist` timed out on the plugin worker during this step. Vault workflow evidence is recorded in this report instead.
