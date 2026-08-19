# Implement report: Hub tests replace PTY process PID and marker lifecycle oracles

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786912572_610381` |
| Run | `run_1787136288_939918` |
| Step | `botster_stack_implement` (`run_step_1787143222_829808`) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786912572_610381` |
| Plan | `docs/plans/replace-pty-process-pid-and-marker-lifecycle-oracles.md` revision 5 |
| Delivery | `merge_policy: direct`; no pull request |
| Class | runtime-teardown, narrowly: terminal-state markers versus live-runtime session completion. Test-only unique diff. Production close-event ordering was `ticket_1787143511_231816` and is now closed. |
| Locked Core | `Cargo.toml` / lockfile pin `8fce2041b9fe742cb2a6df9e74cb262606672742`. Unchanged. |
| Merge | `25e3972` merged `origin/main` `78a6f5b`. Ancestry: `f0c001e`, `f86d1c3`, `78a6f5b`. |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` is `tgt_7e208a0c76a44980a83b63af976b1f22`. `BOTSTER_TARGET_REPO` and the origin remote are `trybotster/botster-hub`. `list_spawn_targets` maps that id to admitted target `botster-hub`. The approved plan used the same `target_id` and repository. Implementation stayed in this run worktree.

Botster MCP in this Grok session failed handshake because `BOTSTER_SESSION_UUID` was passed as the literal `${env:BOTSTER_SESSION_UUID}`. Pipeline tools were called through `botster mcp-serve` NDJSON with the live session UUID. That is a host integration workaround, not a product change.

`project_pipelines_list_ticket_dependencies` for this ticket returns `dependency_1787143530_547584` on `ticket_1787143511_231816` with `depends_on_status: closed`. `blocking_dependencies` is empty.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

Not loaded: [[project-pipelines-playbook]]. This ticket does not change Project Pipelines package or plugin paths.

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[ShutdownSession suppression live tests are not a red oracle]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[flake oracles over typed response frames must print the full typed error body]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[live acceptance tests must not depend on a loop tick window]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[process-global test counters make zero waits observe other tests under default-concurrency lib load]]
- [[real daemon start boundaries serialize against process global taint]]
- [[session registry process pid identifies the pty command not the session worker]]
- [[hub drain advances non attached session lifecycle]]
- [[hub shutdown preserves durable session workers]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[test script required for rust tests not cargo test]]
- [[cli test sh filters match rust test names not filenames]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[implementation reports separate merge cleanup from feature behavior]]
- [[refresh target branches before mitigating failures owned by sibling tickets]]

### Constraints applied before edits

- Work only in the `botster-hub` ticket worktree.
- Unique diff stays test-only. No `src/` change authored by this ticket. No Core pin change. No `packages/hub-test-support` change.
- Completion waits consume production `ReadScreen` exact-session observe and production `ShutdownSession` classification. Terminal Drain does not discover lifecycle.
- Every input-gated producer test arms `SessionCleanupGuard` immediately after Spawn and disarms only after production `ShutdownSession` then `RemoveSession`. Hub-process teardown is not worker cleanup.
- Sleep is polling backoff inside a deadline. It is not a correctness oracle. The held-live negative control counts observe turns.
- Use repository wrappers: `./test.sh --locked`, plus the documented session-worker prebuild.
- Direct merge. Do not create a pull request.
- Consume the closed production ticket by merging `origin/main` before the clean suite. Do not re-author suppress-before-teardown.

## Files changed

Feature behavior (unique versus `origin/main`):

- `tests/hub_daemon_lifecycle/session_fixtures.rs` — `wait_for_authoritative_session_exit` (production `ReadScreen` then `ListSessions`), `assert_session_stays_running_across_observe_turns`, `wait_for_producer_ready`, deterministic `SessionCleanup { already_exited }` assert with full typed error body, owned-id `SessionCleanupGuard`, and `production_cleanup_after_authoritative_exit`.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — finite producer prints `producer-ready` and flushes before the release gate; new held-live producer waits on a second exit-release file after writing exact bytes.
- `tests/hub_daemon_lifecycle/sessions.rs` — focused tests `external_hub_finite_producer_completion_uses_production_lifecycle_signal` and `external_hub_held_live_producer_defers_completion_until_exit_release`; migrations of unix exact-bytes, split-UTF-8, ghostsnp-then-bytes, and `shutdown_after_observed_exit_returns_session_cleanup`, each with a cleanup guard except the ungated `sleep 0.1` observed-exit test.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` — WebRTC exact-bytes and round-based shutdown-after-live-exit tests wait for authoritative exit, then assert one `SessionCleanup { already_exited }` branch, with guards armed after Spawn. Blind-call typed-error siblings stay untouched.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` — `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` waits on production observe for `pex-exit`, arms cleanup guards, and keeps main's occupancy-generation close-event assertions.

Handoff:

- `docs/reports/replace-pty-process-pid-and-marker-lifecycle-oracles-implement.md` — this report.
- `docs/reports/held-live-red-on-revert-ablation.txt` — executed red-on-revert evidence.
- `docs/plans/replace-pty-process-pid-and-marker-lifecycle-oracles.md` — revision 5.

## Merge/rebase cleanup

Consumed closed `ticket_1787143511_231816` by merging `origin/main` `78a6f5b` at `25e3972`. That merge brought production suppress-before-teardown and its proofs into this worktree. Those paths match `origin/main`. This ticket did not re-author them.

Conflict only in `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` at the pex test. Resolution kept this ticket's `SessionCleanupGuard`s and `wait_for_authoritative_session_exit`, and kept main's Status occupancy generation, listed lifecycle, late Status, and generation-scoped `no_terminal_subscription_closed`. Dropped main's `sleep 1s`. rustfmt of that conflicted file also wrapped nearby assertions; no behavior change outside the pex wait and guards.

`git merge-base --is-ancestor f0c001e HEAD`, `f86d1c3`, and `78a6f5b` all succeed.

## Ownership boundaries preserved

Unique `git diff origin/main` is Hub-owned tests and documents. Core pin, hub-client DTOs, published hub-test-support fixtures, and other repositories were not changed by this ticket. Production `src/` on this branch equals `origin/main` after the merge.

The completion signal uses two shipped Hub production surfaces: exact-session observe inside `ReadScreen` and exact-session classification inside `ShutdownSession`. This ticket does not change those production paths. This ticket did not add observation to Spawn, Attach, Drain, Input, Resize, or other operation paths.

## Cross-repo dependencies or separately routed work

No cross-repository ticket. Same-repository production dependency `ticket_1787143511_231816` / child run `run_1787143511_194671` / `dependency_1787143530_547584` is closed. Duplicate ticket `ticket_1787143509_572595` was closed. The close-event matrix in `finding_1787143211_159905` is delivered on that production ticket and consumed here by merge.

## Deviations from plan

None. Consuming the closed production dependency is the revision 4 contract. Revision 5 records that consumption.

Implementation-only helpers that the plan implied but did not name: `wait_for_producer_ready`, `assert_session_cleanup_already_exited`, `shutdown_after_authoritative_exit`, and `production_cleanup_after_authoritative_exit`. `shutdown_short_lived_session` still has other callers and was left in place.

## Review findings addressed

- `finding_1787141572_169138`: cleanup guards armed after Spawn in the five input-gated migrated producer tests; disarm only after RemoveSession.
- `finding_1787141572_503373`: pex test waits on production observe instead of `sleep 1s`.
- `finding_1787141573_874007`: executed ablation recorded in `docs/reports/held-live-red-on-revert-ablation.txt`.
- `finding_1787143211_492928`: this ticket authored no production `ShutdownSession` edit. The required Hub ticket, child run, and dependency were registered. That production ticket is now closed. This visit merged `origin/main` and reran the clean suite.
- `finding_1787143211_159905`: close-event matrix is on closed `ticket_1787143511_231816`. This visit consumes it. Load-bearing suppression oracles are `daemon_transport::tests::shutdown_session_arm_installs_exact_suppression_before_core_request` and the Unix/WebRTC `exact_generation_suppression_silences_running_close_and_preserves_later_generation` unit tests ([[ShutdownSession suppression live tests are not a red oracle]]). The live pex no-event assertions are not credited as that regression proof.

## Tests and downstream proof run

Production path proved by the tests themselves:

1. `ReadScreen { session_id }` calls `observe_session_lifecycle` for that session.
2. `ListSessions` confirms `lifecycle == "exited"`.
3. `ShutdownSession` returns `SessionCleanup { outcome: "already_exited" }`.
4. Focused tests then `RemoveSession` and disarm `SessionCleanupGuard`.
5. Held-live negative control: after exact non-UTF-8 bytes are on the live plane, five production observe turns still report `running` until the exit-release file exists.
6. `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` waits for `pex-exit` through production observe, then uses main's occupancy-generation close-event assertions.

Commands (repository wrappers; no bare `cargo test` for runtime proofs):

- `cargo fmt --all -- --check` — pass
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — pass
- `cargo test --doc --workspace` — pass
- `cargo build --locked -p botster-core-daemon --bin botster-session-worker` — pass
- Focused, each `./test.sh --locked --test hub_daemon_lifecycle_test <name> -- --exact` — pass:
  - `external_hub_finite_producer_completion_uses_production_lifecycle_signal`
  - `external_hub_held_live_producer_defers_completion_until_exit_release`
  - `external_hub_live_output_preserves_exact_bytes`
  - `external_hub_live_output_preserves_split_utf8_frames`
  - `external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes`
  - `shutdown_after_observed_exit_returns_session_cleanup`
  - `external_hub_webrtc_live_output_preserves_exact_bytes`
  - `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`
  - `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed`
- Consumed unit lanes (substring filters; nested modules, so not `--exact`; [[cli test sh filters match rust test names not filenames]]):
  - `./test.sh --locked --lib shutdown_session_arm_installs_exact_suppression_before_core_request` — 1 passed
  - `./test.sh --locked --lib exact_generation_suppression_silences_running_close_and_preserves_later_generation` — 2 passed (Unix and WebRTC)
- Red-on-revert ablation of the held-live running assert — fail, exit 101, then restored green (recorded earlier)
- One clean default-concurrency `./test.sh --locked` without retry at `25e3972`. `hub_daemon_lifecycle_test`: 257 passed, 1 ignored. `botster_hub` lib: 422 passed. Workspace exit 0.

No PID, descendant-PID, done-file, or pre-exit-marker completion oracle was introduced.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | Each focused and migrated test owns one isolated hub or CLI data directory and `daemon_test_guard`. |
| Bounds | Exit and ready waits use a 10 s deadline and print last `ReadScreen` kind, typed error body, and listing. Panic-path worker cleanup is `SessionCleanupGuard` driving production `sessions shutdown`. No unbounded `block_on`. |
| Late-message matrix | Not applicable on this ticket. No new ownership-creating message surface. Tests consume existing Spawn, Attach, ReadScreen, ShutdownSession, RemoveSession. The production close-event matrix is on the consumed closed ticket. |
| Production-path proof | The oracle is the production path: ReadScreen exact-session observe, then ListSessions `exited`, then ShutdownSession `already_exited`. Held-live is the live negative control with an executed red-on-revert ablation. Suppression insertion and exact-generation behavior are proved by the merged unit lanes, not by the live no-event tests. |
| Ownership identity | Per-test `session_id` strings. Guards key on data directory plus session id. |
| Sibling fail-closed | Isolated hubs. Failure-path worker cleanup is the armed guard, not Hub-process teardown. Residual identity failure remains harness taint. |

## Unverified behavior or residual risk

- Exact deadline headroom under heavier-than-this-machine ambient load remains an observation, not a correctness claim. Widening the wait bound is legal; softening `already_exited` is not.
- `python3` remains a host assumption already used by merged tests.
- Production `ReadScreen` must keep its exact-session observe call. This ticket did not change that path; it consumes it.
- Blind-call `ShutdownSession` typed-error contracts remain in sibling tests and were covered by the clean full suite, not by extra isolated reruns.
- Live no-event tests, including pex, are not a red oracle for suppress-before-teardown insertion order ([[ShutdownSession suppression live tests are not a red oracle]]). The merged unit lanes carry that proof.

## Missing vault guidance discovered

1. Capture candidates after merge, not done in this visit:
   - finite and held-live PTY producer completion uses the production exact-session observe wait
   - release-file-gated python producers prove startup with a ready marker before release

Convention conflicts: none. [[host ShutdownSession classification must call the exact-session Core query]] now records that exact-session classification ships on main.
