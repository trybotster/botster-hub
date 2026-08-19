# Implement report: Hub tests replace PTY process PID and marker lifecycle oracles

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786912572_610381` |
| Run | `run_1787136288_939918` |
| Step | `botster_stack_implement` (`run_step_1787143222_829808`) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786912572_610381` |
| Plan | `docs/plans/replace-pty-process-pid-and-marker-lifecycle-oracles.md` revision 4, answering `review_1787143211_621764` |
| Delivery | `merge_policy: direct`; no pull request |
| Class | runtime-teardown, narrowly: terminal-state markers versus live-runtime session completion. Test-only. Production close-event ordering is `ticket_1787143511_231816`. |
| Locked Core | `Cargo.toml` / lockfile pin `8fce2041b9fe742cb2a6df9e74cb262606672742`. Unchanged. |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` is `tgt_7e208a0c76a44980a83b63af976b1f22`. `BOTSTER_TARGET_REPO` and the origin remote are `trybotster/botster-hub`. `list_spawn_targets` maps that id to admitted target `botster-hub`. The approved plan used the same `target_id` and repository. Implementation stayed in this run worktree.

Botster MCP in this Grok session failed handshake because `BOTSTER_SESSION_UUID` was passed as the literal `${env:BOTSTER_SESSION_UUID}`. Pipeline tools were called through `botster mcp-serve` NDJSON with the live session UUID. That is a host integration workaround, not a product change.

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
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[implementation deviations must resync committed plan acceptance checks]]

### Constraints applied before edits

- Work only in the `botster-hub` ticket worktree.
- Test-only. No `src/` change. No Core pin change. No `packages/hub-test-support` change. The ShutdownSession close-order defect is `ticket_1787143511_231816`.
- Completion waits consume production `ReadScreen` exact-session observe and production `ShutdownSession` classification. Terminal Drain does not discover lifecycle.
- Every input-gated producer test arms `SessionCleanupGuard` immediately after Spawn and disarms only after production `ShutdownSession` then `RemoveSession`. Hub-process teardown is not worker cleanup.
- Sleep is polling backoff inside a deadline. It is not a correctness oracle. The held-live negative control counts observe turns.
- Use repository wrappers: `./test.sh --locked`, plus the documented session-worker prebuild.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `tests/hub_daemon_lifecycle/session_fixtures.rs` — `wait_for_authoritative_session_exit` (production `ReadScreen` then `ListSessions`), `assert_session_stays_running_across_observe_turns`, `wait_for_producer_ready`, deterministic `SessionCleanup { already_exited }` assert with full typed error body, owned-id `SessionCleanupGuard`, and `production_cleanup_after_authoritative_exit`.
- `tests/hub_daemon_lifecycle/package_fixtures.rs` — finite producer prints `producer-ready` and flushes before the release gate; new held-live producer waits on a second exit-release file after writing exact bytes.
- `tests/hub_daemon_lifecycle/sessions.rs` — focused tests `external_hub_finite_producer_completion_uses_production_lifecycle_signal` and `external_hub_held_live_producer_defers_completion_until_exit_release`; migrations of unix exact-bytes, split-UTF-8, ghostsnp-then-bytes, and `shutdown_after_observed_exit_returns_session_cleanup`, each with a cleanup guard except the ungated `sleep 0.1` observed-exit test.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs` — WebRTC exact-bytes and round-based shutdown-after-live-exit tests wait for authoritative exit, then assert one `SessionCleanup { already_exited }` branch, with guards armed after Spawn. Blind-call typed-error siblings stay untouched.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` — `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` waits on production observe for `pex-exit` and arms cleanup guards.
- `src/daemon_transport.rs` — reverted. No production ShutdownSession change remains on this ticket.

Handoff:

- `docs/reports/replace-pty-process-pid-and-marker-lifecycle-oracles-implement.md` — this report.
- `docs/reports/held-live-red-on-revert-ablation.txt` — executed red-on-revert evidence.
- `docs/plans/replace-pty-process-pid-and-marker-lifecycle-oracles.md` — revision 4 answering `review_1787143211_621764`.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

All remaining edits are Hub-owned tests and documents. Core pin, hub-client DTOs, published hub-test-support fixtures, and other repositories were not changed. `src/daemon_transport.rs` is restored to the test-only tree.

The completion signal uses two shipped Hub production surfaces: exact-session observe inside `ReadScreen` and exact-session classification inside `ShutdownSession`. This ticket does not change those production paths. This ticket did not add observation to Spawn, Attach, Drain, Input, Resize, or other operation paths.

## Cross-repo dependencies or separately routed work

No cross-repository ticket. Same-repository production dependency: `ticket_1787143511_231816` / child run `run_1787143511_194671` / `dependency_1787143530_547584`. Duplicate ticket `ticket_1787143509_572595` was closed. The close-event matrix in `finding_1787143211_159905` is owned by that production ticket.

## Deviations from plan

Revision 4 restores the original test-only non-scope. Revision 3's production `src/` edit is reverted. The committed plan contract is resynced.

Implementation-only helpers that the plan implied but did not name: `wait_for_producer_ready`, `assert_session_cleanup_already_exited`, `shutdown_after_authoritative_exit`, and `production_cleanup_after_authoritative_exit`. `shutdown_short_lived_session` still has other callers and was left in place.

## Review findings addressed

- `finding_1787141572_169138`: cleanup guards armed after Spawn in the five input-gated migrated producer tests; disarm only after RemoveSession.
- `finding_1787141572_503373`: pex test waits on production observe instead of `sleep 1s`. The production close-order repair is not in this ticket; it is `ticket_1787143511_231816`.
- `finding_1787141573_874007`: executed ablation recorded in `docs/reports/held-live-red-on-revert-ablation.txt`.
- `finding_1787143211_492928`: production `ShutdownSession` edit removed from this ticket. Separate Hub ticket and child run registered.
- `finding_1787143211_159905`: close-event matrix assigned to `ticket_1787143511_231816`, not implemented here.

## Tests and downstream proof run

Production path proved by the tests themselves:

1. `ReadScreen { session_id }` calls `observe_session_lifecycle` for that session.
2. `ListSessions` confirms `lifecycle == "exited"`.
3. `ShutdownSession` returns `SessionCleanup { outcome: "already_exited" }`.
4. Focused tests then `RemoveSession` and disarm `SessionCleanupGuard`.
5. Held-live negative control: after exact non-UTF-8 bytes are on the live plane, five production observe turns still report `running` until the exit-release file exists.
6. `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` waits for `pex-exit` through production observe. Close-event ordering under Active ShutdownSession is owned by `ticket_1787143511_231816`.

Commands (repository wrappers; no bare `cargo test` for runtime proofs):

- `cargo fmt --all -- --check` — pass
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — pass
- `cargo test --doc --workspace` — pass (rustdoc exception)
- `cargo build --locked -p botster-core-daemon --bin botster-session-worker` — pass
- Focused:
  - `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_finite_producer_completion_uses_production_lifecycle_signal -- --exact` — pass
  - `./test.sh --locked --test hub_daemon_lifecycle_test external_hub_held_live_producer_defers_completion_until_exit_release -- --exact` — pass
- Migrated oracles, each `./test.sh --locked --test hub_daemon_lifecycle_test <name> -- --exact`:
  - `external_hub_live_output_preserves_exact_bytes`
  - `external_hub_live_output_preserves_split_utf8_frames`
  - `external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes`
  - `shutdown_after_observed_exit_returns_session_cleanup`
  - `external_hub_webrtc_live_output_preserves_exact_bytes`
  - `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`
- `./test.sh --locked --test hub_daemon_lifecycle_test process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed -- --exact` — pass
- Red-on-revert ablation of the held-live running assert — fail, exit 101, then restored green
- One clean default-concurrency `./test.sh --locked` without retry was recorded at `6aac388` while the now-reverted production edit was present. After the revert, this visit reruns the pex contract test and fmt/clippy. The production close-order flake remains owned by `ticket_1787143511_231816`.

No PID, descendant-PID, done-file, or pre-exit-marker completion oracle was introduced.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | Each focused and migrated test owns one isolated hub or CLI data directory and `daemon_test_guard`. |
| Bounds | Exit and ready waits use a 10 s deadline and print last `ReadScreen` kind, typed error body, and listing. Panic-path worker cleanup is `SessionCleanupGuard` driving production `sessions shutdown`. No unbounded `block_on`. |
| Late-message matrix | Not applicable. No new ownership-creating message surface. Tests consume existing Spawn, Attach, ReadScreen, ShutdownSession, RemoveSession. |
| Production-path proof | The oracle is the production path: ReadScreen exact-session observe, then ListSessions `exited`, then ShutdownSession `already_exited`. Held-live is the live negative control with an executed red-on-revert ablation. Close-event ordering under Active ShutdownSession is the production dependency ticket. |
| Ownership identity | Per-test `session_id` strings. Guards key on data directory plus session id. |
| Sibling fail-closed | Isolated hubs. Failure-path worker cleanup is the armed guard, not Hub-process teardown. Residual identity failure remains harness taint. |

## Unverified behavior or residual risk

- Exact deadline headroom under heavier-than-this-machine ambient load remains an observation, not a correctness claim. Widening the wait bound is legal; softening `already_exited` is not.
- `python3` remains a host assumption already used by merged tests.
- Production `ReadScreen` must keep its exact-session observe call. This ticket did not change that path; it consumes it.
- Blind-call `ShutdownSession` typed-error contracts remain in sibling tests and were not re-run beyond the clean full suite.
- Under default-concurrency load, Active `ShutdownSession` can still emit `TerminalSubscriptionClosed` with `core_adapter_closed`. That defect is `ticket_1787143511_231816`. This ticket does not change that production path.

## Missing vault guidance discovered

1. [[host ShutdownSession classification must call the exact-session Core query]] still says the convention is not shipped. Exact-session classification already ships in Hub `ShutdownSession`. The note is stale.
2. Capture candidates after merge, not done in this visit:
   - finite and held-live PTY producer completion uses the production exact-session observe wait
   - release-file-gated python producers prove startup with a ready marker before release

Convention conflicts: none.
