# Implement report: fix flaky unix_adapter_unbound_printf_stream_attach_completes

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786937228_425608` |
| Run | `run_1786937300_850110` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | ticket `target_id` plus worktree `origin` remote `https://github.com/trybotster/botster-hub.git` |
| Pipeline worktree | the ticket worktree on `project-pipelines/ticket_1786937228_425608` |
| Plan | `docs/plans/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite.md` revision 5 |
| Delivery | direct-merge; no pull request |
| Class | not runtime-teardown (`teardown_class_applies: no`) |
| Implement checklist | `checklist_1787005013_972813` |
| Oracle decision | `question_1787005265_458413` path D |

Independent routing: `project_pipelines_current_context` and the approved plan both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Work stayed in the ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Targeted atomic notes

- [[botster-architecture]]
- [[cli-patterns]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[proposed ProcessExited closes terminal subscriptions but not the host session]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[observe-first attached Drain can return SessionLifecycle without ProcessExit]]
- [[test script required for rust tests not cargo test]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[botster review and verify must scan all committed artifacts for pii]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. [[botster runtime teardown lenses]] — teardown class does not apply.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Keep production observation, budgets, and teardown paths unchanged.
- Do not add exact-session observe to ReadScreen.
- Do not call ShutdownSession as an observation stimulus.
- Use `./test.sh`, not bare `cargo test`.
- Direct merge. Do not create a pull request.

## Files changed

Feature behavior:

- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` — restore `unix_adapter_unbound_printf_stream_attach_completes` to the default-Hello path. Hold the child on a release file until that unbound Attach returns, then print the marker. Accept host-row `running` or `exited`. Add `unix_adapter_bound_printf_stream_attach_delivers_process_exit` for path D `process_exit` proof with a release file, `SessionCleanupGuard`, and a post-printf sleep that is not an attach deadline.

Handoff:

- `docs/plans/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite.md` — revision 4 resyncs the oracle and acceptance checks to path D.
- `docs/reports/fix-flaky-unix-adapter-unbound-printf-attach-under-default-concurrency-lifecycle-suite-implement.md` — this report.

Merge/rebase cleanup: none.

## Ownership boundaries preserved

Hub owns the daemon lifecycle test. Production `src/daemon_transport.rs`, `src/session_projection.rs`, observation budgets, and teardown paths were not edited. Core, hub-client, Web, TUI, and package/plugin paths were not edited. The test decodes opaque unix envelope JSON only inside the test helper. Production Hub still does not inspect terminal bodies.

## Cross-repo routing

No cross-repository prerequisite and no PR. Same-target siblings, not absorbed:

| Ticket | Owns | Relation |
| --- | --- | --- |
| `ticket_1786913892_208903` | WebRTC write-budget sibling continuation | Discovered this flake. Ticket text forbids absorption. Closed before this visit. |
| `ticket_1786921010_869253` | near-limit lib-suite flake | Same flake class, different suite. |
| `ticket_1786938984_190098` | ready_spawn wall-clock budget failures | Known-baseline owner for suite accounting. |
| `ticket_1786977409_499180` | exact-session observe / ShutdownSession suite-load | Not absorbed. This ticket did not add production ReadScreen observe. |
| `ticket_1787011756_403471` | process_ownership spawn_failed and openpty | Created from suite runs 2-4. No serial dependency. |
| `ticket_1787011760_843823` | webrtc write-budget recurrence | Created after the closed owner recurred in run 3. No serial dependency. |
| `ticket_1787011770_110683` | leftover-worker suite extras | Created from run 4 spawn, lease, and attach failures. No serial dependency. |

## Deviations from plan

Revision 3 required a ReadScreen-driven wait for `lifecycle == "exited"`. The first focused wrapper run failed at 5s with the row still `running`. Marker ReadScreen parks ProcessExited. Orchestrator path D replaced that oracle.

Accepted deviations, now in committed plan revision 4:

- Exit oracle is attached-subscription `process_exit`, not `ListSessions.lifecycle`.
- Spawn keeps `sleep 1` after the marker printf so the unix adapter attaches while the child is alive. A printf-only child never delivered `process_exit` in two focused runs.
- Host Drain serviceability uses the owning unix adapter connection. A second default-hello Drain returned `snapshot_stream_forbidden`.
- The unused full-suite slot from `question_1787005080_932674` was released. A new slot was requested only after focused gates passed.
- Plan check 3 asked for five consecutive binding-green suite runs. Orchestrator answer `question_1787011724_901513` advances this ticket on the focused proof plus the five target-test passes. It forbids a full-suite rerun now. Final integration stays the strict clean-suite gate.
- Run 4 is invalid suite-environment evidence. Leftover workers produced three suite tallies in one log. It does not count against this ticket.
- Plan check 6 (full `./test.sh --locked`) was not run. It remains non-binding and needs a later slot.
- Review `review_1787012453_488679` required the named test to stay on the default-Hello unbound path. A printf-only spawn left ReadScreen empty on focused repeats. The named test now holds the child until default-Hello Attach returns. Bound `process_exit` proof lives in a separate test.

## Tests and downstream proof run

Tracked `.gitignore` is 53 bytes and matches `HEAD`. The ticket worktree path has no `:`. No `CARGO_TARGET_DIR` override.

Production entry point: unchanged. The ticket is a test-oracle repair. The production path still parks or observes ProcessExited independently of this test.

### Pre-change / oracle discovery

| Command / observation | Result |
| --- | --- |
| Revision 3 exited poll on printf-only spawn | exit 101; `session must reach exited`; lifecycle still `running` |
| Unix adapter attach after printf-only spawn | `attach_state` + `terminal_output` + `attach_state`; no `process_exit` in 5s |
| Unix adapter attach with `printf ...; sleep 0.5` | `process_exit` arrived; later default-hello Drain was `snapshot_stream_forbidden` |

### Red-proof

Both controls used `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes`. Both sabotages were reverted after the runs.

| Control | Sabotage | Exit | First failure |
| --- | --- | --- | --- |
| A (unbound retention) | `ShutdownSession` before host-row check | 101 | `host session lifecycle stopping is not running or exited` at `unix_terminal_adapter.rs:91` |
| B (bound `process_exit`) | skip writing the bound release file | 101 | `attached terminal subscription must deliver process_exit` at `unix_terminal_adapter.rs:912` |

### Acceptance tallies

| Command | Result |
| --- | --- |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0 |
| `./test.sh --locked --test hub_daemon_lifecycle_test -- --exact unix_adapter_unbound_printf_stream_attach_completes unix_adapter_bound_printf_stream_attach_delivers_process_exit` × 20 | 20/20 PASS after Review split |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `./test.sh --locked --test hub_daemon_lifecycle_test` × 5 | slot `question_1787005950_441740`; see suite table |
| Full `./test.sh --locked` (plan check 6) | not run; non-binding; needs a separate slot after the five-run slot release |

Slot `question_1787005950_441740` ran five consecutive unfiltered lifecycle suites. The branch-owned target test passed in every run. The slot release answer `question_1787011724_901513` confirms the slot is free, forbids a full-suite rerun now, and advances this ticket on the focused proof plus those five target passes.

| Run | Exit | Tally | Target test | Binding class | Other failures |
| --- | --- | --- | --- | --- | --- |
| 1 | 0 | 219 passed; 0 failed; 1 ignored; 337.78s | ok | binding-green | none |
| 2 | 101 | 216 passed; 3 failed; 1 ignored; 350.10s | ok | extras only | `ready_spawn_stays_within_budget_during_session_snapshot_assembly` at `sessions.rs:3634` (63.647208ms) recorded on `ticket_1786938984_190098`. `process_ownership_external_hub_test_support_cleans_up_isolated_daemon` at `sessions.rs:4351` `spawn_failed` routed to `ticket_1787011756_403471`. `process_ownership_operator_console_readiness_failure_reaps_console_and_owned_daemon` at `operator_console_fixtures.rs:357` `openpty: Device not configured` routed to the same ticket. |
| 3 | 101 | 214 passed; 5 failed; 1 ignored; 350.57s | ok | extras only | `process_ownership_daemon_restart_adopts_then_shuts_down_worker_session` at `shutdown.rs:2463` `spawn_failed` routed to `ticket_1787011756_403471`. `process_ownership_external` `spawn_failed` again. operator_console `openpty` again. `ready_spawn` at `sessions.rs:3641` `first snapshot must be complete` recorded on `ticket_1786938984_190098`. `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable` at `webrtc_terminal_adapter.rs:947` routed to `ticket_1787011760_843823`. |
| 4 | 101 | log had three tallies: 218/1 in 808.41s, 215/4 in 978.50s, 212/7 in 943.63s | ok | invalid suite-environment | Leftover workers produced multiple suite tallies. This run is not suite evidence for this ticket. `spawn_failed` and `openpty` extras stay on `ticket_1787011756_403471`. Other extras stay on `ticket_1787011770_110683`. ready_spawn pair stays on `ticket_1786938984_190098`. No serial dependencies. |
| 5 | 0 | 219 passed; 0 failed; 1 ignored; 363.19s | ok | binding-green | none |

Isolation for run-2 `process_ownership_external`: fail on this branch immediately after the suite, pass on this branch after cooldown (4.27s), pass on `origin/main` `c71e22d`. operator_console isolation on this branch passed.

Downstream proof: not required. No public surface, DTO, pin, or runtime behavior changes.

## Unverified behavior or residual risk

- Review `review_1787012453_488679` asks for authorized suite gates after the split. This visit has not started an unfiltered suite. A new exclusive slot is required. Final integration remains the strict clean-suite convergence gate.
- Run 4 is invalid suite-environment evidence and does not count against this ticket.
- Plan check 6 (one full `./test.sh --locked`) was not run. It is non-binding and needs its own orchestrator slot.
- `sleep 1` can still lose the attach-before-exit race under extreme spawn delay. Control B proves the `process_exit` assertion is live when the child stays up.
- Known-baseline `ready_spawn_*` failures remain owned by `ticket_1786938984_190098`. `spawn_failed` and `openpty` extras remain on `ticket_1787011756_403471`. No serial dependencies.

## Missing vault guidance discovered

The vault already has [[observed-exit waits must issue a production exact-session observe turn]] and [[observe-first attached Drain can return SessionLifecycle without ProcessExit]]. It does not yet record that a printf-only child can exit before attach and leave the bound adapter without a later `process_exit` frame. Capture after Verify if the repair holds.
