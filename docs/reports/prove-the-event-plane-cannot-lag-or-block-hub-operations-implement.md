# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787417420_860296`) |
| Approved plan | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` revision 8 |
| Merge policy | `direct` into `main`; do not create a PR |
| Prior implement commits | `b1413884f7b8af67e3fb9b7ac51798c302a3706b`, `0b2448478be940b5e83df31c07111999c84a1c5d` |
| Returned from | `review_1787417409_162846` (`changes_required`, six findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: `project_pipelines_current_context` maps ticket and run `target_id` to `tgt_7e208a0c76a44980a83b63af976b1f22`. `list_spawn_targets` resolves that id to `botster-hub`. Work stayed in this ticket worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

### Targeted atomic notes

- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[event plane client proof uses library contract fixtures]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[hub client event queue max requires Botster test mode]]
- [[test script required for rust tests not cargo test]]
- [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[Client event holders are connection-scoped]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[web event plane budgets are published numeric host limits]]
- [[event plane terminal budgets are new coexistence regression budgets]]
- [[test names do not prove their bodies can fail on the named claim]]

`project-pipelines-playbook` was not loaded. This run did not change Project Pipelines sources.

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787417409_124162` campaign never computes or gates budgets | blocker | `nearest_rank`, `sample_max`, `metrics_from_stats`, `derive_thresholds`, `gate_relative`, and `gate_all` compute p50/p95/p99/max/throughput. Calibration writes ABS*/THRMIN. Acceptance reads the committed dataset and panics on any absolute or relative miss. Always-on `event_plane_saturation_percentile_and_budget_formulas` can fail on the formula. |
| `finding_1787417410_429017` workflow gives the campaign 900 s | blocker | `resolve_run_timeout` forces 3600 s for `event-plane-saturation` even when `BOTSTER_LOADED_RUN_TIMEOUT_SECONDS=900`. The workflow run step exports 3600 for that target. Selftest greps `run_timeout_seconds=3600` and `run_timeout_max=3600` under env 900. |
| `finding_1787417410_697122` required fault, observability, North Star, and teardown gates absent | high | Named fault lanes: ShedFull/over-rate, in-process ShedBusy, plugin mailbox pressure, client mailbox gap, dropped journal wake, journal capacity, handler timeout, plugin reload, Unix reconnect, WebRTC close with Unix survival. Observability asserts admission, delivery/shed, latency, queue bounds, and T1–T4 counters. North Star oracles: identity, non-UTF-8 bytes, ordering, input, resize, detach/reconnect, late-attach history, ProcessExit. Late-message matrix covers SubscribeEvents, Spawn, Attach, SubscribeEntities, UnsubscribeEvents, and admitted holders in both queue orders. Fail-closed blast radius stays the production hang-close child oracle, source-guarded. |
| `finding_1787417410_500688` client conformance runs before saturation | high | Unix event connection is owned through the 600 s window, drives Status, consumes `PackageEvent`/`EventGap`, then subscribe-filter, unsubscribe, reconnect-without-replay. Isolated `run_client_event_conformance` remains the compact always-on contract. |
| `finding_1787417410_169491` detached and forgotten resources | high | No `mem::forget`. Emitter is `JoinHandle` + `AtomicBool` stop, joined before shutdown. Measurement workers return `JoinHandle`s; panics and operation errors fail the arm. Unix and WebRTC clients are dropped after use. |
| `finding_1787417410_538567` absolute path in the plan | medium | Plan spawn-target cell is path-neutral (`botster-hub`). |

## Files changed

| Path | Change |
| --- | --- |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | budget compute/gate, calibration vs acceptance, owned clients/threads, named fault lanes, North Star oracles, late-message matrix, always-on ShedBusy + formula tests |
| `script/run-loaded-daemon-lifecycle` | event-plane target timeout floor 3600 s |
| `script/run-loaded-daemon-lifecycle-selftest` | prove 3600 s wins over workflow env 900 |
| `.github/workflows/loaded-daemon-lifecycle.yml` | export 3600 s for `event-plane-saturation` |
| `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` | path-neutral spawn-target wording |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | this report |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | updated always-on proof list |

## Ownership boundaries preserved

Hub owns the campaign, budget document, generic client-event fixture, and fixture plugins. Production budgets, queue bounds, and scheduling decisions are unchanged. Observability counters and `BOTSTER_ENV=test` seams remain the merged work of `ticket_1787267568_492780`. Core, Web, TUI, and Project Pipelines sources were not edited.

## Cross-repo dependencies or separately routed work

All five plan prerequisites remain closed. This workflow executed only Hub against its locked Core. Cited, not executed: the same five tickets as the first implement visit.

## Deviations from plan

1. **Calibration literals are not yet derived.** Section 5A.4 still requires a residual-tail dispatch on `ubuntu-24.04`. This host is Darwin. `write_calibration_dataset` only overwrites the committed JSON when `cfg!(target_os = "linux")` and `BOTSTER_EVENT_PLANE_COMMIT_CALIBRATION=1`.
2. **The 300-session campaign stays `#[ignore]`.** Default lifecycle-suite runs must not take 1260 s. The loaded runner selects `event_plane_saturation_campaign` with `--ignored --exact`.
3. **ShedBusy stays in-process.** Section 5E marks that lane unreachable from a spawned daemon. Always-on `event_plane_saturation_shed_busy_is_non_blocking` calls `PackageEventRouter::test_with_inner_held`.
4. **WebRTC fail-closed blast radius stays the production hang-close child.** IsolatedHub cannot read `active_peer_count()` / `has_dedicated_runtime()`. The campaign source-guards `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline` and `BOTSTER_HUB_WEBRTC_HANG_CLOSE_CHILD`, and asserts Unix fleet survival after a successful WebRTC close. It does not assert sibling survival on ultimate close failure.

The committed plan's numeric gates still require the reference-runner calibration and acceptance dispatches.

## Runtime-teardown lenses

| Lens | Implementation |
| --- | --- |
| Isolation | Quiet and churn sessions are Unix. WebRTC uses the dedicated runtime. Event holders are `(connection_id, subscription_id)`. |
| Bounds | Existing close/write/invocation timeouts unchanged. Campaign run timeout 3600 s. ShedBusy returns without waiting. Emitter and workers are joined. |
| Late-message matrix | SubscribeEvents, UnsubscribeEvents, Spawn, Attach, SubscribeEntities, admitted holders; closed-first and message-first; reused ids. |
| Production-path proof | IsolatedHub starts the real `botster-hub` binary. Hang-close fail-closed remains the production unit oracle. |
| Ownership identity | Reused subscription ids admit on the replacement connection only. Duplicate live Spawn is a typed operator error. |
| Sibling fail-closed | Successful WebRTC close: Unix fleet survives. Ultimate close failure: production hang-close child asserts bounded sibling sacrifice; campaign does not invert that policy. |

## Tests and downstream proof run

Commands:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 4 passed, 1 ignored
- `script/run-loaded-daemon-lifecycle-selftest` — passed, including `event-plane-saturation run timeout is 3600s when the workflow default is 900s`
- `git diff --check` — pass

The ignored 300-session campaign and the GitHub `event-plane-saturation` dispatch were not run on this Darwin host.

## Unverified behavior or residual risk

- Absolute p50/p95/p99/max/throughput budgets remain unpublished until the ubuntu-24.04 residual-tail calibration commit.
- Fleet `N = 300` PTY admission on the reference runner is still unknown (plan U1).
- Named fault lanes and North Star oracles are implemented in the ignored campaign; they have not executed at fleet scale.
- Ablation of `events.emit` wait, queue-bound removal, resync drop, and adapter snapshot-phase naming is source-guarded, not a live red-on-revert campaign.

## Missing vault guidance discovered

None that blocked this change. A durable candidate from Review remains: target-specific timeout defaults must override workflow-level environment defaults. This visit encoded that rule in `resolve_run_timeout` and the selftest.
