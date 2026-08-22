# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787419015_709716`) |
| Approved plan | `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` revision 8 |
| Merge policy | `direct` into `main`; do not create a PR |
| Returned from | `review_1787418990_292233` (`changes_required`, six findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: `project_pipelines_current_context` maps ticket and run `target_id` to `tgt_7e208a0c76a44980a83b63af976b1f22` = `botster-hub`. Work stayed in this ticket worktree.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[event plane client proof uses library contract fixtures]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[hub shutdown preserves durable session workers]]
- [[event plane terminal budgets are new coexistence regression budgets]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787418990_188680` workflow never selects or retains calibration | blocker | Workflow input `event_plane_phase` is `calibration` or `acceptance`. Runner validate-only **requires** the env and records it. Calibration writes `ARTIFACT_DIR/event-plane-saturation-calibration.json`; acceptance writes the matching acceptance dataset. Linux calibration with `BOTSTER_EVENT_PLANE_COMMIT_CALIBRATION=1` also writes the committed report path. A retain step fails if the dataset is missing and names the reference-data commit. Selftest covers missing phase, calibration, and acceptance. |
| `finding_1787418990_672043` 300 durable sessions left alive | blocker | `shutdown_owned_sessions` sends production `ShutdownSession` for every non-exited session and waits. `assert_no_live_sessions` is the no-survivor oracle after each measurement arm and the fault campaign, before `hub.shutdown`. |
| `finding_1787418990_267207` observability/fault gates accept missing signals | high | Enabled-arm signals require admission attempts, delivery attempts, admission latency, delivery-or-shed, and non-empty queue ages with count and oldest-age bounds. ShedFull requires `shed_full` or `rejected_over_rate`. Handler timeout requires `event_handler_timed_out > 0`. Client mailbox gap requires a gap **and** the quiet fleet still complete. Dropped wakes spawn/shutdown a probe until exited and require baseline or resync reads. |
| `finding_1787418990_291544` no terminal input/output budgets | high | Concurrent terminal driver during the 600 s window: 64-byte input every 500 ms measured to echo, Drain-sampled output latency. Both are gated operations with p50/p95/p99/max/throughput. |
| `finding_1787418990_948524` WebRTC events and incomplete late-message matrix | high | WebRTC Status plus `next_host_event` run through the measurement window and must see `PackageEvent` or `EventGap`. Added Attach message-first and a WebRTC closed-first event-holder reuse. |
| `finding_1787418990_446535` integer throughput and unverified calibration | high | Throughput is `successes as f64 / window_secs`. Comparisons use three-decimal rounding. `THRMIN` is `floor3(throughput * T)`. Always-on test proves 200/600 is not truncated to 0. Acceptance validates literals, profile, formula string, and subject SHA when present. |

Prior-review timeout, join, and path-neutral fixes remain.

## Files changed

| Path | Change |
| --- | --- |
| `.github/workflows/loaded-daemon-lifecycle.yml` | `event_plane_phase` input, env, retain-dataset step |
| `script/run-loaded-daemon-lifecycle` | require and export phase; artifact dir |
| `script/run-loaded-daemon-lifecycle-selftest` | phase required / calibration / acceptance |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | f64 throughput, phase, artifacts, session teardown, terminal metrics, WebRTC under saturation, stricter faults |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | this report |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | always-on proof list |

## Ownership boundaries preserved

Hub owns the campaign and workflow wiring. Production budgets unchanged. Core, Web, TUI, and Project Pipelines sources were not edited. Direct merge; no PR.

## Deviations from plan

1. **Calibration and acceptance still have not executed on ubuntu-24.04.** This host is Darwin. The workflow is now an executable two-phase dispatch; Implement cannot run that dispatch from here.
2. **Queue-byte columns are not on `DaemonQueueAgeObservation`.** The campaign gates count and oldest age against published policy. Bytes remain a Hub observability DTO gap, not invented here.
3. **WebRTC fail-closed blast radius stays the production hang-close child.** IsolatedHub cannot read `active_peer_count()`.

## Runtime-teardown lenses

| Lens | Implementation |
| --- | --- |
| Isolation | Unix fleet, WebRTC dedicated runtime, connection-scoped event holders |
| Bounds | 3600 s run timeout; ShedBusy non-blocking; joined emitter/workers |
| Late-message | SubscribeEvents, Spawn, Attach (both orders), entities, UnsubscribeEvents, admitted holders, WebRTC event reuse |
| Production-path | IsolatedHub real binary; hang-close child remains fail-closed oracle |
| Ownership identity | reused ids on replacement connections only |
| Sibling fail-closed | successful WebRTC close: Unix survives; ultimate failure: production child |

## Tests and downstream proof run

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 5 passed, 1 ignored
- `script/run-loaded-daemon-lifecycle-selftest` — passed, including explicit phase required, calibration recorded, acceptance recorded, 3600 s timeout
- `git diff --check` — pass

The ignored 300-session campaign was not run on Darwin.

## Unverified behavior or residual risk

- Absolute budgets unpublished until the ubuntu-24.04 residual-tail **calibration** dispatch commits `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json`.
- Acceptance cannot run until that commit exists.
- Named fault lanes and terminal/WebRTC paths are implemented in the ignored campaign; they have not executed at N=300.

## Missing vault guidance discovered

None that blocked this change.
