# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787420425_587553`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `032104bcea002b31915e7c5d50a82668d31feb45` |
| Returned from | `review_1787420407_239617` (`changes_required`, five findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[saturation counters do not acquire the contended lock they report]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[Hub test support version bumps must update the Node mirror test literals]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787420407_140713` queue-byte evidence unavailable | blocker | Occupancy bytes already existed on the router. They are now published through the saturation-safe atomic Status path: `queue_bytes` on each `DaemonQueueAgeObservation` and `global_in_flight_bytes` on `DaemonObservabilityCounters`. Campaign gates count, bytes, global bytes, and 1000 ms queue age. Unpublished `@trybotster/hub-test-support` **0.1.42** because `0.1.41` is already published. |
| `finding_1787420407_699650` acceptance not bound to calibration commit | blocker | Workflow input `event_plane_calibration_commit`. Acceptance validate-only and the campaign require `BOTSTER_EVENT_PLANE_CALIBRATION_COMMIT` and compare it to `git log -1` of the calibration file. Acceptance artifacts keep the calibration `source_revision` and record `acceptance_revision` separately. |
| `finding_1787420407_365468` terminal output not sequenced | high | Noisy PTY emits `N%08d` lines. The sampler parses sequence ids, fails on gaps/duplicates, and records one sample per delivered item. Empty polls stay skipped. |
| `finding_1787420407_535488` incomplete observability artifact | high | Dataset records full latency histograms, backpressure, worker-stop, gaps, T1–T4, owner-turn, ready-wait, global bytes, and per-queue count/bytes/age. Queue age gates at the published 1000 ms bound. |
| `finding_1787420407_193981` weak fault oracles | high | Plugin mailbox requires consumer occupancy or backpressure. Cursor expiry wraps the 16-row journal then requires the quiet fleet still running. Reload and Unix reconnect require post-fault event delivery. WebRTC reconnect `.expect`s Status. |

## Files changed

Campaign, event-plane atomics, protocol DTO/TS, unpublished hub-test-support 0.1.42, workflow calibration-commit input, runner selftest.

## Ownership

Hub owns Status observability and the campaign. Optional DTO fields are additive. Direct merge; no PR.

## Deviations

Calibration/acceptance still have not executed on ubuntu-24.04. Package 0.1.42 is unpublished until a publish ticket.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 5 passed, 1 ignored
- `script/run-loaded-daemon-lifecycle-selftest` — passed, including acceptance requiring the calibration commit
- `npm run sync` in `packages/hub-test-support`

`npm test` in that package needs a local `@trybotster/ui-contract` install and was not re-run here after sync.

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not run.
