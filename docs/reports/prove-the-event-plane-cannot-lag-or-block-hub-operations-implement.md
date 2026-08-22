# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787419908_539800`) |
| Approved plan | revision 8 |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `761c79adb61ac7e845fee8842db71b609575196a` |
| Returned from | `review_1787419888_905139` (`changes_required`, six findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work stayed in this ticket worktree.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[hub shutdown preserves durable session workers]]
- [[event plane terminal budgets are new coexistence regression budgets]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787419888_311756` calibration commit makes acceptance SHA check impossible | blocker | Revisions split: `source_revision` is the Hub SHA that produced the numbers. `calibration_dataset_commit` is `git log -1` of the committed dataset path. Acceptance no longer requires `SUBJECT_SHA == source_revision`. |
| `finding_1787419888_797699` empty terminal-output polls recorded as failures | blocker | `terminal_output` samples only when a Drain actually delivers output, or when Drain itself errors. A 20 ms poll between 100 ms lines is not a product failure. |
| `finding_1787419889_651294` accepted artifact written before faults | high | `write_phase_dataset` runs **after** `run_fault_campaign`. Status is `calibrated` or `accepted` only when measurement, gates, faults, and teardown have returned. |
| `finding_1787419889_635184` missing observability recording/gates | high | Dataset includes enabled, decoupled, and fault observability (admission, delivery, latencies, shed, gaps, T1–T4, queue count/age). Owner-turn and ready-wait gate against `MAX_OWNER_TURN_MS` and `MAX_READY_OPERATION_WAIT_MS`. Queue **bytes** are not on `DaemonQueueAgeObservation`; the dataset records `queue_bytes_available: false` rather than inventing a column. |
| `finding_1787419889_903746` unapproved floor3 THRMIN | high | Restored plan formula `THRMIN = floor_int(THRcal_e * T)` as whole operations per second. Throughput **measurement** stays f64 so 200/600 is not truncated to 0. Relative ratios still compare at three decimal places. |
| `finding_1787419889_147805` WebRTC control errors ignored; no message-first | high | WebRTC Status during saturation `.expect`s. Closed-first: drop holder, reuse id on replacement, Unix fleet survives. Message-first: two live holders, drop first, sibling Status and Unix fleet survive. |

## Files changed

| Path | Change |
| --- | --- |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | SHA split, post-fault dataset, floor_int THRMIN, skip empty output polls, observability record/gates, WebRTC holder orders |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md` | this report |
| `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json` | result note |

## Ownership boundaries preserved

Hub owns the campaign. Production budgets and `DaemonQueueAgeObservation` shape are unchanged. Direct merge; no PR.

## Deviations from plan

1. Calibration/acceptance still have not executed on ubuntu-24.04.
2. Queue-byte columns remain absent from the live observability DTO. The campaign records that gap instead of adding a public protocol field in this ticket.
3. `floor_int` of ops/s for a 200-sample/600s stream is 0. That is the approved formula. Relative throughput still gates.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 5 passed, 1 ignored
- `git diff --check` — pass

## Unverified behavior

ubuntu-24.04 residual-tail calibration and acceptance have not run. N=300 campaign is still `#[ignore]` on Darwin.

## Missing vault guidance

None that blocked this change.
