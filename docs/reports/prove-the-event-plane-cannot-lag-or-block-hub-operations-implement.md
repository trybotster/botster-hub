# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787428646_897571`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Verify `review_1787428576_249460` plus human answer `question_1787437854_708832` restating `question_1787428441_900918` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work is in the ticket worktree. This visit replaces a prior Implement agent and does not rebuild the campaign.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[load campaigns need a host validity oracle not fd and pty probes]]
- [[wall clock bounds in campaign fault lanes waste whole runner dispatches]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[host exhaustion markers identify each failed test]]
- [[test script required for rust tests not cargo test]]
- [[event plane terminal budgets are new coexistence regression budgets]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787428577_251857` Host-validity oracle | **blocker** | Campaign classifier uses disabled-arm published-budget validity and a monotonic scheduler-lag probe. FD/PTY probes remain additional evidence. Load average is diagnostic only. A valid disabled arm plus an enabled-arm budget breach is `product_failure`. Both oracles are in the dataset JSON. |
| `finding_1787428577_944142` ShedBusy 5 ms clock | **blocker** | Deleted the wall-clock assertion. `prove_shed_busy_non_blocking` keeps `assert_eq!(status, EventPlaneStatus::ShedBusy)`. |
| `finding_1787428576_764875` Reference profile | **blocker** | Published reference is `stress_profile=none`. Runner, N=300, noisy producer, four drivers, and 150 events/s stay fixed. Dataset writer, calibration JSON, budgets doc, plan A.2 / 12.7, and the event-plane script guard now require `none`. |
| `finding_1787428577_920664` Provenance | medium | Residual-tail runs `32591282234` and `32591872269` at `ef77621`, and `32594580606` at `8ee0d7a`, are recorded as inconclusive `host_exhaustion` observations. Counters from `32594580606` are not product results. |
| `finding_1787428577_842008` Moderate-stress lane | info | Not added. Non-gating and only after the `none` reference campaign passes. |

## Provenance of residual-tail dispatches

These three GitHub `ubuntu-24.04` residual-tail calibration runs are **not** product pass/fail:

| Run | Commit | Classification |
| --- | --- | --- |
| `32591282234` | `ef77621` | inconclusive `host_exhaustion` |
| `32591872269` | `ef77621` | inconclusive `host_exhaustion` |
| `32594580606` | `8ee0d7a` | inconclusive `host_exhaustion` |

`32594580606` reported `max_owner_turn_us` 219723 and `max_ready_operation_wait_us` 1845228 while FD and PTY probes were `Unconfirmed`. Those counters must not be cited as product results. The amended `none` profile supersedes them.

## Files changed

- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `script/run-loaded-daemon-lifecycle`
- `script/run-loaded-daemon-lifecycle-selftest`
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Plan revision 8 named `residual-tail` as the reference. Human answer `question_1787437854_708832` amends **only** that field to `none`. N=300 and the rest of A.2 stay fixed.
- Measurement arms now run decoupled first so a disabled-arm sample exists before enabled-arm classification.
- This Darwin worktree does not re-dispatch ubuntu-24.04 calibration. Verify must dispatch calibration on `stress_profile=none`.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail`.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 13 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none` validate-only and residual-tail rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 ubuntu-24.04 `none` calibration and acceptance have not run after this commit. Verify must dispatch calibration on the reference runner with `-f stress_profile=none`.

## Missing vault guidance

None. The host-validity and ShedBusy wall-clock notes already cover this return.
