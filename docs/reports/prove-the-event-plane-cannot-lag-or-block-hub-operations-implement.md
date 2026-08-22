# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787439385_415615`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Review `review_1787439371_225335` plus human answer `question_1787439231_161941` restated as `question_1787439421_445099` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work is in the ticket worktree. This visit does not rebuild the campaign and does not retune production budgets.

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
| `finding_1787439371_929304` Eleven immutable pre-calibration gates | **blocker** | Disabled-arm validity now evaluates all eleven gates from `question_1787439231_161941`. Host-exhaustion is only scheduler-lag maximum or confirmed FD/PTY. Owner-turn / ready-wait / queue-age / sample / terminal failures without that evidence are `product_failure`. Gate 11 is `environment_tainted` or `survivors_present`. No pre-calibration `ABS*` / `THRMIN`. |
| `finding_1787439371_679197` Persist host-validity before early exit | **blocker** | Pre-measurement, disabled-arm invalid, enabled-arm invalid, WebRTC fail, and inner-arm unwind paths write `event-plane-host-validity.json` with lag, load averages, runnable, total threads, CPU steal plus `linux_proc_stat_steal_ticks`, FD/PTY, every gate result, and the class. `event_plane_saturation_persists_host_validity_artifact_before_failure_exit` writes, classifies, and reads the file back. |
| `finding_1787439371_328634` Full-arm scheduler watchdog | **high** | A 1 ms watchdog records the monotonic lag maximum across the disabled-arm interval, then stops and joins on teardown. Gate 8 uses 50,000 µs. `event_plane_saturation_scheduler_watchdog_records_injected_delayed_sample` injects 75,000 µs deterministically and also proves stop+join. |

Previous Verify findings about ShedBusy, `stress_profile=none`, residual-tail provenance, and no moderate-stress lane remain in force and were not reopened.

## Files changed

- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `tests/hub_daemon_lifecycle_test.rs` (`AtomicU64` import)
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 10)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 ubuntu-24.04 calibration. Gates are proved with classifier, artifact, and watchdog injection tests plus the existing campaign unit tests.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail`. This campaign's published reference stays `stress_profile=none`.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 20 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none` validate-only and residual-tail rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 ubuntu-24.04 `none` calibration and acceptance have not run after this commit. Verify must dispatch calibration on the reference runner with `-f stress_profile=none`.

## Missing vault guidance

None. The eleven-gate answer and host-validity notes already cover this return.
