# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787448019_881307`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Verify `review_1787447963_920277` (Review `review_1787447898_566588`) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Feature commit | this feature commit (artifact records the SHA) |
| SHA-record commit | this report commit (artifact records the SHA) |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work is in the ticket worktree. This visit does not rebuild the campaign, does not retune production budgets, and does not dispatch N=300.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[load campaigns need a host validity oracle not fd and pty probes]]
- [[conformance harnesses gate on deterministic invariants not timing]]
- [[test script required for rust tests not cargo test]]
- [[implementation steps must persist report artifacts for review]]
- [[a regression test must be shown to go red with the fix reverted]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787447963_363990` / `finding_1787447898_928539` Runner admission makes three selftest negative controls tautological | **blocker** | The missing-phase, missing-calibration-commit, and residual-tail cases now run under `with_authorized_event_plane_runner`. Each asserts `event_plane_runner_admission=pass` and its own harness message. Ablating the intended guard now fails the selftest instead of being masked by 4-CPU admission. |
| `finding_1787447963_454843` / `finding_1787447898_210188` Calibration and acceptance remain blocked until the authorized runner exists | **blocker** | No dispatch. Workflow, admission check, and documentation stay. Human answer `question_1787447435_428566` already records that Jason or an org admin must register `botster-ubuntu-24.04-16core`. This report restates the campaign is pending that runner. |

Older findings stay satisfied by prior visits (16-vCPU routing, eleven-gate classifier, Gate 4, stream parser, tail close, full echo validation, fixed 600-second cutoff).

## Files changed

- `script/run-loaded-daemon-lifecycle-selftest`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs` (source guards)
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 16, section 12.6)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out previously; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 calibration.
- Calibration and acceptance are not dispatched. No self-hosted runner with label `botster-ubuntu-24.04-16core` is registered yet (`question_1787447435_428566`).

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 35 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass; residual-tail, missing-phase, and missing-commit now fail on their own harness messages after `event_plane_runner_admission=pass` |

This Darwin worktree cannot execute the ignored N=300 campaign.

## Unverified

N=300 calibration and acceptance have not run after this commit. They remain blocked until a runner registers with `botster-ubuntu-24.04-16core` and the admission check can pass. Residual-tail GitHub runs 32591282234, 32591872269, 32594580606, and none-profile run 32608460536 remain inconclusive `host_exhaustion`, not product results.

## Missing vault guidance

None. `question_1787447435_428566` already routes runner registration to a human. The eleven-gate answer still covers host validity.
