# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787446999_422743`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Verify `review_1787446925_877158` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Feature commit | `f8342c5e518561b73a4036d8709f9bab15e69c25` |
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

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787446925_942023` Move the reference runner to the authorized self-hosted 16-vCPU label | **blocker** | Event-plane saturation selects `runs-on: botster-ubuntu-24.04-16core`. Other loaded-lifecycle targets keep GitHub-hosted `ubuntu-24.04`. N=300, `stress_profile=none`, and every other workload literal stay unchanged (`question_1787446719_111838`). |
| `finding_1787446925_170078` Add a runner admission check for Linux x64, Ubuntu 24.04, and exactly 16 logical CPUs | **blocker** | `admit_event_plane_runner` in `validate_inputs` rejects a mismatch before load starts, records `uname`, OS id/version, and logical CPU count in `metadata.txt`, and the selftest proves a 4-CPU Ubuntu 24.04 stub is refused. |
| `finding_1787446925_490225` Record the 16-vCPU runner as the fixed reference in the plan and the budgets document | **blocker** | Plan revision 15 and A.2, plus `docs/event-plane-load-proof.md`, name the label and 16 logical CPUs. Source guards assert the label, CPU count, workflow selection, and admission helper. |
| `finding_1787446925_410834` Dispatch stays blocked until the runner label exists | **blocker** | This report states calibration and acceptance remain blocked until a runner registers with `botster-ubuntu-24.04-16core`. Implement does not dispatch. Runner provisioning is routed to a human. |
| `finding_1787446925_739787` Record run 32608460536 as host_exhaustion and exclude its measurements | **medium** | Evidence JSON records run `32608460536` at `15aa80d` as inconclusive `host_exhaustion` (gate 8 lag 71428 µs / 50000 µs, 523114 samples). Gate 6 `203974` µs and gate 7 `1964694` µs are not product results. |

Older open findings stay satisfied by prior visits (eleven-gate classifier, Gate 4 attempt accounting, stream parser, tail close, full echo validation, fixed 600-second cutoff).

## Files changed

- `.github/workflows/loaded-daemon-lifecycle.yml`
- `script/run-loaded-daemon-lifecycle`
- `script/run-loaded-daemon-lifecycle-selftest`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 15)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out previously; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 calibration. Gates are proved with classifier, artifact, watchdog, measurement-fold, stream-parser, tail-close, echo-suffix, fixed-cutoff, runner-admission, poison, and injected-panic tests plus the existing campaign unit tests.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail` on GitHub-hosted `ubuntu-24.04`. This campaign's published reference is `botster-ubuntu-24.04-16core` with `stress_profile=none`.
- Calibration and acceptance are not dispatched from this visit. No self-hosted runner with label `botster-ubuntu-24.04-16core` is registered yet.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 35 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none`, 16-CPU admission stubs, and 4-CPU rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 calibration and acceptance have not run after this commit. They remain blocked until a runner registers with `botster-ubuntu-24.04-16core` and the admission check can pass. Residual-tail GitHub runs 32591282234, 32591872269, 32594580606, and none-profile run 32608460536 remain inconclusive `host_exhaustion`, not product results.

## Missing vault guidance

None. `question_1787446719_111838` authorizes the 16-vCPU self-hosted runner. The eleven-gate answer still covers host validity.
