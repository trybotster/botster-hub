# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787445054_448117`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Review `review_1787445038_849246` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Feature commit | `FEATURE_COMMIT_PLACEHOLDER` |
| SHA-record commit | `SHA_RECORD_COMMIT_PLACEHOLDER` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work is in the ticket worktree. This visit does not rebuild the campaign and does not retune production budgets.

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
| `finding_1787445038_975569` Terminal samples use a moving cutoff instead of the fixed 600-second window | **blocker** | Gate 5 captures one monotonic origin with `start_at`/`end_at` and computes `window_end_ns = origin_ns + warmup + 600s`. Every `OutputStreamFold` ingest and `close_window` uses that fixed cutoff. A late loop iteration that starts before Instant `end_at` but receives a record emitted after the cutoff does not add a `terminal_output` sample. `event_plane_saturation_late_iteration_post_cutoff_record_is_not_sampled` drives the production helper and proves the post-cutoff record is not sampled. |

Older open findings stay satisfied by prior visits (eleven-gate classifier, Gate 4 attempt accounting, stateful stream parser, final drain, full echo validation). Plan revision 14 records this cutoff finding.

## Files changed

- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 14)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out previously; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 ubuntu-24.04 calibration. Gates are proved with classifier, artifact, watchdog, measurement-fold, stream-parser, tail-close, echo-suffix, fixed-cutoff, poison, and injected-panic tests plus the existing campaign unit tests.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail`. This campaign's published reference stays `stress_profile=none`.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 35 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none` validate-only and residual-tail rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 ubuntu-24.04 `none` calibration and acceptance have not run after this commit. Verify must dispatch calibration on the reference runner with `-f stress_profile=none`.

## Missing vault guidance

None. The eleven-gate answer already covers host validity; this return pins Gate 5 terminal samples to the fixed 600-second window.
