# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787440727_917843`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Review `review_1787440703_182294` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Implement commit | pending first commit SHA |
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
| `finding_1787440704_750397` Gate 4 drops late completions | **blocker** | Workers send `MeasurementSample::Start` for every post-warm-up attempt and always send `Finish`, including after `end_at`. Op errors no longer abort the window. Gate 4 fails on failures, incomplete cycles, attempts≠successes, or worker errors. Gate 2 uses only `window_completed`. Tests cover late completion, incomplete cycle, mismatch, and worker-error isolation. |
| `finding_1787440704_332477` Gate 5 one boolean | **blocker** | `TerminalOracles` records exact bytes/ordering, continuous sequence, zero I/O failure, zero unexpected **terminal** gap, and no peer loss. `parse_output_records` counts decode failures and invalid `N` headers; identity / echo / `0x80 0xff` bytes are not malformed. Unix/WebRTC PackageEvent or EventGap stay event-plane observations. Negative tests per oracle. |
| `finding_1787440704_480919` scheduler `>=` vs at most | medium | `exceeds_budget` is `lag_us > 50_000`. Boundary test: 49,999 pass, 50,000 pass, 50,001 fail. |
| `finding_1787440703_272480` panic wipes gates | **blocker** | `ArmRunBuilder` publishes partial ops, terminal, and errors during the window. `WorkerStopGuard` and `EmitterGuard` stop work on unwind. `catch_unwind` classifies from the builder, persists the artifact, then fails. Injected early-failure test reads gates 1–5 as `fail`, not `not_evaluated`. WebRTC peer loss records `event-plane-peer-loss` instead of classifying `None`. |

Older open findings (`finding_1787439371_929304`, `679197`, `finding_1787428577_251857`) stay satisfied.

## Files changed

- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 11)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out previously; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 ubuntu-24.04 calibration. Gates are proved with classifier, artifact, watchdog, measurement-fold, and oracle injection tests plus the existing campaign unit tests.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail`. This campaign's published reference stays `stress_profile=none`.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 28 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none` validate-only and residual-tail rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 ubuntu-24.04 `none` calibration and acceptance have not run after this commit. Verify must dispatch calibration on the reference runner with `-f stress_profile=none`.

## Missing vault guidance

None. The eleven-gate answer already covers host validity; this return tightens attempt recording, terminal evidence, the scheduler boundary, and panic classification.
