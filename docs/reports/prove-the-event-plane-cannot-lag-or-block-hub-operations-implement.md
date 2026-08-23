# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787443532_101518`) |
| Merge policy | `direct` into `main`; no PR |
| Returned from | Review `review_1787443515_507854` |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Feature commit | `088e515090d1a85d53a1f0dade241e36018780de` |
| SHA-record commit | this report commit (artifact records the SHA) |
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
| `finding_1787443515_608390` Gate 5 does not close the terminal measurement tail or validate full echo bytes | **blocker** | After the 600-second loop, a bounded final subscription drain parses records whose `emit_ns` is inside the window. The in-window sequence is closed only when the first post-window record is `last + 1`. A leftover post-window partial is allowed; an unresolved in-window tail is not. Input echoes must match `ns-echo:` plus the 64-byte padded token plus a line ending. The parser counts a corrupted echo suffix as malformed. Echo-path events are folded through the same stream parser so mixed records are not dropped. Negative tests cover a queued final in-window record and a corrupted echo suffix. |
| `finding_1787443515_343369` The implementation artifact does not identify its final report commit | **medium** | This report and the replacement implement artifact record both the feature commit and the SHA-record commit. |

Older open findings (`finding_1787442149_580893`, `finding_1787440704_332477`, `finding_1787439371_929304`, `finding_1787428577_251857`) stay satisfied by prior visits plus this tail and echo tightening. Plan revision 13 records the two new findings.

## Files changed

- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `docs/event-plane-load-proof.md`
- `docs/plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` (revision 13)
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-evidence.json`
- `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures and the loaded-lifecycle runner. Direct merge; no PR. No production budget retune. No public protocol change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

- Project Pipelines vault-checklist create timed out previously; this report and the gate evidence are the checklist fallback.
- Local Darwin cannot run ignored N=300 ubuntu-24.04 calibration. Gates are proved with classifier, artifact, watchdog, measurement-fold, stream-parser, tail-close, echo-suffix, poison, and injected-panic tests plus the existing campaign unit tests.
- Workflow default for other loaded-lifecycle tickets remains `residual-tail`. This campaign's published reference stays `stress_profile=none`.

## Tests

| Command | Result |
| --- | --- |
| `cargo fmt --all` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` | 34 passed, 0 failed, 1 ignored (`event_plane_saturation_campaign`) |
| `script/run-loaded-daemon-lifecycle-selftest` | pass, including `stress_profile=none` validate-only and residual-tail rejection |

This Darwin worktree cannot execute the ignored N=300 ubuntu-24.04 campaign.

## Unverified

N=300 ubuntu-24.04 `none` calibration and acceptance have not run after this commit. Verify must dispatch calibration on the reference runner with `-f stress_profile=none`.

## Missing vault guidance

None. The eleven-gate answer already covers host validity; this return closes the measurement-window tail and full echo-byte proof.
