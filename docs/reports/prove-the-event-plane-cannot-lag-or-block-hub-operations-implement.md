# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787423257_503379`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `bd280def27265df068f726832e59bf30a4752270` |
| Returned from | `review_1787423243_916770` (`changes_required`, three findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[saturation counters do not acquire the contended lock they report]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[event plane client proof uses library contract fixtures]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[hub shutdown preserves durable session workers]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787423243_120556` dataset omits resync | high | `FaultReport` carries observability and lifecycle counters. Calibration and acceptance JSON now include `lifecycle.faults.lifecycle_baseline_reads` and `lifecycle_resync_reads`. A dataset-shape test fails when those fields are absent. |
| `finding_1787423243_652235` env lookup in owner loop | medium | `hold_journal_pull` is parsed once into `HubTestSeams`. `HubRuntime::journal_pull_held` checks the stored path. Production stays on `None` with no environment read. |
| `finding_1787423244_611055` duplicate libc | low | Removed the extra `[dev-dependencies]` `libc` entry. The campaign uses `botster_hub_test_support::monotonic_now_ns`. |

## Files changed

- `src/runtime.rs` — startup-parsed hold path on `HubTestSeams`.
- `src/daemon_maintenance.rs` — uses `runtime.journal_pull_held()`.
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs` — fault lifecycle dataset fields.
- `crates/botster-hub-test-support/src/lib.rs` — shared monotonic clock.
- `Cargo.toml` — removed duplicate libc.

## Ownership

Hub owns Status lifecycle counters, the test-only hold seam, and the campaign. Direct merge; no PR.

## Teardown lenses

Isolation, bounds, late-message matrix, production-path hard-stop, ownership identity, and sibling fail-closed remain implemented. This visit does not change those paths.

## Deviations

None. Calibration and acceptance still have not executed on ubuntu-24.04. Package 0.1.42 is unpublished.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 8 passed, 1 ignored
- `./test.sh --locked --lib hub_test_seams_require_test_mode` — passed

The ignored N=300 campaign was not run on this Darwin worktree.

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not run.

## Missing vault guidance

None.
