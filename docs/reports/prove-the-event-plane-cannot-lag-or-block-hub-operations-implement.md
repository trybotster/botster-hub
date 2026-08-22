# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787422528_557286`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `8b971e46e0fcebcbd7e7c5a9fc25175cfea5b3c5` |
| Returned from | `review_1787422514_495972` (`changes_required`, two findings) |
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
| `finding_1787422514_600046` 4 KiB workload and unsafe clock | high | One long-lived Python process emits 4096-byte records (`N%08dT%020d` plus pad plus newline) using `time.monotonic_ns()`. The sampler reads the matching OS monotonic clock. The record-length parser test checks 4096 bytes. |
| `finding_1787422514_577746` cursor expiry not deterministic | high | `BOTSTER_HUB_TEST_HOLD_JOURNAL_PULL` skips journal pull while the file exists. The lane writes the hold, spawns `cursor-changed`, wraps 20 rows past capacity 16, then removes the hold. Acceptance requires a new `lifecycle_resync_reads` increment and a running `cursor-changed`. |

## Files changed

- `src/runtime.rs` — `journal_pull_held` file seam, `BOTSTER_ENV=test` only.
- `src/daemon_maintenance.rs` — skip journal pull while the hold file exists.
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs` — 4 KiB monotonic producer and held cursor-expiry lane.
- `Cargo.toml` — `libc` dev-dependency for the monotonic clock in the campaign test.

## Ownership

Hub owns the test-only journal-pull hold and the campaign. Direct merge; no PR.

## Teardown lenses

Isolation, bounds, late-message matrix, production-path hard-stop, ownership identity, and sibling fail-closed remain implemented. This visit does not change those paths.

## Deviations

None from the approved 4 KiB / 100 ms terminal workload. Calibration and acceptance still have not executed on ubuntu-24.04. Package 0.1.42 is unpublished.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 7 passed, 1 ignored
- `./test.sh --locked --lib hub_test_seams_require_test_mode` — passed

The ignored N=300 campaign was not run on this Darwin worktree.

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not run. The ignored campaign contains the live 4 KiB producer and the held cursor-expiry lane.

## Missing vault guidance

None.
