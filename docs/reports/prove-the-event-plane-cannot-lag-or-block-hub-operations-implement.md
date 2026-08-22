# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787426636_597511`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `5ca341f75be9260e10058da5c0575c5622ac9941` |
| Returned from | `review_1787426623_901468` (`changes_required`, three findings) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | **yes** |

Independent routing: ticket and run `target_id` resolve to `botster-hub`. Work is in the ticket worktree.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[saturation counters do not acquire the contended lock they report]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[event plane client proof uses library contract fixtures]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787426623_667584` inbound occupancy underflow race | high | The DataChannel callback reserves count, bytes, and enqueue time before `try_send`. A failed send rolls the reservation back from the newest frame. Occupancy keeps a per-frame enqueue deque so pop selects the next oldest. A reentrant producer-consumer test pops inside the send callback after reserve. |
| `finding_1787426623_264297` pending host events lack byte and age bounds | high | Pending events reject before count 128 or 512 KiB is exceeded. Each parked event stores enqueue time. The snapshot publishes `max_bytes` and `oldest_age_us`. The campaign gates overflow, missing `max_bytes`, byte overshoot, and age above 1000 ms. |
| `finding_1787426623_519544` tests skip async cancel and overflow | high | `inbound_chunk_reassembly_survives_cancelled_read` admits the first encrypted chunk, cancels `receive_delivery`, resumes with the second chunk, and asserts the exact plaintext. Count and byte overflows drive the real bounded channel and assert typed `CountLimit` / `ByteLimit` plus unchanged occupancy. |

## Files changed

- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`

Report path: `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-implement.md`

## Ownership

Hub owns IsolatedHub campaign fixtures. Direct merge; no PR. No public contract change. No cross-repo routing.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle. This visit does not alter peer close, sibling sacrifice, or late-message admission.

## Deviations

None from N=300, 600 s, or 150 events/s. This Darwin worktree did not re-dispatch ubuntu-24.04 calibration.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test inbound_` — 3 passed
- `./test.sh --locked --test hub_daemon_lifecycle_test pending_host_events` — 1 passed
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 8 passed, 1 ignored

One parallel run of `event_plane_saturation_shed_busy_is_non_blocking` exceeded the 5 ms bound (8.9 ms). The isolated rerun passed in 0.00 s. The test body is unchanged.

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not been re-run after this commit. Verify must dispatch calibration on the reference runner.

## Missing vault guidance

None.
