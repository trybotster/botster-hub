# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787425945_624329`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `903b94c094731b91bee4ff7d09e4a51f5daeef54` |
| Returned from | `review_1787425929_870062` (`changes_required`, three findings) |
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
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787425929_791219` Python 3.13-only PTY probe | blocker | `probe_pty_allocation` calls `libc::posix_openpt`. Tests prove a free PTY is Unconfirmed and EMFILE/ENFILE/EAGAIN are Confirmed. |
| `finding_1787425930_854018` enlarged queues hide lag | high | Inbound frames and pending host events are capped at 128 events. Occupancy records count, bytes, oldest age, high-water, and overflow. The dataset stores `client_fixture_queues.webrtc`. Overflow or age above 1000 ms is `product_failure`. |
| `finding_1787425930_464537` no behavioral tests | high | `inbound_chunk_reassembly_survives_cancelled_read` delivers two chunks with a pause between them. `inbound_occupancy_overflows_at_explicit_count_and_byte_limits` forces overflow. |

## Files changed

- `tests/hub_daemon_lifecycle/harness.rs`
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`

## Ownership

Hub owns IsolatedHub campaign fixtures and the PTY probe. Direct merge; no PR.

## Teardown lenses

Unchanged. Hang-close remains the live fail-closed oracle.

## Deviations

None from N=300, 600 s, or 150 events/s. This Darwin worktree did not re-dispatch ubuntu-24.04 calibration.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 8 passed, 1 ignored
- `./test.sh --locked --test hub_daemon_lifecycle_test inbound_` — 2 passed
- `./test.sh --locked --test hub_daemon_lifecycle_test pty_probe` — 2 passed

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not been re-run after this commit. Verify must dispatch calibration on the reference runner.

## Missing vault guidance

None.
