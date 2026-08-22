# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787425224_722611`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `2fdb0caf335857d42a91e14f487e1e9413bab01e` |
| Returned from | `review_1787425150_815377` (`changes_required`, five findings, Verify) |
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
- [[hub client event queue max requires Botster test mode]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]

## Review findings addressed

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787425150_146627` calibration dies on WebRTC Status | blocker | The lane drains host events for up to 100 ms per outer turn with persistent reassembly. Inbound queue is 8192. Overflow is counted, not dropped silently. N, window, and event rate are unchanged. |
| `finding_1787425150_398991` opaque client panic | blocker | Status errors read `local-webrtc-sender-terminal.json`, Status observability, PTY/FD probes, and inbound overflow, then panic with `product_failure` or `host_exhaustion`. |
| `finding_1787425150_680061` dropped frames and cancelled chunks | high | `receive_delivery` stores chunk state on the peer. A cancelled `timeout` does not discard taken chunks. `try_send` failure increments `inbound_overflow`. |
| `finding_1787425150_470361` mailbox re-serializes bytes | low | `publish_age` stores `inner.bytes as u64`. |
| `finding_1787425150_464068` PTY probe uses `os.openpt` | medium | Probe calls `os.posix_openpt`. |

## Files changed

- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `tests/hub_daemon_lifecycle/harness.rs`
- `src/daemon_event_subscriptions.rs`

## Ownership

Hub owns the campaign, mailbox age publication, and the PTY probe used by IsolatedHub tests. Direct merge; no PR.

## Teardown lenses

Isolation, bounds, late-message matrix, production-path hard-stop, ownership identity, and sibling fail-closed remain implemented. The hang-close child oracle still passed.

## Deviations

None from N=300, 600 s, or 150 events/s. This Darwin worktree did not re-dispatch ubuntu-24.04 calibration.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 8 passed, 1 ignored
- `./test.sh --locked --lib mailbox` — 5 passed
- `./test.sh --locked --lib local_webrtc_close_hang_fail_closed_returns_handler_within_deadline` — passed in parent and child
- `python3 -c 'os.posix_openpt(...)'` — succeeded on this host

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not been re-run after this commit. Verify must dispatch calibration on the reference runner. No calibration dataset exists yet.

## Missing vault guidance

None.
