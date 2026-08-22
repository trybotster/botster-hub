# Implement report: prove the event plane cannot lag or block Hub operations

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786663585_879846` |
| Run | `run_1787262311_549251` |
| Step | `botster_stack_implement` (`run_step_1787421545_304726`) |
| Merge policy | `direct` into `main`; no PR |
| This implement commit | `f74bb60ee95912f809c51a024c1b9b7790b21292` |
| Returned from | `review_1787421530_877245` (`changes_required`, five findings) |
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
| `finding_1787421530_433070` queue bounds skip indeterminate rows | high | `queue_bound_violations` fails every active `Indeterminate`/`Unknown` row. Empty rows must prove `count=0` and `bytes=0`. `event_age_sample_failures` must stay at `EVENT_PLANE_MAX_AGE_SAMPLE_FAILURES` (0). |
| `finding_1787421530_216730` terminal output is drain time | high | The noisy PTY writes `N%08dT%020d` with `time.time_ns()` at emit. Each sample is receipt Unix ns minus emit ns. Sequence checks remain. Empty polls stay skipped. |
| `finding_1787421530_671555` cursor expiry lacks resync proof | high | Hub copies `MaintenanceState.resync_reads` onto `lifecycle_resync_reads` when Core returns `resync_required`. The lane captures that counter, wraps the 16-row journal, and requires a new increment plus reconstructed `cursor-changed`. |
| `finding_1787421530_392098` reload accepts admission without delivery | high | After reload, the lane subscribes with a unique subject, emits that token, and requires the PackageEvent or the EventGap for that subscription. Admission count is not a success path. |
| `finding_1787421530_190149` WebRTC reconnect does not reconnect | high | The lane closes the first subscribed peer, opens a replacement subscribed peer, emits a unique token after that subscribe, and requires that peer to receive the event or its gap. Unix Status and quiet-fleet survival remain. |

## Files changed

- `src/daemon_maintenance.rs` — increment `resync_reads` on Core `resync_required`.
- `src/daemon_transport.rs` — publish `lifecycle_resync_reads` on Status.
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs` — queue, output, and fault oracles.

## Ownership

Hub owns Status lifecycle counters and the campaign. No client DTO shape change. Direct merge; no PR.

## Teardown lenses

| Lens | Implemented |
| --- | --- |
| Isolation | One failed WebRTC peer is closed. Unix and the quiet fleet stay. Ultimate hang-close sibling sacrifice remains the production fail-closed oracle in `src/local_webrtc.rs`. |
| Bounds | Queue count, bytes, age, and `event_age_sample_failures` are gated. Hang-close remains bounded. |
| Late-message matrix | Closed-first and message-first WebRTC holder orders remain. Reload and reconnect use unique post-fault markers. |
| Production-path hard-stop | IsolatedHub hang-close child remains the live blast-radius oracle. Durable sessions still get `ShutdownSession` before Hub stop. |
| Ownership identity | Replacement WebRTC subscribe uses a new peer after the first peer is dropped. |
| Sibling fail-closed | Successful close keeps Unix and the fleet. Ultimate close failure stays the documented sibling sacrifice. |

## Deviations

None from the approved plan contract. Calibration and acceptance still have not executed on ubuntu-24.04. Package 0.1.42 is unpublished until a publish ticket.

## Tests

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` (exit 0)
- `./test.sh --locked --test hub_daemon_lifecycle_test event_plane_saturation_` — 7 passed, 1 ignored
- `./test.sh --locked --lib cursor_expired` — 2 passed

The ignored N=300 campaign was not run on this Darwin worktree.

## Unverified

N=300 ubuntu-24.04 residual-tail calibration and acceptance have not run. Cursor expiry on the live 16-row journal is proved in the campaign lane after this commit; that lane is inside the ignored campaign.

## Missing vault guidance

None. Existing notes covered the queue, output, resync, and reconnect oracles.
